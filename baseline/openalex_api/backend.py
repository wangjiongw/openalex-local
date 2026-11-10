import gzip, json, time, random, logging

LOG = logging.getLogger(__name__)
import httpx, orjson
from .config import config, generation_name, ENTITIES, generation_path
from .model import mapping
from .budget import remaining


class BackendError(RuntimeError):
    pass


class TombstoneConflict(BackendError):
    pass


def sql_string(s):
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_list(items):
    return "(" + ",".join(sql_string(x) for x in items) + ")"


def database(g):
    return "oa_" + generation_name(g)


def index(g, e):
    if e not in ENTITIES:
        raise ValueError("unknown entity")
    manifest = generation_path(g) / "indexes.json"
    if manifest.exists():
        return json.loads(manifest.read_text())[e]
    return "oa-" + generation_name(g).replace("_", "-") + "-" + e


def index_expression(g):
    # Never use a generation prefix wildcard: g and g_v2 must remain isolated.
    return ",".join(index(g, e) for e in ENTITIES)


def checked(response):
    if response.is_error:
        raise BackendError(
            f"Backend HTTP {response.status_code}: {response.text[:1500]}"
        )
    return response


def fatal_storage(value):
    value = json.dumps(value).lower()
    return any(
        x in value
        for x in (
            "e_o_f_exception",
            "eofexception",
            "read past eof",
            "corruptindex",
            "corrupt_index",
            "checksum failed",
            "no space left",
            "disk quota",
        )
    )


def retry_http(call):
    for attempt in range(6):
        try:
            response = call()
            if response.is_error and fatal_storage(response.text):
                return checked(response)
            if response.status_code not in (429, 502, 503):
                return checked(response)
            if attempt == 5:
                return checked(response)
        except (httpx.TimeoutException, httpx.NetworkError):
            if attempt == 5:
                raise
        delay = min(2**attempt, 16) + random.random()
        if remaining(delay + 1) <= delay:
            raise TimeoutError("Retry would exceed query deadline")
        LOG.warning("backend retry attempt=%d delay_seconds=%.3f", attempt + 1, delay)
        time.sleep(delay)


class Backend:
    def __init__(self, writer=False):
        self.cfg = config()
        self.writer = writer
        self.http = httpx.Client(
            timeout=120 if writer else self.cfg["query_timeout"] + 2, trust_env=False
        )

    def close(self):
        self.http.close()

    def ch(self, query, data=None):
        user = "ingest" if self.writer else "api_reader"
        pw = self.cfg["ch_admin_password" if self.writer else "ch_reader_password"]
        kwargs = {
            "params": {"query": query},
            "auth": (user, pw),
            "timeout": remaining(7200 if self.writer else self.cfg["query_timeout"]),
        }
        if self.writer:
            kwargs["params"].update(
                async_insert=0,
                max_bytes_before_external_group_by=2147483648,
                max_bytes_before_external_sort=2147483648,
                max_bytes_in_join=4294967296,
                join_algorithm="grace_hash",
            )
        if data is not None:
            kwargs.update(
                content=gzip.compress(data, compresslevel=1),
                headers={"Content-Encoding": "gzip"},
            )
        r = retry_http(lambda: self.http.post(self.cfg["ch_url"], **kwargs))
        return (
            [orjson.loads(line) for line in r.content.splitlines()]
            if query.rstrip().endswith("FORMAT JSONEachRow")
            else r.text
        )

    def os(self, method, path, body=None, content=None):
        kwargs = {"json": body} if body is not None else {}
        if content is not None:
            kwargs = {
                "content": content,
                "headers": {"Content-Type": "application/x-ndjson"},
            }
        r = retry_http(
            lambda: self.http.request(
                method,
                self.cfg["os_url"] + path,
                timeout=remaining(120 if self.writer else self.cfg["query_timeout"]),
                **kwargs,
            )
        )
        value = r.json()
        if isinstance(value, dict) and (
            value.get("timed_out") or value.get("_shards", {}).get("failed", 0)
        ):
            raise BackendError("Search timeout or partial shard failure")
        return value

    def insert(self, g, table, rows):
        if rows:
            self.ch(
                f"INSERT INTO {database(g)}.{table} FORMAT JSONEachRow",
                b"".join(orjson.dumps(r) + b"\n" for r in rows),
            )

    def create(self, g, shards, search=True):
        db = database(g)
        self.ch(f"CREATE DATABASE IF NOT EXISTS {db}")
        self.ch(f"""CREATE TABLE IF NOT EXISTS {db}.entities (
          entity LowCardinality(String), id String, numeric_id Nullable(UInt64), version UInt64,
          source String, line UInt64, raw String CODEC(ZSTD(6)), digest FixedString(64),
          cited_by_count Nullable(UInt64), publication_year Nullable(UInt16)
        ) ENGINE=ReplacingMergeTree(version) PARTITION BY entity ORDER BY (entity,id)
        SETTINGS index_granularity=2048""")
        self.ch(f"""CREATE TABLE IF NOT EXISTS {db}.identifiers (
          entity LowCardinality(String), scheme LowCardinality(String), value String, id String, version UInt64
        ) ENGINE=ReplacingMergeTree(version) ORDER BY (entity,scheme,value,id)""")
        for table, order in [
            ("references_out", "citing,cited"),
            ("references_in", "cited,citing"),
        ]:
            self.ch(f"""CREATE TABLE IF NOT EXISTS {db}.{table} (citing UInt64,cited UInt64,version UInt64)
             ENGINE=ReplacingMergeTree(version) ORDER BY ({order}) SETTINGS index_granularity=2048""")
        # Audit table retains every source occurrence, including duplicate IDs with different JSON.
        self.ch(f"""CREATE TABLE IF NOT EXISTS {db}.audit (
          entity LowCardinality(String),id String,source String,line UInt64,digest FixedString(64),version UInt64
        ) ENGINE=ReplacingMergeTree(version) PARTITION BY entity ORDER BY (entity,id,source,line)""")
        if not search:
            return
        for e in ENTITIES:
            name = index(g, e)
            r = self.http.head(self.cfg["os_url"] + "/" + name)
            if r.status_code == 404:
                self.os("PUT", "/" + name, mapping(shards.get(e, 1)))
            else:
                checked(r)

    def bulk(self, g, e, records):
        target = index(g, e)
        payload = []
        for key, version, projection in records:
            payload.extend(
                [
                    orjson.dumps(
                        {
                            "index": {
                                "_index": target,
                                "_id": key,
                                "version": version,
                                "version_type": "external_gte",
                            }
                        }
                    ),
                    orjson.dumps(projection),
                ]
            )
        if not payload:
            return
        encoded = b"\n".join(payload) + b"\n"
        expected = {key: version for key, version, _ in records}
        for attempt in range(6):
            result = self.os("POST", "/_bulk", content=encoded)
            items = result.get("items", [])
            if len(items) != len(records):
                raise BackendError("Bulk response item count mismatch")
            transient = []
            for item, (key, version, _) in zip(items, records):
                val = item.get("index", {})
                if val.get("_id") != key or val.get("_index") != target:
                    raise BackendError("Bulk response identity mismatch")
                code = val.get("status", 0)
                if code in (200, 201):
                    continue
                if code == 409:
                    actual = self.mget(g, e, [key])[0]
                    if actual.get("found") and actual.get("_version") == expected[key]:
                        continue
                    if not actual.get("found"):
                        raise TombstoneConflict(
                            "Deleted ID still has a version tombstone: " + key
                        )
                    raise BackendError(
                        "Unexpected conflicting external version: " + key
                    )
                if fatal_storage(val):
                    raise BackendError(
                        "Fatal index storage error: " + json.dumps(val)[:1800]
                    )
                if code in (429, 502, 503):
                    transient.append(val)
                else:
                    raise BackendError("Bulk indexing error: " + json.dumps(val)[:1800])
            if not transient:
                return
            if attempt == 5:
                raise BackendError(
                    "Bulk transient retry exhausted: "
                    + json.dumps(transient[:2])[:1800]
                )
            LOG.warning("bulk retry generation=%s entity=%s attempt=%d transient_items=%d payload_bytes=%d", g, e, attempt + 1, len(transient), len(encoded))
            time.sleep(min(2**attempt, 16) + random.random())

    def mget(self, g, e, keys):
        data = self.os(
            "POST", "/" + index(g, e) + "/_mget?_source=false", {"ids": keys}
        )
        docs = data.get("docs", [])
        if len(docs) != len(keys):
            raise BackendError("mget response count mismatch")
        for key, doc in zip(keys, docs):
            if doc.get("_id") != key or "error" in doc:
                raise BackendError("mget partial error: " + json.dumps(doc)[:1500])
        return docs

    def details(self, g, e, keys):
        if not keys:
            return {}
        rows = self.ch(
            f"SELECT id,raw FROM {database(g)}.entities FINAL WHERE entity={sql_string(e)} AND id IN {sql_list(keys)} FORMAT JSONEachRow"
        )
        return {r["id"]: orjson.loads(r["raw"]) for r in rows}

    def resolve(self, g, e, scheme, value):
        return [
            r["id"]
            for r in self.ch(
                f"SELECT id FROM {database(g)}.identifiers FINAL WHERE entity={sql_string(e)} AND scheme={sql_string(scheme)} AND value={sql_string(value)} ORDER BY id LIMIT 101 FORMAT JSONEachRow"
            )
        ]
