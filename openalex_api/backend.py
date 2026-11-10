import gzip, json, time, random, logging
import contextlib, fcntl, uuid, re
from .config import ROOT

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
    from .search_versions import names
    selected = names(g)
    if selected:
        return selected[e]
    manifest = generation_path(g) / "indexes.json"
    if manifest.exists():
        return json.loads(manifest.read_text())[e]
    return "oa-" + generation_name(g).replace("_", "-") + "-" + e


def index_expression(g):
    # Never use a generation prefix wildcard: g and g_v2 must remain isolated.
    from .search_versions import names
    selected = names(g)
    return ",".join(selected.values()) if selected else ",".join(index(g, e) for e in ENTITIES)


def checked(response):
    if response.is_error:
        match = re.search(r"Code:\s*(\d+)", response.text)
        code = match.group(1) if match else "unknown"
        labels = [
            label
            for label in (
                "READONLY",
                "MEMORY_LIMIT_EXCEEDED",
                "TOO_MANY_PARTS",
                "NO_SPACE_LEFT_ON_DEVICE",
                "CORRUPTED_DATA",
            )
            if label in response.text
        ]
        kind = ",".join(labels) or "unknown"
        raise BackendError(
            f"Backend HTTP {response.status_code}; code={code}; type={kind}; query ID is recorded separately"
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
            "array_index_out_of_bounds_exception",
            "arrayindexoutofboundsexception",
            "merge_exception",
            "merge failed",
            "unrecoverable error",
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


@contextlib.contextmanager
def request_slot(kind, enabled=True, limit=8):
    if not enabled:
        yield
        return
    from .ingest import local_lock_path

    directory = ROOT / "var/run/request-slots"
    directory.mkdir(parents=True, exist_ok=True)
    while True:
        acquired = None
        for n in range(limit):
            f = local_lock_path(directory / (kind + "-" + str(n))).open("a")
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = f
                break
            except BlockingIOError:
                f.close()
        if acquired is not None:
            break
        time.sleep(0.05)
    try:
        yield
    finally:
        fcntl.flock(acquired, fcntl.LOCK_UN)
        acquired.close()


class Backend:
    def __init__(self, writer=False):
        self.cfg = config()
        self.writer = writer
        self.http = httpx.Client(
            timeout=120 if writer else self.cfg["query_timeout"] + 2, trust_env=False
        )

    def close(self):
        self.http.close()

    def ch_stream(self, query):
        """Decode JSONEachRow incrementally, never buffering a complete HTTP response."""
        params = dict(
            max_threads=self.cfg.get("ch_read_threads", 4),
            query_id="oa-stream-" + uuid.uuid4().hex,
            max_bytes_before_external_sort=2147483648,
        )
        started = time.monotonic()
        rows = 0
        try:
            with request_slot(
                "read", self.cfg.get("execution_profile") == "throughput96"
            ):
                with self.http.stream(
                    "POST",
                    self.cfg["ch_url"],
                    params=params,
                    content=query.encode(),
                    auth=("ingest", self.cfg["ch_admin_password"]),
                    timeout=7200,
                ) as response:
                    if response.is_error:
                        response.read()
                        checked(response)
                    pending = bytearray()
                    # Text splitlines also splits U+0085/U+2028 inside JSON strings.
                    # JSONEachRow framing is byte LF only; decode after a whole row.
                    for chunk in response.iter_bytes(chunk_size=65536):
                        parts = chunk.split(b"\n")
                        pending.extend(parts[0])
                        if len(parts) == 1:
                            continue
                        if pending:
                            rows += 1
                            yield orjson.loads(pending)
                        for line in parts[1:-1]:
                            if line:
                                rows += 1
                                yield orjson.loads(line)
                        pending = bytearray(parts[-1])
                    if pending:
                        rows += 1
                        yield orjson.loads(pending)
        finally:
            LOG.info(
                "stream query_id=%s rows=%d read_seconds=%.3f",
                params["query_id"],
                rows,
                time.monotonic() - started,
            )

    def ch(self, query, data=None, query_id=None):
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
        else:
            # Long ID/Boolean filters exceed URL limits; SQL belongs in the POST body.
            kwargs["params"].pop("query")
            kwargs["content"] = query.encode()
        if self.writer and self.cfg.get("execution_profile") == "throughput96":
            kwargs["params"].update(
                max_threads=self.cfg.get("ch_write_threads", 2)
                if data is not None
                else self.cfg.get("ch_read_threads", 4),
                query_id=query_id or "oa-" + uuid.uuid4().hex,
            )
        with request_slot(
            "write" if data is not None else "read",
            self.writer and self.cfg.get("execution_profile") == "throughput96",
        ):
            r = retry_http(lambda: self.http.post(self.cfg["ch_url"], **kwargs))
        return (
            [orjson.loads(line) for line in r.content.splitlines()]
            if query.rstrip().endswith("FORMAT JSONEachRow")
            else r.text
        )

    def os(self, method, path, body=None, content=None, headers=None):
        kwargs = {"json": body} if body is not None else {}
        if content is not None:
            kwargs = {
                "content": content,
                "headers": {"Content-Type": "application/x-ndjson"},
            }
        if headers:
            kwargs.setdefault("headers", {}).update(headers)
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
            failure = BackendError(
                "Search timed out" if value.get("timed_out") else "Search shard failure"
            )
            failure.backend_context = {
                "category": "search_partial",
                "method": method,
                "path": path,
                "timed_out": bool(value.get("timed_out")),
                "took": value.get("took"),
                "shards": value.get("_shards", {}),
            }
            try:
                LOG.error("search failure %s", json.dumps(failure.backend_context, ensure_ascii=True))
            except Exception:
                pass  # Preserve the backend failure if its diagnostic log cannot be written.
            raise failure
        return value

    def insert(self, g, table, rows):
        if rows:
            started = time.monotonic()
            data = b"".join(orjson.dumps(r) + b"\n" for r in rows)
            encoded = time.monotonic()
            query_id = "oa-insert-" + uuid.uuid4().hex
            self.ch(
                f"INSERT INTO {database(g)}.{table} FORMAT JSONEachRow",
                data,
                query_id=query_id,
            )
            LOG.info(
                "insert generation=%s table=%s query_id=%s rows=%d bytes=%d encode_seconds=%.3f write_seconds=%.3f",
                g,
                table,
                query_id,
                len(rows),
                len(data),
                encoded - started,
                time.monotonic() - encoded,
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
                        "Fatal index storage error: "
                        + str(
                            val.get("error", {}).get("type", "unknown")
                            if isinstance(val.get("error"), dict)
                            else "unknown"
                        )
                    )
                if code in (429, 502, 503):
                    transient.append(val)
                else:
                    raise BackendError(
                        "Bulk indexing error: status=" + str(code) + " id=" + str(key)
                    )
            if not transient:
                return
            if attempt == 5:
                raise BackendError(
                    "Bulk transient retry exhausted; failed items="
                    + str(len(transient))
                )
            LOG.warning(
                "bulk retry generation=%s entity=%s attempt=%d transient_items=%d payload_bytes=%d",
                g,
                e,
                attempt + 1,
                len(transient),
                len(encoded),
            )
            time.sleep(min(2**attempt, 16) + random.random())

    def mget(self, g, e, keys):
        request_id = "oa-mget-" + uuid.uuid4().hex
        context = dict(request_id=request_id, generation=g, entity=e,
                       requested_count=len(keys), first_id=keys[0] if keys else None,
                       last_id=keys[-1] if keys else None)
        self.last_mget_context = context
        def fail(message, **details):
            failure = BackendError(message)
            failure.backend_context = dict(context, **details)
            try:
                LOG.error("mget failure %s", json.dumps(failure.backend_context, ensure_ascii=True))
            except Exception:
                pass  # An unwritable log must not replace the read failure.
            raise failure
        # Item-level shard errors can arrive in an HTTP 200 response while a
        # shard is briefly recovering. Retry only that narrow transient case;
        # merge/EOF/corruption causes remain fatal and are never hidden.
        # ponytail: six attempts cap shard-recovery wait; longer outages need a new stage run.
        for attempt in range(6):
            try:
                data = self.os("POST", "/" + index(g, e) + "/_mget?_source=false",
                               {"ids": keys}, headers={"X-Opaque-Id": request_id})
            except Exception as error:
                error.backend_context = dict(context, category="request_failure", type=type(error).__name__)
                raise
            docs = data.get("docs", [])
            if not isinstance(docs, list) or len(docs) != len(keys):
                fail("mget response count mismatch", category="response_count",
                     returned_count=len(docs) if isinstance(docs, list) else None)
            malformed = []
            transient = True
            for position, (key, doc) in enumerate(zip(keys, docs)):
                if isinstance(doc, dict) and doc.get("_id") == key and "error" not in doc:
                    continue
                doc = doc if isinstance(doc, dict) else {}
                error = doc.get("error", {})
                chain = []
                current = error
                for _ in range(4):
                    if not isinstance(current, dict):
                        break
                    chain.append({"type": str(current.get("type", ""))[:128],
                                  "reason": str(current.get("reason", ""))[:512]})
                    current = current.get("caused_by")
                malformed.append((position, key, doc, chain))
                if doc.get("_id") != key or not chain:
                    transient = False
                if fatal_storage(chain):
                    transient = False
                elif chain[0]["type"] not in {
                    "no_shard_available_action_exception",
                    "unavailable_shards_exception",
                    "node_not_connected_exception",
                }:
                    transient = False
            if not malformed:
                return docs
            if transient and attempt < 5:
                delay = min(2**attempt, 16) + random.random()
                if remaining(delay + 1) <= delay:
                    transient = False
                else:
                    LOG.warning(
                        "mget transient shard retry generation=%s entity=%s attempt=%d errors=%d",
                        g, e, attempt + 1, len(malformed)
                    )
                    time.sleep(delay)
                    continue
            position, key, doc, chain = malformed[0]
            fail("mget partial error for ID: " + str(key),
                 category="response_id" if doc.get("_id") != key else "item_error",
                 position=position, requested_id=key, response_id=str(doc.get("_id"))[:128],
                 type=chain[0]["type"] if chain else None,
                 cause_type=chain[1]["type"] if len(chain) > 1 else None,
                 error_chain=chain, retries=attempt)

    def details(self, g, e, keys):
        if not keys:
            return {}
        from .config import read_generation
        from .official import source_table

        try:
            table = source_table(self, read_generation(g), e)
        except FileNotFoundError:
            # Unpublished generations may have no release metadata yet.
            table = database(g) + ".entities FINAL"
        rows = self.ch(
            f"SELECT id,raw FROM {table} WHERE entity={sql_string(e)} AND id IN {sql_list(keys)} FORMAT JSONEachRow"
        )
        records = {r["id"]: orjson.loads(r["raw"]) for r in rows}
        if len(records) != len(rows):
            raise BackendError("duplicate metadata IDs in details response")
        return records

    def resolve(self, g, e, scheme, value):
        return [
            r["id"]
            for r in self.ch(
                f"SELECT id FROM {database(g)}.identifiers FINAL WHERE entity={sql_string(e)} AND scheme={sql_string(scheme)} AND value={sql_string(value)} ORDER BY id LIMIT 101 FORMAT JSONEachRow"
            )
        ]

    def resolve_many(self, g, e, scheme, values):
        if not values:
            return {}
        rows = self.ch(
            f"SELECT value,id FROM {database(g)}.identifiers FINAL "
            f"WHERE entity={sql_string(e)} AND scheme={sql_string(scheme)} "
            f"AND value IN {sql_list(sorted(set(values)))} "
            "ORDER BY value,id LIMIT 101 BY value FORMAT JSONEachRow"
        )
        result = {}
        for row in rows:
            result.setdefault(row["value"], []).append(row["id"])
        return result
