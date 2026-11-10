"""Local Manticore transport and the versioned, rebuildable search projection."""
import hashlib
import json
import os
import re
import unicodedata
from pathlib import Path

import httpx
import orjson

from .config import ROOT, config
from .backend import BackendError, sql_string
from . import model

VERSION = 1
ENGINE_VERSION = "29.9.0"
TEXT_FIELDS = ("name", "name_en", "abstract", "abstract_en", "keywords", "keywords_en",
               "author_name", "source_name", "institution_name", "aliases", "description", "universe")
NUMBERS = {"publication_year": "integer", "cited_by_count": "bigint", "works_count": "bigint",
           "fwci": "float", "is_oa": "integer", "is_retracted": "integer", "is_paratext": "integer",
           "is_xpac": "integer", "has_fulltext": "integer"}
STRINGS = ("original_id", "source_version", "source_hash", "projection_hash", "group_name")


def runtime_root():
    suffix = hashlib.sha256(str(ROOT).encode()).hexdigest()[:12]
    return Path(config().get("manticore_runtime", f"/tmp/openalex-search-{os.getuid()}-{suffix}"))


def normalize(value):
    from .search_projection import latin_alias
    return latin_alias(unicodedata.normalize("NFKC", value or "")).casefold()


def checked_name(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,100}", value):
        raise ValueError("invalid Manticore table name")
    return value


class Client:
    def __init__(self, url=None, timeout=None):
        self.http = httpx.Client(base_url=url or config().get("manticore_url", "http://127.0.0.1:19308"),
                                 timeout=timeout or config()["query_timeout"], trust_env=False)

    def close(self):
        self.http.close()

    def sql(self, query):
        r = self.http.post("/sql?mode=raw", data={"query": query})
        result = r.json()
        if isinstance(result, dict):
            raise BackendError("Manticore: " + str(result.get("error", "invalid SQL response")))
        r.raise_for_status()
        for item in result:
            if item.get("error") or item.get("warning"):
                raise BackendError("Manticore: " + str(item.get("error") or item["warning"]))
        return result

    def rows(self, query):
        return self.sql(query)[0].get("data", [])

    def bulk(self, table, documents):
        checked_name(table)
        payload = b"".join(orjson.dumps({"replace": {"index": table, "id": key, "doc": doc}}) + b"\n"
                           for key, doc in documents)
        r = self.http.post("/bulk", content=payload, headers={"Content-Type": "application/x-ndjson"})
        r.raise_for_status()
        data = r.json()
        if data.get("errors") or data.get("error"):
            raise BackendError("Manticore bulk failed: " + str(data)[:1200])
        return data

    def search(self, body):
        r = self.http.post("/search", json=body)
        result = r.json()
        if result.get("error") or result.get("warning") or result.get("timed_out"):
            raise BackendError("Manticore search failed: " + str(result.get("error") or result.get("warning") or "timeout"))
        r.raise_for_status()
        return result


def create(client, table):
    columns = [f"{f} text" for f in TEXT_FIELDS] + [f"{f} {t}" for f, t in NUMBERS.items()]
    columns += [f"{f} string" for f in STRINGS] + ["attrs json"]
    client.sql(f"CREATE TABLE {checked_name(table)} ({','.join(columns)}) "
               "morphology='stem_en' index_exact_words='1' "
               "charset_table='non_cjk' ngram_len='1' ngram_chars='cjk' rt_mem_limit='256M'")


def project(entity, row, group="", reasons=()):
    raw = row["raw"]
    record = orjson.loads(raw)
    from .semantics import digest
    source_hash = digest(record)
    invalid_abstract = False
    try:
        model.abstract(record.get("abstract_inverted_index"))
    except ValueError:
        # Preserve the original record and digest; only the invalid search field is omitted.
        record = dict(record, abstract_inverted_index=None)
        invalid_abstract = True
    p = model.project(entity, record)
    if isinstance(p.get("country"), str):
        p["country"] = [p["country"]]
    # Include primary_location even when the snapshot's locations array is empty.
    source = (record.get("primary_location") or {}).get("source") or {}
    if entity == "works" and source.get("id"):
        sid = model.canonical_id(source["id"])
        p["source_id"] = sorted(set(p["source_id"] + [sid]))
        p["source_name"] = list(dict.fromkeys(p["source_name"] + [source.get("display_name") or ""]))
    p["primary_topic_id"] = model.canonical_id(record["primary_topic"]["id"]) if (record.get("primary_topic") or {}).get("id") else None
    p["primary_source_id"] = model.canonical_id(source["id"]) if source.get("id") else None
    p["earth_group"] = group
    p["scope_field_ids"] = p.get("field_id", [])
    authors = p.get("authorships", [])
    for a in authors:
        for f in ("author_name", "institution_name"):
            a[f] = [normalize(n) for n in a.get(f, [])]
            a[f + "_text"] = "\n".join(a[f])
        for f in ("institution_id", "ror", "country"):
            a[f + "_keys"] = "\x1f".join(a.get(f, []))
        a["present"] = {k: 1 for k, v in a.items() if v is not None and v != [] and v != ""}
    keywords = "\n".join(k.get("display_name") or "" for k in (record.get("keywords") or [])) if entity == "works" else ""
    doc = dict(name=normalize(p["name"]), abstract=normalize(p.get("abstract", "")), keywords=normalize(keywords),
               author_name="\n".join(n for a in authors for n in a["author_name"]),
               institution_name="\n".join(n for a in authors for n in a["institution_name"]),
               source_name=normalize("\n".join(p.get("source_name", []))),
               aliases=normalize("\n".join(p.get("aliases", []))), description=normalize(p.get("description", "")),
               universe="oaalltoken", original_id=p["id"], source_version=str(row["version"]),
               source_hash=source_hash, group_name=group)
    for f in ("name", "abstract", "keywords"):
        doc[f + "_en"] = doc[f]
    for f in NUMBERS:
        doc[f] = p.get(f) or 0
    attrs = {k: v for k, v in p.items() if k not in ("name", "abstract", "aliases", "description", "source_name")}
    attrs["name_literal"] = (record.get("title") or record.get("display_name") or "").lower()
    attrs["present"] = {k: 1 for k, v in p.items() if v is not None and v != [] and v != ""}
    if attrs["name_literal"]:
        attrs["present"]["name_literal"] = 1
    attrs["invalid_abstract"] = invalid_abstract
    attrs["reasons"] = list(reasons)
    doc["attrs"] = attrs
    doc["projection_hash"] = hashlib.sha256(orjson.dumps(doc, option=orjson.OPT_SORT_KEYS)).hexdigest()
    numeric = model.numeric_id(p["id"])
    if numeric is None or numeric <= 0 or numeric >= 2**63:
        raise ValueError("search entity requires a positive signed 64-bit OpenAlex numeric ID")
    return numeric, doc


def engine_config():
    root = runtime_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    (root / "data").mkdir(exist_ok=True)
    (root / "binlog").mkdir(exist_ok=True)
    binary_root = ROOT / "var/engines/manticore-29.9.0"
    path = ROOT / "config/manticore.conf"
    port = __import__("urllib.parse", fromlist=["urlparse"]).urlparse(config().get("manticore_url", "http://127.0.0.1:19308")).port
    path.write_text("searchd {\n"
                    f" listen = 127.0.0.1:{port}:http\n"
                    f" log = {root}/searchd.log\n query_log = {root}/query.log\n"
                    f" pid_file = {root}/searchd.pid\n data_dir = {root}/data\n"
                    f" binlog_path = {root}/binlog\n binlog_flush = 1\n"
                    f" threads = {min(32, config()['cpus'])}\n"
                    " max_packet_size = 64M\n buddy_path =\n}\n")
    return path, binary_root / "usr/bin/searchd"
