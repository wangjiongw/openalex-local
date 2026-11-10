"""Manticore requests over immutable builds; raw records still come from ClickHouse."""
import time
import os
import logging
import re

from fastapi import HTTPException

from .config import config, read_generation
from .query import encode_cursor, decode_cursor, fingerprint
from .model import normalize_title
from .backend import sql_string
from . import search_versions
from .backend import BackendError
from .manticore_engine import Client, NUMBERS
from .manticore_query import request_body

LOG = logging.getLogger(__name__)


def native_filters(entity, node):
    """Only push conditions required by the complete predicate."""
    if "term" in node:
        field, value = next(iter(node["term"].items()))
        value = value["value"] if isinstance(value, dict) else value
        prefix = {"works": "W", "topics": "T", "sources": "S", "authors": "A", "institutions": "I"}.get(entity)
        if field == "id" and prefix and re.fullmatch(prefix + r"[1-9]\d*", str(value)):
            number = int(value[1:])
            if number < 2**63:
                return [{"equals": {"id": number}}]
        if field in NUMBERS and NUMBERS[field] != "float":
            return [{"equals": {field: int(value)}}]
    if "range" in node:
        field, value = next(iter(node["range"].items()))
        if field in NUMBERS and NUMBERS[field] != "float":
            return [{"range": {field: value}}]
    spec = node.get("bool", {})
    result = []
    for kind in ("must", "filter"):
        children = spec.get(kind, [])
        for child in children if isinstance(children, list) else [children]:
            result.extend(native_filters(entity, child))
    choices = spec.get("should", [])
    required = spec.get("minimum_should_match", 0 if spec.get("must") or spec.get("filter") else 1)
    # A required OR can be pruned only when every branch has a necessary condition.
    if choices and required == 1:
        branches = [native_filters(entity, child) for child in choices]
        if all(branches):
            if all(len(b) == 1 and set(b[0].get("equals", {})) == {"id"} for b in branches):
                result.append({"in": {"id": [b[0]["equals"]["id"] for b in branches]}})
            else:
                result.append({"bool": {"should": [{"bool": {"must": b}} for b in branches]}})
    return result


def client():
    from .api import clients
    if not hasattr(clients, "manticore"):
        clients.manticore = Client()
    return clients.manticore


def coverage(manifest):
    return dict(engine="manticore", catalog_scopes=manifest.get("catalog_scopes", {}),
                catalog_counts={e: dict(selected_ids=p["selected"], missing_entities=p["missing"], indexed_entities=manifest["entities"].get(e, {}).get("count", 0)) for e,p in manifest.get("catalog_progress", {}).items()}, collection=manifest["collection"],
                collection_signature=manifest["collection_signature"],
                indexed_groups=manifest["expected_groups"], earth_complete=manifest.get("earth_complete", False),
                earth_expected=manifest["earth_expected"])


def scope(meta, entity, search_scope):
    from .search_scopes import validate_scope
    validate_scope(search_scope)
    if search_scope not in ("available", "earth_sciences") or entity != "works" and search_scope != "available":
        raise ValueError("this build supports available or earth_sciences for works, available for catalogs")
    manifest = meta["_search_build"]
    segments = [s for s in manifest["segments"] if s["entity"] == entity and s.get("sealed")]
    if not segments:
        raise HTTPException(503, detail=dict(code="search_not_ready", message="Entity search is not published."))
    return segments


def catalog_text(node):
    if "multi_match" in node:
        spec = node["multi_match"]
        if any(f.split("^")[0].split(".")[0] == "name" for f in spec["fields"]):
            return spec["query"]
    spec = node.get("bool", {})
    if spec.get("should") and not spec.get("must") and not spec.get("must_not"):
        choices = [catalog_text(child) for child in spec["should"]]
        if choices[0] and all(text == choices[0] for text in choices):
            return choices[0]
    if spec.get("must") and not spec.get("should") and not spec.get("must_not"):
        parts = [catalog_text(child) for child in spec["must"]]
        if all(parts):
            return " ".join(parts)
    return None


def search_request(entity, body, tables, limit, offset=0, scroll=None, layout="legacy"):
    original_body = body
    if layout == "native_v2":
        from .manticore_native_query import split
        residual, extra = split(entity, body["query"])
        body = body | {"query": residual}
    else:
        extra = native_filters(entity, body["query"])
    native = request_body(body, tables, limit, offset, scroll)
    if extra:
        native["query"].setdefault("bool", {}).setdefault("must", []).extend(extra)
        native["query"].pop("match_all", None)
    # Broad text scoring needs parallelism; cheap queries retain the shared pool's smaller fan-out.
    clauses = native["query"].get("bool", {}).get("must", [])
    if entity == "works" and any(re.search(r"@\([^)]*\b(?:abstract|keywords)(?:_en)?\b", c.get("query_string", "")) for c in clauses):
        native["options"]["threads"] = 32
    # shortcut: nested identity filters still scan; add flat postings if broad filters dominate load.
    if entity == "works" and "attrs.authorships" in native["expressions"].get("oa_predicate", "") and not any(c.get("query_string") for c in clauses):
        native["options"]["threads"] = min(96, len(os.sched_getaffinity(0)))
    native["_source"] = ["original_id", "group_name"]
    if entity != "works" and "_score" in native["sort"][0]:
        text = catalog_text(original_body["query"])
        if text:
            native["expressions"]["oa_exact_name"] = "attrs.name_exact=" + sql_string(normalize_title(text))
            native["sort"].insert(0, {"oa_exact_name": "desc"})
    return native


def listing(entity, body, select, page, per_page, cursor, count, scores, search_scope, meta):
    from .api import backend, selected, require_search
    started = time.perf_counter()
    cfg = config()
    if entity != "works":
        def validate(node):
            if not isinstance(node, dict):
                return
            for operator, value in node.items():
                if operator in ("term", "range", "exists"):
                    field = value["field"] if operator == "exists" else next(iter(value))
                    if field not in {"id", "name_exact", "name_literal", "type", "country", "works_count", "cited_by_count", "institution_id"}:
                        raise ValueError("filter is not indexed for this catalog: " + field)
                elif isinstance(value, dict):
                    validate(value)
                elif isinstance(value, list):
                    for child in value:
                        validate(child)
        validate(body["query"])
    fp = fingerprint(dict(entity=entity, body=body, select=select, per_page=per_page, count=count, search_scope=search_scope))
    payload = None
    if cursor and cursor != "*":
        if page != 1:
            raise ValueError("cursor and page cannot be combined")
        payload = decode_cursor(cursor, cfg["cursor_secret"])
        if payload.get("kind") != "manticore" or payload["query"] != fp:
            raise ValueError("cursor belongs to a different query")
        import os
        meta = search_versions.attach(read_generation(payload["generation"]), payload["search_build"],
            allow_sealed=os.environ.get("OPENALEX_SEARCH_VALIDATION") == "1" and os.environ.get("OPENALEX_GENERATION") == payload["generation"])
    elif page * per_page > 10000:
        raise ValueError("use cursor beyond the first 10,000 results")
    require_search(meta, entity)
    segments = scope(meta, entity, search_scope)
    manifest = meta["_search_build"]
    offset = 0 if payload else (page - 1) * per_page
    native = search_request(entity, body, [s["index"] for s in segments], per_page, offset,
                            payload["scroll"] if payload else None, manifest.get("physical_layout", "legacy"))
    prepared = time.perf_counter()
    result = client().search(native)
    searched = time.perf_counter()
    if result["hits"].get("total_relation") != "eq":
        raise BackendError("Manticore did not return an exact count")
    hits = result["hits"]["hits"]
    total = payload["total"] if payload else result["hits"]["total"]
    consumed = (payload["consumed"] if payload else offset) + len(hits)
    keys = [h["_source"]["original_id"] for h in hits]
    details = backend().details(meta["generation"], entity, keys)
    LOG.info("manticore_query build=%s entity=%s prepare=%.4f engine=%.4f hydrate=%.4f count=%s",
             manifest["build"], entity, prepared-started, searched-prepared,
             time.perf_counter()-searched, total)
    if len(details) != len(keys) or len(set(keys)) != len(keys):
        raise BackendError("search/details consistency failure")
    next_cursor = None
    if consumed < total and hits:
        if not result.get("scroll"):
            raise BackendError("Manticore omitted the continuation token")
        next_cursor = encode_cursor(dict(kind="manticore", generation=meta["generation"], search_build=manifest["build"],
            query=fp, scroll=result["scroll"], total=total, consumed=consumed,
            expires=payload["expires"] if payload else time.time() + cfg["cursor_ttl"]), cfg["cursor_secret"])
    records = []
    for h, key in zip(hits, keys):
        record = details[key]
        if scores:
            record = dict(record, relevance_score=h.get("_score"))
        records.append(selected(record, select))
    bounded = count == "bounded" and total > 10000
    return dict(meta=dict(generation=meta["generation"], search_build=manifest["build"], complete=False, global_search_complete=False,
        search_scope=search_scope, published_scopes=manifest["ready_scopes"], **coverage(manifest),
        indexed_entities=sum(s["count"] for s in segments), count=10000 if bounded else total,
        count_relation="gte" if bounded else "eq", per_page=per_page, next_cursor=next_cursor),
        results=records, group_by=None)
