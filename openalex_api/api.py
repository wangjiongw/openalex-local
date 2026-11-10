"""Snapshot-only search API. OpenAlex JSON is returned without fabricating metadata."""

import asyncio, contextlib, difflib, hmac, json, logging, threading, time, re
from pathlib import Path
from typing import Literal
from fastapi import FastAPI, HTTPException, Query, Request, Security
from fastapi.security import APIKeyHeader
from .budget import DEADLINE
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.convertors import StringConvertor, register_url_convertor
from pydantic import BaseModel, Field, ConfigDict
import httpx
from .config import ROOT, ENTITIES, config, active, read_generation
from .backend import Backend, BackendError, index, database
from .model import canonical_id, numeric_id, doi, normalize_title
from . import official, search_versions
from .query import (
    query,
    FIELDS,
    SEARCH_FIELDS,
    SORTS,
    GROUPS,
    fingerprint,
    encode_cursor,
    decode_cursor,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
LOG = logging.getLogger(__name__)
api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)
app = FastAPI(
    dependencies=[Security(api_key_scheme)],
    title="Local OpenAlex",
    version="0.1.0",
    description="Search local OpenAlex snapshot records, authors, institutions and citation edges. Completeness is reported per generation; no upstream API calls.",
)
clients = threading.local()
STATIC = Path(__file__).parent / "static"
app.mount("/assets", StaticFiles(directory=STATIC), name="assets")


class EntityConvertor(StringConvertor):
    regex = "|".join(ENTITIES)


register_url_convertor("openalex_entity", EntityConvertor())


@app.get("/", include_in_schema=False)
def home():
    return FileResponse(STATIC / "index.html")


def backend():
    if not hasattr(clients, "backend"):
        clients.backend = Backend()
    return clients.backend


def generation():
    try:
        import os

        pinned = os.environ.get("OPENALEX_GENERATION")
        meta = read_generation(pinned) if pinned else active()
        if pinned and meta.get("validated"):
            meta = dict(meta, metadata_only=False)
        return search_versions.attach(meta, os.environ.get("OPENALEX_SEARCH_BUILD"),
                                      allow_sealed=bool(pinned and os.environ.get("OPENALEX_SEARCH_VALIDATION") == "1"))
    except RuntimeError as e:
        raise HTTPException(503, str(e))


def entity_ok(entity):
    if entity not in ENTITIES:
        raise HTTPException(404, "unknown entity")


def require_search(meta, entity=None):
    if (meta.get("_search_error") or meta.get("_search_disabled") or (meta.get("metadata_only") and not meta.get("_search_build"))
            or (entity and (meta.get("_search_build") or {}).get("incremental") and entity not in meta["_search_build"]["indexes"])):
        raise HTTPException(503, detail={
            "code": "search_not_ready", "generation": meta["generation"],
            "message": "Full metadata is available; text search has not passed validation.",
        })


def availability(meta):
    from .manage import health

    manifest = meta.get("_search_build") or {}
    engine = manifest.get("engine", "opensearch") if manifest else config().get("search_engine", "opensearch")
    backends = {name: health(name) for name in ("clickhouse", engine)}
    if manifest.get("engine") == "manticore" and backends["manticore"]:
        from .manticore_api import client
        from .manticore_build import tables
        try:
            backends["manticore"] = {s["index"] for s in manifest["segments"]} <= tables(client())
        except (BackendError, httpx.HTTPError, ValueError):
            backends["manticore"] = False
    if manifest.get("incremental") and manifest.get("views"):
        try:
            state = backend().os("GET","/_cluster/health/"+",".join(manifest["views"].values()))
            backends["opensearch"] = state.get("status") in ("green","yellow")
        except (BackendError,httpx.HTTPError,ValueError):
            backends["opensearch"] = False
    metadata = bool(meta.get("validated") or meta.get("metadata_only"))
    return dict(
        metadata_ready=metadata and backends["clickhouse"],
        metadata_complete=metadata and meta.get("mode") == "full" and meta.get("snapshot_complete", True),
        search_ready=not meta.get("_search_error") and not meta.get("_search_disabled")
        and bool(meta.get("_search_build") or (meta.get("validated") and not meta.get("metadata_only", False)))
        and all(backends.values()),
        backends=backends,
    )


def search_coverage(meta):
    manifest = meta.get("_search_build") or {}
    from .search_scopes import catalog_path
    catalog = {}
    path = catalog_path(meta["generation"]) if meta.get("generation") else None
    if path and path.exists():
        try:
            value = json.loads(path.read_text())
            catalog = {k:value.get(k) for k in ("state","phase","rule_version","count","finalized_count","owners","rule_counts","author_seeds","evidence_counts","baseline","repair_checked","repair_written","repair_after","repair_end","repaired_from","updated","error")}
        except (OSError, ValueError):
            pass
    extra = {}
    if manifest.get("engine") == "manticore":
        from .manticore_api import coverage
        extra = coverage(manifest)
    return dict(default_scope="available", published_scopes=manifest.get("ready_scopes", []),
                global_search_complete=bool(manifest.get("global_complete", not manifest.get("incremental") and bool(manifest))),
                entities={e:dict(ready=e in manifest.get("indexes", {}), indexed_entities=manifest.get("entities", {}).get(e, {}).get("count",0)) for e in ENTITIES},
                catalog=catalog,
                **extra)


def params_only(request, allowed):
    if unknown := set(request.query_params) - set(allowed):
        raise HTTPException(
            422, "unsupported parameters: " + ", ".join(sorted(unknown))
        )
    if len(request.query_params) != len(request.query_params.multi_items()):
        raise HTTPException(422, "duplicate query parameters are not supported")


def selected(record, fields):
    if not fields:
        return record
    names = fields.split(",")
    if any(not f or "." in f for f in names):
        raise ValueError("select supports comma-separated top-level source fields")
    # Snapshot entities evolve; missing requested fields stay null, never invented.
    return {f: record.get(f) for f in names}


limiter = {}
inflight = 0


@app.middleware("http")
async def controls(request: Request, call_next):
    global inflight
    if request.url.path == "/" or request.url.path.startswith("/assets/"):
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "connect-src 'self'; img-src 'self' data:; base-uri 'none'; "
            "frame-ancestors 'none'; form-action 'self'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
    if request.url.path in [
        "/healthz", "/readyz",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/docs/oauth2-redirect",
    ]:
        return await call_next(request)
    c = config()
    supplied = request.headers.get("x-api-key", "")
    if not supplied and request.headers.get("authorization", "").lower().startswith("bearer "):
        supplied = request.headers["authorization"][7:]
    if not supplied:
        supplied = request.query_params.get("api_key", "")
    if not any(hmac.compare_digest(supplied.encode(), k.encode()) for k in c["api_keys"]):
        return JSONResponse({"detail": "valid X-API-Key required"}, 401)
    if c["qps"] > 0:
        now = time.monotonic()
        tokens, last = limiter.get(supplied, (c["burst"], now))
        tokens = min(c["burst"], tokens + (now - last) * c["qps"])
        if tokens < 1:
            return JSONResponse(
                {"detail": "rate limit exceeded"}, 429, headers={"Retry-After": "1"}
            )
        limiter[supplied] = (tokens - 1, now)
    if c["concurrency"] > 0 and inflight >= c["concurrency"]:
        return JSONResponse(
            {"detail": "concurrency limit exceeded"}, 429, headers={"Retry-After": "1"}
        )
    inflight += 1
    started = time.monotonic()
    deadline_token = DEADLINE.set(started + c["query_timeout"])
    search_token = search_versions.CONTEXT.set(None)
    try:
        # Count uploads against both concurrency and total request duration.
        if request.method == "POST":

            async def read_body():
                body = bytearray()
                async for chunk in request.stream():
                    body.extend(chunk)
                    if len(body) > 1024 * 1024:
                        return None
                return bytes(body)

            body = await asyncio.wait_for(read_body(), timeout=c["query_timeout"])
            if body is None:
                return JSONResponse({"detail": "request body exceeds 1 MiB"}, 413)
            request._body = body
        remaining = max(0.001, started + c["query_timeout"] - time.monotonic())
        response = await asyncio.wait_for(call_next(request), timeout=remaining)
        response.headers["X-Data-Source"] = "local-openalex-snapshot"
        LOG.info(
            "%s %s %s %.3fs",
            request.method,
            request.url.path,
            response.status_code,
            time.monotonic() - started,
        )
        return response
    except asyncio.TimeoutError:
        return JSONResponse({"detail": "request deadline exceeded"}, 504)
    finally:
        DEADLINE.reset(deadline_token)
        search_versions.CONTEXT.reset(search_token)
        inflight -= 1


@app.exception_handler(ValueError)
async def bad_request(request, ex):
    return JSONResponse({"detail": str(ex)}, 422)


@app.exception_handler(TimeoutError)
async def deadline_error(request, ex):
    return JSONResponse({"detail": "request deadline exceeded"}, 504)


@app.exception_handler(BackendError)
async def backend_error(request, ex):
    LOG.error("Backend request failed: %s", ex)
    return JSONResponse(
        {"detail": "backend unavailable, timed out, or returned incomplete results"},
        503,
    )


@app.exception_handler(httpx.HTTPError)
async def http_error(request, ex):
    return JSONResponse({"detail": "backend connection or query timeout"}, 503)


@app.get("/healthz")
def healthz():
    return {"status": "alive"}


@app.get("/readyz")
def readyz():
    try:
        meta = generation()
        available = availability(meta)
        ready = bool(available["metadata_ready"] and available["search_ready"])
        return JSONResponse({"ready": ready, "generation": meta["generation"],
                             "search_build": (meta.get("_search_build") or {}).get("build"),
                             **available}, status_code=200 if ready else 503)
    except (HTTPException, RuntimeError, OSError, ValueError):
        return JSONResponse({"ready": False}, status_code=503)


@app.get("/status")
def status(request: Request):
    params_only(request, [])
    meta = generation()
    g = meta["generation"]
    validation = ROOT / "var/generations" / g / (
        "metadata-release.json" if meta.get("metadata_only") else "validation.json"
    )
    notes = Path(meta["snapshot"]) / "RELEASE_NOTES.txt" if meta.get("snapshot") else None
    release = re.search(r"^RELEASE (\d{4}-\d{2}-\d{2})", notes.read_text(), re.M) if notes and notes.exists() else None
    coverage = json.loads(validation.read_text())["entities"] if validation.exists() else {}
    if meta.get("_search_build"):
        coverage = {e: dict(v, indexed_entities=meta["_search_build"]["entities"].get(e, {}).get("count", 0)) for e, v in coverage.items()}

    pipeline = {}
    from .earth_control import read_json
    from .config import generation_path
    try:
        candidate = read_json(generation_path(g) / "manticore-candidate.json")
        path = search_versions.build_path(g, candidate["build"])
        state = read_json(path / "build.json")
        pipeline = {k: state.get(k) for k in ("build", "engine", "state", "entities", "updated", "error", "expected_groups",
                    "physical_layout", "rebuild_source", "source_reconciliation")}
        pipeline["sealed_segments"] = [dict(index=s["index"], count=s["count"],
            disk_chunks=s.get("index_check", {}).get("disk_chunks"), restore_verified=s.get("restore_verified"))
            for s in state["segments"] if s.get("sealed")]
        for name in ("publication", "pause"):
            if (path / (name + ".json")).exists():
                pipeline[name] = read_json(path / (name + ".json"))
        observation = path / "observation.json"
        if observation.exists():
            pipeline["observation"] = read_json(observation)
        if (path / "progress.json").exists():
            pipeline["progress"] = read_json(path / "progress.json")
        if state.get("parent"):
            parent_path = search_versions.build_path(g, state["parent"])
            pipeline["parent"] = dict(build=state["parent"], observation=read_json(parent_path / "observation.json"))
            for name in ("mixed-load", "mixed-recovery", "serving-retest"):
                report = parent_path / "qualification" / (name + ".json")
                if report.exists():
                    value = read_json(report)
                    pipeline["parent"][name] = {k: value.get(k) for k in
                        ("state", "passed", "error", "requests", "errors", "p95_seconds", "p99_seconds", "build_records_added")}
    except FileNotFoundError:
        legacy = generation_path(g) / "search-pipeline.json"
        if legacy.exists():
            pipeline = read_json(legacy)
    return {
        "application_version": __import__("openalex_api").__version__,
        "search_pipeline": pipeline,
        "generation": g,
        "search_build": (meta.get("_search_build") or {}).get("build"),
        "search_error": meta.get("_search_error") or ((pipeline.get("error") or (pipeline.get("observation") or {}).get("error")) if not meta.get("_search_build") else None),
        "search_coverage": search_coverage(meta),
        "complete": meta.get("complete", False),
        "mode": meta["mode"],
        "snapshot_manifest_sha256": meta["manifest_sha256"],
        "snapshot_date": release.group(1) if release else None,
        "coverage": coverage,
        **availability(meta),
        "source": "local OpenAlex snapshot",
        "upstream_sync": False,
    }


@app.get("/capabilities")
def capabilities(request: Request):
    params_only(request, [])
    try:
        meta = generation()
    except HTTPException:
        meta = {}
    from . import official
    from .official_text import PROJECTED
    return {
        "generation": meta.get("generation"),
        **availability(meta),
        "search_coverage": search_coverage(meta),
        "entities": ENTITIES,
        "filters": FIELDS,
        "search_fields": SEARCH_FIELDS,
        "author_search": "works only; publication-time author name combined with title/abstract and filters; author identity is not inferred",
        "sort": SORTS,
        "group_by": GROUPS,
        "pagination": {
            "default": 25,
            "max": 100,
            "page_window": 10000,
            "cursor_ttl_seconds": config()["cursor_ttl"],
            "cursor_bound_to_query_and_generation": True,
        },
        "filter_syntax": "AND: comma; same-field OR: |; NOT: !; numeric/date range: low..high, >=value, <value; missing: null; present: !null",
        "authorship_semantics": "positive author/institution/orcid/ror predicates must match the same authorship; negative predicates exclude a work with any matching authorship",
        "corpus": "core means is_xpac=false in this local snapshot; null is not core",
        "match": "Unique exact DOI may be accepted. Bibliographic candidates are uncalibrated ranking scores, not probabilities; never auto-accepted.",
        "group_counts": "terms buckets include error bounds and omitted document count; country/topic buckets can overlap",
        "unsupported": [
            "upstream synchronization",
            "Crossref envelope emulation",
            "semantic search",
            "PDF/full text retrieval",
            "multi-hop graph search",
            "graph+text combined filtering",
            "calibrated fuzzy match acceptance",
            "spelling correction",
        ],
        "title_equal": "normalized complete name/title equality, safely accepts punctuation, up to 8191 characters",
        "counts": "bounded counts default to exact up to 10000 then gte; count=exact requests exact computation",
        "source_records": "all original JSON fields are retained; select returns top-level fields, missing selected fields as null",
        "search_engine": (meta.get("_search_build") or {}).get("engine", config().get("search_engine", "opensearch")),
        "manticore_limitations": ["no fuzzy/proximity operators", "no text aggregations or date range sorting", "only published immutable collections", "counts apply to indexed coverage"],
        "official_compatibility": {
            "complete": False,
            "reference": "https://help.openalex.org/api/",
            "paths": "/{entity} and /{entity}/{identifier}",
            "filters": {e: official.fields(e) for e in ENTITIES},
            "graph_filters": ["cites", "cited_by", "related_to"],
            "has_filters": {e: official.has_fields(e) for e in ENTITIES},
            "metadata_execution": "Qualified search indexes execute registered common filters; identifier, graph and unindexed metadata predicates use ClickHouse. Filters precede counts/sorting/pagination; broad JSON scans can exceed the deadline.",
            "indexed_filters": {e: sorted(k for k in PROJECTED if k in official.fields(e)) for e in ENTITIES},
            "default_sort": "id:asc (local stable order)",
            "auth": ["X-API-Key", "Authorization: Bearer", "api_key query parameter (local key)"],
            "pending": ([] if (meta.get("_search_build") or {}).get("global_complete") else ["remaining subject/entity text index qualification"]) + ["upstream fulltext search",
                "metric group sorts and numeric range buckets", "text search combined with every metadata filter",
                "semantic search and reranking", "merged ID maps and observed ORCID lookup",
                "post-snapshot entities, OQL/OQO, autocomplete, text tagging, upstream synchronization"],
        },
    }


@app.get("/v1/{entity}")
def listing(
    entity: str,
    request: Request,
    search: str = Query("", max_length=2000),
    search_field: str = "all",
    search_scope: str = "available",
    filter: str = Query("", max_length=8000),
    corpus: str = "all",
    sort: str | None = None,
    select: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    cursor: str | None = None,
    group_by: str | None = None,
    count: Literal["exact", "bounded"] = "bounded",
    title_equal: str | None = Query(None, max_length=8191),
    author_search: str = Query("", max_length=500),
):
    params_only(
        request,
        [
            "search",
            "search_field",
            "search_scope",
            "filter",
            "corpus",
            "sort",
            "select",
            "page",
            "per_page",
            "cursor",
            "group_by",
            "count",
            "title_equal",
            "author_search",
        ],
    )
    entity_ok(entity)
    body = query(entity, search, search_field, filter, corpus, sort, group_by, author_search)
    if title_equal is not None:
        body["query"] = {
            "bool": {
                "must": [body["query"]],
                "filter": [{"term": {"name_exact": normalize_title(title_equal)}}],
            }
        }
    return search_listing(entity, body, select, page, per_page, cursor, count, search_scope=search_scope)


def search_listing(entity, body, select, page, per_page, cursor, count="exact", scores=False, search_scope="available"):
    from .search_scopes import validate_scope
    validate_scope(search_scope)
    meta = generation()
    route = meta.get("_search_build") or {}
    if cursor and cursor != "*":
        token = decode_cursor(cursor, config()["cursor_secret"])
        if token.get("kind") == "manticore":
            route = dict(engine="manticore")
    if route.get("engine") == "manticore":
        from .manticore_api import listing as manticore_listing
        return manticore_listing(entity, body, select, page, per_page, cursor, count, scores, search_scope, meta)
    c = config()
    b = backend()
    body["track_total_hits"] = True if count == "exact" else 10000
    fp = fingerprint(dict(entity=entity, body=body, select=select, per_page=per_page, search_scope=search_scope))
    payload = None
    if cursor and cursor != "*":
        if page != 1:
            raise ValueError("cursor and page cannot be combined")
        payload = decode_cursor(cursor, c["cursor_secret"])
        if payload["query"] != fp:
            raise ValueError("cursor belongs to a different query")
        meta = read_generation(payload["generation"])
        if payload.get("search_build"):
            import os
            meta = search_versions.attach(meta, payload["search_build"], allow_sealed=bool(
                os.environ.get("OPENALEX_GENERATION") == payload["generation"] and os.environ.get("OPENALEX_SEARCH_VALIDATION") == "1"))
        else:
            search_versions.CONTEXT.set((meta["generation"], None))
        require_search(meta, entity)
        pit = payload["pit"]
        body["search_after"] = payload["after"]
    else:
        if page * per_page > 10000:
            raise ValueError("use cursor beyond the first 10,000 results")
        meta = generation()
        require_search(meta, entity)
        from .search_scopes import scope_info
        scope = scope_info(meta, entity, search_scope)
        pit = b.os(
            "POST",
            "/"
            + (scope["indexes"] or index(meta["generation"], entity))
            + f"/_search/point_in_time?keep_alive={c['cursor_ttl']}s",
        )["pit_id"]
        body["from"] = (page - 1) * per_page
    from .search_scopes import scope_info
    scope = scope_info(meta, entity, search_scope)
    if scope.get("scope_filter"):
        body["query"] = {"bool": {"must": [body["query"]], "filter": [scope["scope_filter"]]}}
    g = meta["generation"]
    body.update(
        size=per_page + 1,
        pit={"id": pit, "keep_alive": str(c["cursor_ttl"]) + "s"},
        timeout=str(c["query_timeout"]) + "s",
    )
    if body.get("from", 0) + body["size"] > 10000:
        body["size"] = per_page
    result = b.os("POST", "/_search", body)
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]
    if count == "exact" and total["relation"] != "eq":
        raise BackendError("backend did not return an exact total")
    more = len(hits) > per_page or (
        len(hits) == per_page
        and (
            total["relation"] == "gte"
            or body.get("from", 0) + len(hits) < total["value"]
        )
    )
    if payload:
        more = len(hits) > per_page
    hits = hits[:per_page]
    keys = [h["_id"] for h in hits]
    details = b.details(g, entity, keys)
    if len(details) != len(keys):
        raise BackendError("search/details generation inconsistency")
    next_cursor = None
    if more and hits:
        next_cursor = encode_cursor(
            dict(
                generation=g,
                query=fp,
                search_build=(meta.get("_search_build") or {}).get("build"),
                pit=result.get("pit_id", pit),
                after=hits[-1]["sort"],
                expires=payload["expires"]
                if payload
                else time.time() + c["cursor_ttl"],
            ),
            c["cursor_secret"],
        )
    else:
        with contextlib.suppress(Exception):
            b.os("DELETE", "/_search/point_in_time", {"pit_id": [pit]})
    groups = result.get("aggregations", {}).get("groups")
    return {
        "meta": {
            "generation": g,
            "search_build": (meta.get("_search_build") or {}).get("build"),
            **{k:v for k,v in scope.items() if k not in ("indexes", "scope_filter")},
            "complete": meta.get("complete", False),
            "count": total["value"],
            "count_relation": total["relation"],
            "per_page": per_page,
            "next_cursor": next_cursor,
        },
        "results": [selected(dict(details[h["_id"]], relevance_score=h.get("_score"))
                             if scores else details[h["_id"]], select) for h in hits],
        "group_by": groups,
    }


@app.get("/v1/works/classification")
def work_classification(
    request: Request,
    identifier: str = Query(..., max_length=2048),
    scheme: Literal["doi", "pmid", "pmcid", "openalex"] | None = None,
):
    params_only(request, ["identifier", "scheme"])
    from .search_scopes import catalog_path
    from .backend import sql_list
    meta = generation()
    g = meta["generation"]
    keys = resolve(g, "works", identifier, scheme)
    records = backend().details(g, "works", keys)
    if not records:
        raise HTTPException(404, "identifier not present in this local generation")
    path = catalog_path(g)
    catalog = json.loads(path.read_text()) if path.exists() else {}
    rows = {}
    complete = catalog.get("state") == "complete" and catalog.get("rule_version") == 2
    repairing = catalog.get("phase") == "unicode_repair"
    if catalog.get("rule_version") == 2 and not repairing:
        table = catalog["table"] if complete else catalog["facts_table"]
        # During extraction an unvisited work has no row; absence is never a negative label.
        if backend().ch("EXISTS TABLE " + table + " FORMAT JSONEachRow")[0]["result"]:
            rows = {r["id"]:r for r in backend().ch("SELECT id,reasons FROM " + table + " FINAL WHERE id IN " + sql_list(keys) + " FORMAT JSONEachRow")}
    return dict(rule_version=catalog.get("rule_version"), rules_signature=catalog.get("rules_signature"),
                catalog_complete=complete, results=[dict(id=record["id"], doi=record.get("doi"),
                    title=record.get("title") or record.get("display_name"),
                    classification=dict(label="earth_sciences" if rows.get(key,{}).get("reasons") else None,
                        earth_related=bool(rows[key]["reasons"]) if key in rows and (complete or rows[key]["reasons"]) else None,
                        state="repairing" if repairing else "complete" if complete else "provisional" if key in rows else "pending",
                        reasons=rows.get(key,{}).get("reasons", []))) for key,record in records.items()])


@app.get("/v1/works/lookup")
def lookup(
    request: Request,
    identifier: str,
    scheme: Literal["doi", "pmid", "pmcid", "openalex"] = "doi",
    select: str | None = None,
):
    params_only(request, ["identifier", "scheme", "select"])
    meta = generation()
    keys = resolve(meta["generation"], "works", identifier, scheme)
    return lookup_response(meta, "works", keys, select)


def resolve(g, e, value, scheme=None):
    b = backend()
    if scheme == "openalex" or (scheme is None and not doi(value)):
        key = canonical_id(value)
        if (
            e
            not in [
                "works",
                "authors",
                "institutions",
                "sources",
                "publishers",
                "funders",
                "awards",
                "topics",
                "concepts",
            ]
            and "/" not in key
        ):
            key = e + "/" + key
        return [key]
    scheme = scheme or "doi"
    if scheme == "doi":
        value = doi(value)
        if not value:
            raise ValueError("invalid DOI")
    else:
        value = value.strip().rstrip("/").split("/")[-1].lower()
    return b.resolve(g, e, scheme, value)


def lookup_response(meta, e, keys, select=None):
    if not keys:
        raise HTTPException(404, "identifier not present in this local generation")
    details = backend().details(meta["generation"], e, keys)
    if len(keys) > 1:
        return JSONResponse(
            {
                "status": "ambiguous",
                "generation": meta["generation"],
                "candidates": [
                    selected(details[k], select) for k in keys[:100] if k in details
                ],
                "truncated": len(keys) > 100,
            },
            409,
        )
    if keys[0] not in details:
        raise HTTPException(404, "identifier not present in this local generation")
    return selected(details[keys[0]], select)


class Batch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    identifiers: list[str] = Field(min_length=1, max_length=100)


@app.post("/v1/works/batch")
def batch(body: Batch, request: Request, select: str | None = None):
    params_only(request, ["select"])
    meta = generation()
    g = meta["generation"]
    out = []
    for identifier in body.identifiers:
        if len(identifier) > 2048:
            raise ValueError("identifier too long")
    b = backend()
    dois = [doi(identifier) for identifier in body.identifiers]
    mapping = b.resolve_many(g, "works", "doi", [value for value in dois if value]) if any(dois) else {}
    resolved = [mapping.get(value, []) if value else [canonical_id(identifier)]
                for identifier, value in zip(body.identifiers, dois)]
    docs = b.details(
        g, "works", sorted({key for keys in resolved for key in keys})
    )
    for identifier, keys in zip(body.identifiers, resolved):
        records = [selected(docs[key], select) for key in keys if key in docs]
        out.append(
            {
                "input": identifier,
                "status": "not_found"
                if not records
                else "ambiguous"
                if len(records) > 1
                else "found",
                "results": records,
            }
        )
    return {"generation": g, "results": out}


def canonical_work(work_id):
    import re

    if not re.fullmatch(r"W[0-9]+", work_id):
        raise ValueError("citation routes require a canonical W numeric ID")
    return numeric_id(work_id)


def edge_list(work_id, direction, request, per_page, cursor, select):
    params_only(request, ["per_page", "cursor", "select"])
    n = canonical_work(work_id)
    c = config()
    b = backend()
    fp = fingerprint(
        dict(work_id=work_id, direction=direction, per_page=per_page, select=select)
    )
    last = 0
    expires = time.time() + c["cursor_ttl"]
    if cursor:
        p = decode_cursor(cursor, c["cursor_secret"])
        if p["query"] != fp:
            raise ValueError("cursor belongs to a different query")
        meta = read_generation(p["generation"])
        last = int(p["after"])
        expires = p["expires"]
    else:
        meta = generation()
    g = meta["generation"]
    if not b.details(g, "works", [work_id]):
        raise HTTPException(404, "work not present in local generation")
    fixed, other = ("citing", "cited") if direction == "out" else ("cited", "citing")
    table = "references_" + direction
    rows = b.ch(
        f"SELECT {other} target FROM {database(g)}.{table} FINAL WHERE {fixed}={n} AND {other}>{last} ORDER BY {other} LIMIT {per_page + 1} FORMAT JSONEachRow"
    )
    more = len(rows) > per_page
    rows = rows[:per_page]
    keys = ["W" + str(r["target"]) for r in rows]
    details = b.details(g, "works", keys)
    return {
        "meta": {
            "generation": g,
            "complete": meta.get("complete", False),
            "next_cursor": encode_cursor(
                dict(
                    generation=g,
                    query=fp,
                    after=int(rows[-1]["target"]),
                    expires=expires,
                ),
                c["cursor_secret"],
            )
            if more
            else None,
        },
        "results": [
            {
                "id": "https://openalex.org/" + key,
                "resolved": key in details,
                "work": selected(details[key], select) if key in details else None,
            }
            for key in keys
        ],
    }


@app.get("/v1/works/{work_id}/references")
def references(
    work_id: str,
    request: Request,
    per_page: int = Query(25, ge=1, le=100),
    cursor: str | None = None,
    select: str | None = None,
):
    return edge_list(work_id, "out", request, per_page, cursor, select)


@app.get("/v1/works/{work_id}/citations")
def citations(
    work_id: str,
    request: Request,
    per_page: int = Query(25, ge=1, le=100),
    cursor: str | None = None,
    select: str | None = None,
):
    return edge_list(work_id, "in", request, per_page, cursor, select)


@app.get("/v1/works/{work_id}/related-works")
def related_works(work_id: str, request: Request, select: str | None = None):
    params_only(request, ["select"])
    canonical_work(work_id)
    meta = generation()
    g = meta["generation"]
    b = backend()
    records = b.details(g, "works", [work_id])
    if work_id not in records:
        raise HTTPException(404, "work not present in local generation")
    keys = [canonical_id(value) for value in records[work_id].get("related_works") or []]
    details = b.details(g, "works", keys)
    return {
        "meta": {
            "generation": g,
            "complete": meta.get("complete", False),
            "method": "snapshot_related_works",
            "count": len(keys),
        },
        "results": [
            {
                "id": "https://openalex.org/" + key,
                "resolved": key in details,
                "work": selected(details[key], select) if key in details else None,
            }
            for key in keys
        ],
    }


@app.get("/v1/works/{work_id}/citation-counts")
def citation_counts(work_id: str, request: Request):
    params_only(request, [])
    n = canonical_work(work_id)
    meta = generation()
    g = meta["generation"]
    b = backend()
    records = b.details(g, "works", [work_id])
    if not records:
        raise HTTPException(404, "work not present")
    r = records[work_id]
    db = database(g)
    incoming = int(
        b.ch(
            f"SELECT count() n FROM {db}.references_in FINAL WHERE cited={n} FORMAT JSONEachRow"
        )[0]["n"]
    )
    outgoing = int(
        b.ch(
            f"SELECT count() n FROM {db}.references_out FINAL WHERE citing={n} FORMAT JSONEachRow"
        )[0]["n"]
    )
    resolved = int(
        b.ch(
            f"SELECT count() n FROM {db}.entities FINAL WHERE entity='works' AND id IN (SELECT concat('W',toString(cited)) FROM {db}.references_out FINAL WHERE citing={n}) FORMAT JSONEachRow"
        )[0]["n"]
    )
    return {
        "generation": g,
        "complete": meta.get("complete", False),
        "cited_by_count": r.get("cited_by_count"),
        "local_cited_by_count": incoming,
        "snapshot_counts_by_year": r.get("counts_by_year"),
        "raw_reference_array_length": len(r.get("referenced_works") or []),
        "unique_reference_edges": outgoing,
        "resolved_reference_targets": resolved,
    }


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    doi: str | None = Field(None, max_length=2048)
    title: str | None = Field(None, max_length=2000)
    author: str | None = Field(None, max_length=500)
    year: int | None = Field(None, ge=1, le=9999)
    source: str | None = Field(None, max_length=500)


class Matches(BaseModel):
    model_config = ConfigDict(extra="forbid")
    references: list[Reference] = Field(min_length=1, max_length=20)


@app.post("/v1/references/match")
def match(body: Matches, request: Request, search_scope: str = "available"):
    params_only(request, ["search_scope"])
    from .search_scopes import validate_scope
    validate_scope(search_scope)
    meta = generation()
    g = meta["generation"]
    b = backend()
    output = []
    for ref in body.references:
        if ref.doi:
            keys = resolve(g, "works", ref.doi, "doi")
            docs = b.details(g, "works", keys)
            if keys:
                output.append(
                    {
                        "status": "matched" if len(keys) == 1 else "ambiguous",
                        "method": "exact_doi",
                        "candidates": [
                            {"work": r, "score": None} for r in docs.values()
                        ],
                    }
                )
                continue
        if not ref.title:
            output.append({"status": "insufficient_information", "candidates": []})
            continue
        require_search(meta, "works")
        from .search_scopes import scope_info
        scope = scope_info(meta, "works", search_scope)
        q = query("works", ref.title, "title")
        boosts = [{"term": {"name_exact": {"value": normalize_title(ref.title), "boost": 4}}}]
        if ref.author:
            boosts.append({"nested": {"path": "authorships", "query": {"match": {
                "authorships.author_name": {"query": ref.author, "operator": "and", "boost": 3}}}}})
        if ref.year is not None:
            boosts.append({"term": {"publication_year": {"value": ref.year, "boost": 2}}})
        if ref.source:
            boosts.append({"match_phrase": {"source_name": {"query": ref.source, "boost": 1}}})
        q["query"] = {"bool": {"must": [q["query"]], "should": boosts, "filter": [scope["scope_filter"]] if scope.get("scope_filter") else []}}
        q.update(
            size=50,
            track_total_hits=False,
            timeout=str(config()["query_timeout"]) + "s",
        )
        if (meta.get("_search_build") or {}).get("engine") == "manticore":
            from .manticore_api import client, scope as manticore_scope, search_request
            tables = [s["index"] for s in manticore_scope(meta, "works", search_scope)]
            results = client().search(search_request("works", q, tables, 50,
                                      layout=meta["_search_build"].get("physical_layout", "legacy")))
            keys = [h["_source"]["original_id"] for h in results["hits"]["hits"]]
        else:
            results = b.os("POST", "/" + (scope["indexes"] or index(g, "works")) + "/_search", q)
            keys = [h["_id"] for h in results["hits"]["hits"]]
        docs = b.details(g, "works", keys)
        ranked = []
        for r in docs.values():
            score = (
                0.75
                * difflib.SequenceMatcher(
                    None, normalize_title(ref.title), normalize_title(r.get("title"))
                ).ratio()
            )
            if ref.year is not None and r.get("publication_year") is not None:
                score += 0.1 * max(0, 1 - abs(ref.year - r["publication_year"]) / 3)
            if ref.author:
                names = [
                    normalize_title(a.get("author", {}).get("display_name"))
                    for a in r.get("authorships") or []
                ]
                score += 0.1 * max(
                    [
                        difflib.SequenceMatcher(
                            None, normalize_title(ref.author), n
                        ).ratio()
                        for n in names
                    ]
                    or [0]
                )
            if ref.source:
                src = ((r.get("primary_location") or {}).get("source") or {}).get(
                    "display_name"
                )
                score += (
                    0.05
                    * difflib.SequenceMatcher(
                        None, normalize_title(ref.source), normalize_title(src)
                    ).ratio()
                )
            ranked.append({"score": round(score, 5), "work": r})
        ranked.sort(key=lambda x: x["score"], reverse=True)
        output.append(
            {
                "status": "candidates" if ranked else "not_found",
                "method": "uncalibrated_bibliographic_ranking",
                "candidates": ranked[:10],
            }
        )
    return {"generation": g, "search_scope": search_scope, "results": output}


@app.get("/v1/{entity}/lookup")
def entity_lookup(
    entity: str,
    request: Request,
    identifier: str = Query(..., max_length=2048),
    scheme: Literal[
        "doi", "pmid", "pmcid", "orcid", "ror", "issn", "openalex"
    ] = "openalex",
    select: str | None = None,
):
    params_only(request, ["identifier", "scheme", "select"])
    entity_ok(entity)
    meta = generation()
    return lookup_response(
        meta, entity, resolve(meta["generation"], entity, identifier, scheme), select
    )


# Greedy DOI/string-ID route is deliberately registered after graph and lookup routes.
@app.get("/v1/{entity}/{identifier:path}")
def detail(entity: str, identifier: str, request: Request, select: str | None = None):
    params_only(request, ["select"])
    entity_ok(entity)
    meta = generation()
    return lookup_response(
        meta, entity, resolve(meta["generation"], entity, identifier), select
    )


# Official plural paths are registered last to preserve all existing local routes.
@app.get("/{entity:openalex_entity}", tags=["OpenAlex compatible"], openapi_extra={
    "parameters": [{"name": name, "in": "query", "required": False,
        "schema": {"type": "integer" if name in ("page", "per_page", "sample", "seed") else "string"}}
        for name in sorted(official.PARAMS | {"search", "search.exact", "search.title", "search.title.exact",
            "search.title_and_abstract", "search.title_and_abstract.exact"}) if name not in ("api_key", "per-page", "group-by")]
})
def official_listing(entity: str, request: Request):
    from . import official

    entity_ok(entity)
    try:
        return official.execute(backend(), generation(), entity, official.parameters(request))
    except ValueError as error:
        return JSONResponse({"error": "Invalid query parameters", "message": str(error)}, 400)


@app.get("/{entity:openalex_entity}/{identifier:path}", tags=["OpenAlex compatible"], openapi_extra={
    "parameters": [{"name": "select", "in": "query", "schema": {"type": "string"}}]
})
def official_detail(entity: str, identifier: str, request: Request):
    from . import official

    entity_ok(entity)
    try:
        params = official.parameters(request)
        if set(params) - {"select", "api_key", "mailto"}:
            raise ValueError("single entity accepts select, api_key and mailto")
        selected({}, params.get("select"))
        meta = generation()
        if identifier == "random":
            result = official.execute(backend(), meta, entity, params | {"sample": "1"})
            if not result["results"]:
                raise HTTPException(404, "no entity present in this local generation")
            return result["results"][0]
        scheme, token = official.identifier(entity, identifier)
        if scheme == "openalex":
            keys = [token]
        elif scheme == "wikidata":
            mapping = backend().resolve_many(meta["generation"], entity, scheme, official.external_values(entity, scheme, [token]))
            keys = sorted({key for values in mapping.values() for key in values})
        else:
            keys = backend().resolve(meta["generation"], entity, scheme, token)
        docs = backend().details(meta["generation"], entity, keys)
        if not docs:
            raise HTTPException(404, "identifier not present in this local generation")
        # Official singleton semantics choose one record for a shared external ID.
        records = sorted(docs.values(), key=lambda r: (
            -int(scheme == "orcid" and (r.get("orcid") or "").lower().rstrip("/").endswith("/" + token)),
            -(r.get("works_count") or 0), r["id"],
        ))
        return selected(records[0], params.get("select"))
    except ValueError as error:
        return JSONResponse({"error": "Invalid query parameters", "message": str(error)}, 400)
