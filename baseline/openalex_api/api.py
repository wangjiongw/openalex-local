"""Snapshot-only search API. OpenAlex JSON is returned without fabricating metadata."""

import asyncio, contextlib, difflib, hmac, json, logging, threading, time
from typing import Literal
from fastapi import FastAPI, HTTPException, Query, Request, Security
from fastapi.security import APIKeyHeader
from .budget import DEADLINE
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ConfigDict
import httpx
from .config import ROOT, ENTITIES, config, active, read_generation
from .backend import Backend, BackendError, index, database
from .model import canonical_id, numeric_id, doi, normalize_title
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


def backend():
    if not hasattr(clients, "backend"):
        clients.backend = Backend()
    return clients.backend


def generation():
    try:
        import os

        pinned = os.environ.get("OPENALEX_GENERATION")
        return read_generation(pinned) if pinned else active()
    except RuntimeError as e:
        raise HTTPException(503, str(e))


def entity_ok(entity):
    if entity not in ENTITIES:
        raise HTTPException(404, "unknown entity")


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
    if request.url.path in [
        "/healthz",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/docs/oauth2-redirect",
    ]:
        return await call_next(request)
    c = config()
    supplied = request.headers.get("x-api-key", "")
    if not any(hmac.compare_digest(supplied, k) for k in c["api_keys"]):
        return JSONResponse({"detail": "valid X-API-Key required"}, 401)
    now = time.monotonic()
    tokens, last = limiter.get(supplied, (c["burst"], now))
    tokens = min(c["burst"], tokens + (now - last) * c["qps"])
    if tokens < 1:
        return JSONResponse(
            {"detail": "rate limit exceeded"}, 429, headers={"Retry-After": "1"}
        )
    limiter[supplied] = (tokens - 1, now)
    if inflight >= c["concurrency"]:
        return JSONResponse(
            {"detail": "concurrency limit exceeded"}, 429, headers={"Retry-After": "1"}
        )
    inflight += 1
    started = time.monotonic()
    deadline_token = DEADLINE.set(started + c["query_timeout"])
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


@app.get("/status")
def status(request: Request):
    params_only(request, [])
    meta = generation()
    g = meta["generation"]
    validation = ROOT / "var/generations" / g / "validation.json"
    from .manage import health

    return {
        "application_version": "0.1.0",
        "generation": g,
        "complete": meta.get("complete", False),
        "mode": meta["mode"],
        "snapshot_manifest_sha256": meta["manifest_sha256"],
        "coverage": json.loads(validation.read_text())["entities"]
        if validation.exists()
        else {},
        "backends": {e: health(e) for e in ["clickhouse", "opensearch"]},
        "source": "local OpenAlex snapshot",
        "upstream_sync": False,
    }


@app.get("/capabilities")
def capabilities(request: Request):
    params_only(request, [])
    return {
        "entities": ENTITIES,
        "filters": FIELDS,
        "search_fields": SEARCH_FIELDS,
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
    }


@app.get("/v1/{entity}")
def listing(
    entity: str,
    request: Request,
    search: str = Query("", max_length=2000),
    search_field: str = "all",
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
):
    params_only(
        request,
        [
            "search",
            "search_field",
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
        ],
    )
    entity_ok(entity)
    c = config()
    b = backend()
    body = query(entity, search, search_field, filter, corpus, sort, group_by)
    if title_equal is not None:
        body["query"] = {
            "bool": {
                "must": [body["query"]],
                "filter": [{"term": {"name_exact": normalize_title(title_equal)}}],
            }
        }
    body["track_total_hits"] = True if count == "exact" else 10000
    fp = fingerprint(dict(entity=entity, body=body, select=select, per_page=per_page))
    payload = None
    if cursor and cursor != "*":
        if page != 1:
            raise ValueError("cursor and page cannot be combined")
        payload = decode_cursor(cursor, c["cursor_secret"])
        if payload["query"] != fp:
            raise ValueError("cursor belongs to a different query")
        meta = read_generation(payload["generation"])
        pit = payload["pit"]
        body["search_after"] = payload["after"]
    else:
        if page * per_page > 10000:
            raise ValueError("use cursor beyond the first 10,000 results")
        meta = generation()
        pit = b.os(
            "POST",
            "/"
            + index(meta["generation"], entity)
            + f"/_search/point_in_time?keep_alive={c['cursor_ttl']}s",
        )["pit_id"]
        body["from"] = (page - 1) * per_page
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
            "complete": meta.get("complete", False),
            "count": total["value"],
            "count_relation": total["relation"],
            "per_page": per_page,
            "next_cursor": next_cursor,
        },
        "results": [selected(details[k], select) for k in keys],
        "group_by": groups,
    }


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
def batch(body: Batch, request: Request):
    params_only(request, [])
    meta = generation()
    g = meta["generation"]
    out = []
    resolved = []
    for identifier in body.identifiers:
        if len(identifier) > 2048:
            raise ValueError("identifier too long")
        resolved.append(resolve(g, "works", identifier))
    docs = backend().details(
        g, "works", sorted({key for keys in resolved for key in keys})
    )
    for identifier, keys in zip(body.identifiers, resolved):
        records = [docs[key] for key in keys if key in docs]
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
def match(body: Matches, request: Request):
    params_only(request, [])
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
        q = query("works", ref.title, "title")
        q.update(
            size=50,
            track_total_hits=False,
            timeout=str(config()["query_timeout"]) + "s",
        )
        results = b.os("POST", "/" + index(g, "works") + "/_search", q)
        docs = b.details(g, "works", [h["_id"] for h in results["hits"]["hits"]])
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
    return {"generation": g, "results": output}


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
