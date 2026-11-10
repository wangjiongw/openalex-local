"""Interactive acceptance against a pinned candidate; no quotas or silent partial results."""
import asyncio
import math
import time

import httpx
from .config import config
from .backend import Backend, database

GOLD = [
    dict(doi="10.1038/nature14539", title="Deep learning", author="Yann LeCun", year=2015,
         source="https://www.nature.com/articles/nature14539"),
    dict(doi="10.1038/s41586-021-03819-2", title="Highly accurate protein structure prediction with AlphaFold", author="John Jumper", year=2021,
         source="https://www.nature.com/articles/s41586-021-03819-2"),
    dict(doi="10.1126/science.1225829", title="A programmable dual-RNA-guided DNA endonuclease in adaptive bacterial immunity", author="Martin Jinek", year=2012,
         source="https://www.science.org/doi/10.1126/science.1225829"),
]
EARTH_GOLD = [
    dict(doi="10.1002/qj.3803", title="The ERA5 global reanalysis", author="Hans Hersbach", year=2020, source="https://doi.org/10.1002/qj.3803"),
    dict(doi="10.1038/sdata.2015.66", title="The climate hazards infrared precipitation with stations—a new environmental record for monitoring extremes", author="Chris Funk", year=2015, source="https://doi.org/10.1038/sdata.2015.66"),
    dict(doi="10.1038/nature20584", title="High-resolution mapping of global surface water and its long-term changes", author="Jean-François Pekel", year=2016, source="https://doi.org/10.1038/nature20584"),
]
SELECT = "id,doi,title,publication_year,authorships,primary_location"


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)] if values else None


def require_passed(report, g, build):
    sustained = report.get("sustained", {})
    matrix = report.get("matrix", [])
    if (not report.get("passed") or report.get("generation") != g or report.get("build") != build
            or report.get("source_sample", 0) < 1000
            or len(report.get("independent_bibliography", [])) < len(GOLD)
            or not all(p.get("passed") for p in report["independent_bibliography"])
            or sustained.get("requested_seconds", 0) < 1800 or sustained.get("elapsed_seconds", 0) < 1800
            or sustained.get("requests", 0) < 1000 or sustained.get("errors", 1)
            or sustained.get("p95_seconds") is None or sustained["p95_seconds"] > 2
            or sustained.get("p99_seconds") is None or sustained["p99_seconds"] > 5
            or {p.get("concurrency") for p in matrix} != {1, 4, 8, 16, 32}
            or any(p.get("requested_seconds", 0) < 60 or p.get("errors", 1) for p in matrix)):
        raise RuntimeError("interactive search acceptance is incomplete or failed")


async def load(url, queries, concurrency, seconds, headers):
    started = time.monotonic()
    latencies, errors, requests = [], [], 0
    async with httpx.AsyncClient(base_url=url, headers=headers, timeout=config()["query_timeout"] + 5, trust_env=False) as h:
        async def worker(offset):
            nonlocal requests
            position = offset
            while time.monotonic() - started < seconds:
                path, params = queries[position % len(queries)]
                position += concurrency
                before = time.monotonic()
                try:
                    reply = await h.get(path, params=params)
                    if reply.status_code != 200:
                        errors.append(dict(status=reply.status_code, path=path))
                    else:
                        data = reply.json()
                        if not isinstance(data.get("results"), list):
                            errors.append(dict(status="invalid_response", path=path))
                        else:
                            latencies.append(time.monotonic() - before)
                except httpx.HTTPError as error:
                    errors.append(dict(status=type(error).__name__, path=path))
                requests += 1
        await asyncio.gather(*(worker(i) for i in range(concurrency)))
    elapsed = time.monotonic() - started
    return dict(concurrency=concurrency, requested_seconds=seconds, elapsed_seconds=elapsed, requests=requests,
                throughput=requests / elapsed, p95_seconds=percentile(latencies, .95),
                p99_seconds=percentile(latencies, .99), errors=len(errors), error_examples=errors[:10])


def run(g, build, url, seconds=1800):
    from .model import normalize_title, doi
    from .search_versions import signature
    from .proofs import file_hash
    from .config import ROOT
    headers = {"X-API-Key": config()["api_keys"][0]}
    before = {p.name: file_hash(p) for p in (ROOT / "openalex_api").glob("*.py")}
    from .search_versions import read
    manifest = read(g, build)
    gold = EARTH_GOLD if manifest.get("incremental") else GOLD
    b = Backend(writer=True)
    batch_records = []
    try:
        if manifest.get("incremental"):
            from .search_scopes import load as catalog, EARTH
            from .backend import sql_string
            table = catalog(g)["table"]
            owner = manifest["batch"] if manifest["batch_entity"] == "works" else EARTH
            keys = b.ch(f"SELECT id FROM {table} FINAL WHERE owner={sql_string(owner)} ORDER BY cityHash64(id) LIMIT 1000 FORMAT JSONEachRow")
            if owner != EARTH:
                keys += b.ch(f"SELECT id FROM {table} FINAL WHERE owner={sql_string(EARTH)} ORDER BY cityHash64(id) LIMIT 1000 FORMAT JSONEachRow")
        else:
            keys = b.ch(f"SELECT id FROM {database(g)}.entities FINAL WHERE entity='works' ORDER BY cityHash64(id) LIMIT 1000 FORMAT JSONEachRow")
        records = []
        for offset in range(0, len(keys), 100):
            records.extend(b.details(g, "works", [r["id"] for r in keys[offset:offset + 100]]).values())
        if manifest.get("incremental") and manifest["batch_entity"] != "works":
            e = manifest["batch_entity"]
            ids = b.ch(f"SELECT id FROM {database(g)}.entities FINAL WHERE entity={sql_string(e)} ORDER BY cityHash64(id) LIMIT 1000 FORMAT JSONEachRow")
            for offset in range(0,len(ids),100):
                batch_records.extend(b.details(g,e,[r["id"] for r in ids[offset:offset+100]]).values())
    finally:
        b.close()
    if not records:
        raise RuntimeError("search acceptance source sample is empty")
    queries, probes = [], []
    for record in records:
        title = record.get("title")
        if not title:
            continue
        params = dict(search=title[:2000], search_field="title", per_page=25, select=SELECT)
        queries.append(("/v1/works", params))
        name = next((a.get("author", {}).get("display_name") for a in record.get("authorships") or []
                     if a.get("author", {}).get("display_name")), None)
        if name:
            queries.append(("/v1/works", params | dict(author_search=name[:500])))
            queries.append(("/v1/works", dict(search=name[:500], search_field="author", per_page=25, select=SELECT)))
    for record in batch_records:
        if record.get("display_name"):
            queries.append(("/v1/" + manifest["batch_entity"],dict(search=record["display_name"][:2000],search_field="title",select="id,display_name",per_page=25)))
    if not queries:
        raise RuntimeError("no usable interactive search probes")
    with httpx.Client(base_url=url, headers=headers, timeout=config()["query_timeout"] + 5, trust_env=False) as h:
        state = h.get("/status")
        state.raise_for_status()
        if state.json().get("generation") != g or state.json().get("search_build") != build:
            raise RuntimeError("acceptance API is not pinned to the candidate")
        # Public bibliography cases are independent of the source-derived performance sample.
        for case in gold:
            resolved = h.get("/v1/works/lookup", params=dict(scheme="doi", identifier=case["doi"]))
            resolved.raise_for_status()
            work = resolved.json()
            if normalize_title(work.get("title")) != normalize_title(case["title"]) or work.get("publication_year") != case["year"]:
                raise RuntimeError("independent bibliography differs from local record: " + case["doi"])
            tests = [h.get("/works", params={"search.title": case["title"], "select": "id,doi,title"}),
                     h.get("/v1/works", params=dict(search=case["title"], search_field="title", author_search=case["author"],
                           filter="year:" + str(case["year"]), select="id,doi,title"))]
            matched = h.post("/v1/references/match", json={"references": [{k: v for k, v in case.items() if k != "source"}]})
            matched.raise_for_status()
            # Exercise title/author ranking too, without the exact DOI shortcut.
            fuzzy = h.post("/v1/references/match", json={"references": [{k: case[k] for k in ("title", "author", "year")}]})
            fuzzy.raise_for_status()
            if not any(doi(r["work"].get("doi")) == case["doi"] for r in fuzzy.json()["results"][0]["candidates"]):
                raise RuntimeError("bibliographic candidate recall failed")
            for reply in tests:
                reply.raise_for_status()
                if not any(doi(r.get("doi")) == case["doi"] for r in reply.json()["results"]):
                    raise RuntimeError("title/author interactive recall failed")
            probes.append(dict(doi=case["doi"], source=case["source"], passed=True))
        cold = []
        for path, params in queries[:30]:
            started = time.monotonic()
            reply = h.get(path, params=params)
            reply.raise_for_status()
            cold.append(time.monotonic() - started)
    matrix = [asyncio.run(load(url, queries, c, 60 if seconds >= 1800 else seconds, headers)) for c in (1, 4, 8, 16, 32)]
    sustained = asyncio.run(load(url, queries, 10, seconds, headers))
    unchanged = before == {p.name: file_hash(p) for p in (ROOT / "openalex_api").glob("*.py")}
    passed = bool(seconds >= 1800 and unchanged and not sustained["errors"] and sustained["requests"] >= 1000
                  and sustained["p95_seconds"] is not None and sustained["p95_seconds"] <= 2
                  and sustained["p99_seconds"] <= 5 and all(not row["errors"] for row in matrix))
    return dict(passed=passed, generation=g, build=build, projection_signature=signature(), code=before,
                source_sample=len(records), independent_bibliography=probes, user_human_reviewed=False,
                first_process_requests=dict(p95_seconds=percentile(cold, .95), filesystem_cache_flushed=False),
                matrix=matrix, sustained=sustained,
                recommended_client_concurrency=max((r for r in matrix if not r["errors"]), key=lambda r: r["throughput"], default={"concurrency": None})["concurrency"],
                limitations=["Public bibliography probes are a small curated regression set, not a measured general precision/recall benchmark.",
                             "No OS cache flush was performed; exact counts and unusually large full records are separate workloads."])
