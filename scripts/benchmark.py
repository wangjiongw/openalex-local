#!/usr/bin/env python3
"""Reproducible open-loop HTTP load; no credentials in output or process argv."""

import argparse, asyncio, collections, json, math, random, time
import httpx
from openalex_api.config import ROOT, config, atomic_json, active
from openalex_api.backend import Backend, database


def percentile(v, p):
    return sorted(v)[max(0, math.ceil(len(v) * p) - 1)] if v else None


async def run(
    seconds,
    qps,
    concurrency,
    label,
    cases,
    case_file=None,
    skip_probes=False,
    base_url=None,
    generation=None,
):
    c = config()
    url = base_url or f"http://127.0.0.1:{c['port']}"
    headers = {"X-API-Key": c["api_keys"][0]}
    async with httpx.AsyncClient(
        base_url=url, headers=headers, timeout=c["query_timeout"] + 5, trust_env=False
    ) as h:
        from openalex_api.config import read_generation

        meta = read_generation(generation) if generation else active()
        g = meta["generation"]
        if case_file:
            from pathlib import Path

            saved = json.loads(Path(case_file).read_text())
            if saved["generation"] != g:
                raise RuntimeError("case file belongs to another generation")
            records = saved["cases"][:cases]
        else:
            b = Backend(writer=True)
            try:
                keys = b.ch(
                    f"SELECT id FROM {database(g)}.entities FINAL WHERE entity='works' ORDER BY cityHash64(id) LIMIT {cases} FORMAT JSONEachRow"
                )
                records = list(
                    b.details(g, "works", [row["id"] for row in keys]).values()
                )
            finally:
                b.close()
        data = {"meta": {"generation": g, "complete": meta.get("complete", False)}}
        if not records:
            raise RuntimeError("no works to benchmark")
        queries = []
        probes = []
        for x in records:
            key = x["id"].split("/")[-1]
            detail = ("detail", "GET", "/v1/works/" + key, None, x["id"])
            queries.extend(
                [
                    detail,
                    (
                        "references",
                        "GET",
                        "/v1/works/" + key + "/references",
                        None,
                        None,
                    ),
                ]
            )
            probes.append(detail)
            if x.get("title"):
                queries.extend(
                    [
                        (
                            "search",
                            "GET",
                            "/v1/works",
                            {
                                "search": x["title"][:2000],
                                "per_page": 10,
                                "select": "id,title",
                            },
                            None,
                        ),
                        (
                            "match",
                            "POST",
                            "/v1/references/match",
                            {"references": [{"title": x["title"][:2000]}]},
                            None,
                        ),
                    ]
                )
                if len(x["title"]) <= 8191:
                    probes.append(
                        (
                            "title_equal",
                            "GET",
                            "/v1/works",
                            {"title_equal": x["title"], "per_page": 10, "select": "id"},
                            x["id"],
                        )
                    )
        if skip_probes:
            probes = []
        random.Random(20260908).shuffle(probes)
        atomic_json(
            ROOT / "reports/source-derived-cases.json",
            {
                "generation": g,
                "human_reviewed": 0,
                "provenance": "source hash sample; not independent bibliographic gold",
                "cases": [
                    {
                        "id": x["id"],
                        "title": x.get("title"),
                        "language": x.get("language"),
                    }
                    for x in records
                ],
            },
        )
        sem = asyncio.Semaphore(concurrency)
        results = []
        start = time.monotonic()
        rng = random.Random(20260908)

        async def one(case, scheduled, is_probe=False):
            kind, method, path, body, expected = case
            async with sem:
                began = time.monotonic()
                status = 0
                error = None
                found = None
                candidate_count = None
                try:
                    r = await h.request(
                        method,
                        path,
                        params=body if method == "GET" else None,
                        json=body if method == "POST" else None,
                    )
                    status = r.status_code
                    if r.is_error:
                        error = r.text[:200]
                    elif expected:
                        value = r.json()
                        if kind == "detail":
                            found = value.get("id") == expected
                        else:
                            found = any(x["id"] == expected for x in value["results"])
                            candidate_count = value["meta"]["count"]
                except httpx.HTTPError as e:
                    error = type(e).__name__
                results.append(
                    dict(
                        kind=kind,
                        status=status,
                        seconds=time.monotonic() - began,
                        queue_seconds=began - scheduled,
                        error=error,
                        expected=expected,
                        found=found,
                        candidate_count=candidate_count,
                        is_probe=is_probe,
                    )
                )

        pending = set()
        for n in range(int(seconds * qps)):
            due = start + n / qps
            await asyncio.sleep(max(0, due - time.monotonic()))
            task = asyncio.create_task(
                one(
                    probes[n] if n < len(probes) else rng.choice(queries),
                    due,
                    n < len(probes),
                )
            )
            pending.add(task)
            task.add_done_callback(pending.discard)
        if pending:
            await asyncio.gather(*pending)
        summary = {}
        for kind in sorted({x["kind"] for x in results}):
            rows = [x for x in results if x["kind"] == kind]
            lat = [x["seconds"] for x in rows if x["status"] == 200]
            summary[kind] = {
                "requests": len(rows),
                "status_counts": dict(collections.Counter(x["status"] for x in rows)),
                "p50_seconds": percentile(lat, 0.5),
                "p95_seconds": percentile(lat, 0.95),
                "p99_seconds": percentile(lat, 0.99),
                "queue_p95_seconds": percentile(
                    [x["queue_seconds"] for x in rows], 0.95
                ),
                "errors": [x for x in rows if x["status"] != 200][:10],
            }
        out = {
            "label": label,
            "generation": data["meta"]["generation"],
            "complete": data["meta"]["complete"],
            "requested_duration_seconds": seconds,
            "elapsed_seconds": time.monotonic() - start,
            "offered_qps": qps,
            "concurrency_limit": concurrency,
            "summary": summary,
            "limitations": [
                f"Source-hash sample of {len(records)} records; probe phase can be disabled; not human-reviewed bibliographic gold.",
                "Successful response latencies and error rates are reported separately.",
                "Cold process restart does not imply an OS filesystem cache flush.",
            ],
        }
        out["source_case_count"] = len(records)
        out["probe_phase_requests"] = min(len(probes), len(results))
        eval_summary = {}
        for kind in ["detail", "title_equal"]:
            rows = [
                x
                for x in results
                if x["is_probe"] and x["kind"] == kind and x["expected"]
            ]
            eval_summary[kind] = {
                "requests": len(rows),
                "hits": sum(bool(x["found"]) for x in rows),
                "errors": sum(x["status"] != 200 for x in rows),
            }
        atomic_json(
            ROOT
            / "reports"
            / ("evaluation.json" if label == "warm" else f"evaluation-{label}.json"),
            {
                "generation": g,
                "human_reviewed": 0,
                "summary": eval_summary,
                "failures": [
                    x
                    for x in results
                    if x["is_probe"] and x["expected"] and not x["found"]
                ],
                "limitations": [
                    "Source-derived checks are not independent bibliographic evaluation.",
                    "Duplicate normalized titles may have more than ten correct records.",
                    "No fuzzy-match acceptance is enabled.",
                ],
            },
        )
        path = ROOT / "reports" / f"benchmark-{label}.json"
        atomic_json(path, out)
        print(json.dumps(out, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=1800)
    p.add_argument("--qps", type=float, default=5)
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--label", default="warm")
    p.add_argument("--cases", type=int, default=1000)
    p.add_argument("--case-file")
    p.add_argument("--skip-probes", action="store_true")
    p.add_argument("--base-url")
    p.add_argument("--generation")
    a = p.parse_args()
    if a.seconds < 1 or a.qps <= 0 or a.concurrency < 1 or not 1 <= a.cases <= 10000:
        raise SystemExit("positive parameters required")
    if not a.label.replace("-", "").replace("_", "").isalnum():
        raise SystemExit("invalid label")
    asyncio.run(
        run(
            a.seconds,
            a.qps,
            a.concurrency,
            a.label,
            a.cases,
            a.case_file,
            a.skip_probes,
            a.base_url,
            a.generation,
        )
    )
