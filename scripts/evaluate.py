#!/usr/bin/env python3
"""Source-derived regression probes. This is not a human-reviewed IR benchmark."""

import argparse, json, time
import httpx, orjson
from openalex_api.config import ROOT, config, active, atomic_json
from openalex_api.backend import Backend, database


def main(n):
    c = config()
    meta = active()
    g = meta["generation"]
    b = Backend(writer=True)
    records = b.ch(
        f"SELECT id,raw FROM {database(g)}.entities FINAL WHERE entity='works' ORDER BY cityHash64(id) LIMIT {n} FORMAT JSONEachRow"
    )
    b.close()
    cases = []
    for x in records:
        r = orjson.loads(x["raw"])
        cases.append(
            dict(
                id=x["id"],
                title=r.get("title"),
                doi=r.get("doi"),
                language=r.get("language"),
                year=r.get("publication_year"),
            )
        )
    atomic_json(
        ROOT / "reports/source-derived-cases.json",
        {
            "generation": g,
            "provenance": "deterministic source sample, not human reviewed",
            "cases": cases,
        },
    )
    results = []
    with httpx.Client(
        base_url=f"http://127.0.0.1:{c['port']}",
        headers={"X-API-Key": c["api_keys"][0]},
        timeout=c["query_timeout"] + 5,
        trust_env=False,
    ) as h:
        for i, case in enumerate(cases):
            for kind, path, params in (
                [
                    ("id", "/v1/works/" + case["id"], {"select": "id"}),
                    (
                        "title_exact",
                        "/v1/works",
                        {"title_equal": case["title"], "per_page": 10, "select": "id"},
                    ),
                ]
                if case.get("title") and len(case["title"]) <= 8191
                else [("id", "/v1/works/" + case["id"], {"select": "id"})]
            ):
                if c["qps"] > 0:
                    time.sleep(1 / c["qps"] + 0.02)
                r = h.get(path, params=params)
                v = r.json()
                found = []
                if r.status_code == 200:
                    found = (
                        [v["id"]] if kind == "id" else [x["id"] for x in v["results"]]
                    )
                results.append(
                    dict(
                        id=case["id"],
                        kind=kind,
                        status=r.status_code,
                        found=any(x.endswith("/" + case["id"]) for x in found),
                        candidate_count=v.get("meta", {}).get("count"),
                    )
                )
            if i % 100 == 0:
                print(f"{i}/{len(cases)}", flush=True)
    summary = {
        kind: {
            "n": sum(x["kind"] == kind for x in results),
            "hits": sum(x["kind"] == kind and x["found"] for x in results),
        }
        for kind in ["id", "title_exact"]
    }
    out = {
        "generation": g,
        "human_reviewed": 0,
        "summary": summary,
        "failures": [x for x in results if not x["found"]],
        "limitations": [
            "Source-derived self-consistency probes do not establish real bibliographic recall or precision.",
            "Identical normalized titles can have more than ten valid works.",
            "No fuzzy match acceptance is enabled.",
        ],
    }
    atomic_json(ROOT / "reports/evaluation.json", out)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--cases", type=int, default=1000)
    a = p.parse_args()
    main(a.cases)
