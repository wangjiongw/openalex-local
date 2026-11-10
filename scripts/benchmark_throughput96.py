"""Real complete-batch benchmark in an isolated ClickHouse database; no production writes."""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import orjson
from openalex_api.backend import Backend, database, sql_string
from openalex_api.config import ROOT, atomic_json
from openalex_api.ingest import flush
from openalex_api import resources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=1800)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--legacy-baseline", action="store_true")
    args = parser.parse_args()
    # Read credentials only in memory; output contains no raw documents or configuration.
    production = ROOT.parent / "openalex-api"
    backend_type = Backend
    writer = flush
    if args.legacy_baseline:
        import importlib.util

        def baseline(module):
            spec = importlib.util.spec_from_file_location(
                "openalex_api.benchmark_legacy_" + module,
                ROOT / "baseline/openalex_api" / (module + ".py"),
            )
            loaded = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loaded)
            return loaded

        backend_type = baseline("backend").Backend
        writer = baseline("ingest").flush
    b = backend_type(writer=True)
    if args.legacy_baseline:
        b.ch_stream = lambda query: iter(b.ch(query))
    b.cfg = json.loads((production / "config/local.json").read_text())
    b.cfg.update(resources.PROFILE)
    g = args.resume.name if args.resume else "test_throughput_" + uuid.uuid4().hex[:12]
    if not g.startswith("test_throughput_"):
        raise ValueError("not a benchmark fixture")
    out = args.resume if args.resume else ROOT / "reports" / g
    out.mkdir(parents=True, exist_ok=True)
    atomic_json(
        out / "owner.json",
        dict(pid=os.getpid(), generation=g, started=time.time(), seconds=args.seconds),
    )
    b.create(g, {}, search=False)
    checkpoint = sqlite3.connect(out / "checkpoints.sqlite")
    checkpoint.execute("PRAGMA synchronous=FULL")
    checkpoint.execute(
        "CREATE TABLE IF NOT EXISTS batches(variant INTEGER,sample TEXT,last_id TEXT,rows INTEGER,seconds REAL)"
    )
    source = "oa_full_20260908"
    from openalex_api.batches import bounded

    anchors = {"authors": "A5000000000", "works": "W6948335331", "dense": "W1588504500"}
    pool = {}
    for label, anchor in anchors.items():
        entity = "authors" if label == "authors" else "works"
        pool[label] = list(
            b.ch_stream(
                f"SELECT id,raw,version FROM {source}.entities FINAL WHERE entity={sql_string(entity)} AND id>{sql_string(anchor)} ORDER BY id LIMIT 20000 FORMAT JSONEachRow"
            )
        )
    groups = {
        "authors": pool["authors"][:5000],
        "works": pool["works"][:5000],
        "dense": sorted(
            pool["dense"],
            key=lambda r: len(orjson.loads(r["raw"]).get("referenced_works") or []),
            reverse=True,
        )[:5000],
        "long": sorted(pool["dense"], key=lambda r: len(r["raw"]), reverse=True)[:5000],
    }
    coverage = {
        label: dict(
            rows=len(items),
            references=sum(
                len(orjson.loads(r["raw"]).get("referenced_works") or []) for r in items
            ),
            max_raw_bytes=max(len(r["raw"].encode()) for r in items),
        )
        for label, items in groups.items()
    }
    if (
        coverage["dense"]["references"] < 50000
        or coverage["long"]["max_raw_bytes"] < 65536
    ):
        raise RuntimeError("sample coverage inadequate: " + json.dumps(coverage))
    atomic_json(
        out / "samples.json",
        dict(
            version=2, anchors=anchors, coverage=coverage, raw_batch_bytes=32 * 1024**2
        ),
    )
    summary = json.loads((out / "summary.json").read_text()) if args.resume else []
    variants = (1000,) if args.legacy_baseline else (1000, 2500, 5000)
    for variant in variants:
        if any(row["batch_rows"] == variant for row in summary):
            continue
        started = time.monotonic()
        count = 0
        batches = 0
        while time.monotonic() - started < args.seconds / len(variants):
            for sample, items in groups.items():
                entity = "authors" if sample == "authors" else "works"
                for offset in range(0, len(items), variant):
                    tick = time.monotonic()
                    keys = items[offset : offset + variant]
                    query = (
                        f"SELECT id,raw,version FROM {source}.entities FINAL WHERE entity={sql_string(entity)} AND id IN ("
                        + ",".join(sql_string(r["id"]) for r in keys)
                        + ") ORDER BY id FORMAT JSONEachRow"
                    )
                    seen = 0
                    for rows in bounded(
                        b.ch_stream(query),
                        max_rows=variant,
                        max_bytes=32 * 1024**2,
                        encode=lambda row: row["raw"].encode(),
                    ):
                        records = []
                        for r in rows:
                            version = int(r["version"])
                            raw = r["raw"].encode()
                            records.append(
                                (
                                    version % 1000000000,
                                    version // 1000000000,
                                    "benchmark-" + entity,
                                    raw,
                                    orjson.loads(raw),
                                )
                            )
                        writer(b, g, entity, records, metadata_only=True)
                        checkpoint.execute(
                            "INSERT INTO batches VALUES(?,?,?,?,?)",
                            (
                                variant,
                                sample,
                                rows[-1]["id"],
                                len(rows),
                                time.monotonic() - tick,
                            ),
                        )
                        checkpoint.commit()
                        count += len(rows)
                        seen += len(rows)
                        batches += 1
                    if seen != len(keys):
                        raise RuntimeError("sample read coverage changed")
                    if b.cfg.get("disk_budget_gib") is not None:
                        raise RuntimeError("unexpected fixed budget")
                    if __import__("shutil").disk_usage(ROOT).free < 1024 * 1024**3:
                        raise RuntimeError("benchmark space reserve")
            atomic_json(
                out / "progress.json",
                dict(
                    variant=variant,
                    rows=count,
                    batches=batches,
                    seconds=time.monotonic() - started,
                ),
            )
        summary.append(
            dict(
                batch_rows=variant,
                records=count,
                seconds=time.monotonic() - started,
                records_per_second=count / (time.monotonic() - started),
            )
        )
        atomic_json(out / "summary.json", summary)
    # Every retained fixture entity and edge must match its original production version.
    differences = {}
    for table, fields in [
        ("entities", "entity,id,version,digest"),
        ("identifiers", "entity,scheme,value,id,version"),
        ("references_out", "citing,cited,version"),
        ("references_in", "citing,cited,version"),
    ]:
        differences[table] = b.ch(
            f"SELECT count() n FROM (SELECT {fields} FROM {database(g)}.{table} FINAL) l LEFT ANTI JOIN (SELECT {fields} FROM {source}.{table} FINAL) r USING({fields}) FORMAT JSONEachRow"
        )[0]["n"]
    edges = b.ch(
        f"SELECT count() n FROM (SELECT citing,cited FROM {database(g)}.references_out FINAL) l FULL OUTER JOIN (SELECT citing,cited FROM {database(g)}.references_in FINAL) r USING(citing,cited) WHERE l.citing=0 OR r.citing=0 FORMAT JSONEachRow"
    )[0]["n"]
    passed = not any(int(v) for v in differences.values()) and not int(edges)
    atomic_json(
        out / "result.json",
        dict(
            passed=passed,
            sample_coverage_passed=True,
            sampling_version=2,
            legacy_implementation=args.legacy_baseline,
            summary=summary,
            differences=differences,
            bidirectional_difference=edges,
            improvement=summary[-1]["records_per_second"]
            / summary[0]["records_per_second"],
            minimum_seconds_met=sum(r["seconds"] for r in summary) >= 1800,
        ),
    )
    checkpoint.close()
    b.close()
    # Keep the fixture and evidence until explicitly reviewed; never clean production.


if __name__ == "__main__":
    main()
