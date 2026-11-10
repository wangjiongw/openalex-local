"""Independent exact fixture comparison against raw production sample records."""

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import orjson
from openalex_api.backend import Backend, database, sql_string
from openalex_api.config import atomic_json
from openalex_api.model import canonical_id, numeric_id, external_ids


def main():
    p = argparse.ArgumentParser()
    p.add_argument("report", type=Path)
    a = p.parse_args()
    result = json.loads(a.report.read_text())
    g = a.report.parent.name
    if not g.startswith("test_throughput_"):
        raise RuntimeError("not an isolated fixture")
    b = Backend(writer=True)
    expected = {}
    try:
        samples = a.report.parent / "samples.json"
        anchors = (
            json.loads(samples.read_text())["anchors"]
            if samples.exists()
            else {"authors": "A5000000000", "works": "W6948335331"}
        )
        for label, anchor in anchors.items():
            entity = "authors" if label == "authors" else "works"
            records = list(
                b.ch_stream(
                    f"SELECT id,raw,version FROM oa_full_20260908.entities FINAL WHERE entity={sql_string(entity)} AND id>{sql_string(anchor)} ORDER BY id LIMIT 20000 FORMAT JSONEachRow"
                )
            )
            selected = records[:5000] if label != "dense" else []
            if label == "dense" or (label == "works" and "dense" not in anchors):
                selected += sorted(
                    records,
                    key=lambda r: len(
                        orjson.loads(r["raw"]).get("referenced_works") or []
                    ),
                    reverse=True,
                )[:5000]
                selected += sorted(records, key=lambda r: len(r["raw"]), reverse=True)[
                    :5000
                ]
            for row in selected:
                expected[(entity, row["id"])] = (row["raw"], int(row["version"]))
        actual = {
            (r["entity"], r["id"]): (r["raw"], int(r["version"]))
            for r in b.ch_stream(
                f"SELECT entity,id,raw,version,digest FROM {database(g)}.entities FINAL FORMAT JSONEachRow"
            )
        }
        if actual != expected:
            raise RuntimeError("exact raw entity fixture mismatch")
        edges = set()
        identifiers = set()
        audits = set()
        for (entity, key), (raw, version) in expected.items():
            record = orjson.loads(raw)
            digest = hashlib.sha256(
                orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
            ).hexdigest()
            audits.add(
                (
                    entity,
                    key,
                    "benchmark-" + entity,
                    version % 1000000000,
                    digest,
                    version,
                )
            )
            for scheme, value in external_ids(record):
                identifiers.add((entity, scheme, value, key, version))
            if entity == "works":
                for cited in record.get("referenced_works") or []:
                    edges.add(
                        (numeric_id(key), numeric_id(canonical_id(cited)), version)
                    )
        for table in ("references_out", "references_in"):
            found = {
                (int(r["citing"]), int(r["cited"]), int(r["version"]))
                for r in b.ch_stream(
                    f"SELECT citing,cited,version FROM {database(g)}.{table} FINAL FORMAT JSONEachRow"
                )
            }
            if found != edges:
                raise RuntimeError("exact fixture reference mismatch: " + table)
        found = {
            (r["entity"], r["scheme"], r["value"], r["id"], int(r["version"]))
            for r in b.ch_stream(
                f"SELECT entity,scheme,value,id,version FROM {database(g)}.identifiers FINAL FORMAT JSONEachRow"
            )
        }
        if found != identifiers:
            raise RuntimeError("exact fixture identifier mismatch")
        found = {
            (
                r["entity"],
                r["id"],
                r["source"],
                int(r["line"]),
                r["digest"],
                int(r["version"]),
            )
            for r in b.ch_stream(
                f"SELECT entity,id,source,line,digest,version FROM {database(g)}.audit FINAL FORMAT JSONEachRow"
            )
        }
        if found != audits:
            raise RuntimeError("exact fixture source audit mismatch")
        result.update(
            exact_fixture_verified=True,
            unique_entities=len(expected),
            unique_edges=len(edges),
            unique_identifiers=len(identifiers),
            unique_source_records=len(audits),
        )
        atomic_json(a.report, result)
        print(json.dumps({k: v for k, v in result.items() if k != "summary"}))
    finally:
        b.close()


if __name__ == "__main__":
    main()
