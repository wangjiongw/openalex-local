"""Isolated test database, real CH durability and process-exit replay; no production writes."""

import gzip
import json
import os
import subprocess
import sys
import uuid
import pytest
from openalex_api import ingest, recovery
from openalex_api.backend import Backend, database
from openalex_api.config import ROOT, ENTITIES, atomic_json, generation_path, config

pytestmark = pytest.mark.skipif(
    os.getenv("OA_THROUGHPUT_LIVE") != "1", reason="isolated CH integration opt-in"
)


def test_full_range_pipeline_with_process_exit_and_replay(monkeypatch):
    g = "test_ranges_" + uuid.uuid4().hex[:10]
    fixture = ROOT / "var/test-fixtures" / g
    total = 0
    for entity in ENTITIES:
        records = [
            {
                "id": "https://openalex.org/" + entity + "/example",
                "display_name": entity,
            }
        ]
        if entity in ("works", "authors"):
            prefix = "W" if entity == "works" else "A"
            records = [
                dict(
                    id="https://openalex.org/" + prefix + str(i),
                    display_name=entity + str(i),
                    ids={"openalex": "https://openalex.org/" + prefix + str(i)},
                )
                for i in range(1, 5202)
            ]
        if entity == "works":
            records[0]["referenced_works"] = [
                "https://openalex.org/W" + str(i) for i in range(100, 100201)
            ]
        path = fixture / "data" / entity / "updated_date=2026-01-01/part.gz"
        path.parent.mkdir(parents=True)
        with gzip.open(path, "wt") as out:
            for r in records:
                out.write(json.dumps(r) + "\n")
        atomic_json(
            path.parent.parent / "manifest",
            dict(
                entries=[
                    dict(
                        url="s3://openalex/data/"
                        + entity
                        + "/updated_date=2026-01-01/part.gz",
                        meta=dict(
                            content_length=path.stat().st_size,
                            record_count=len(records),
                        ),
                    )
                ]
            ),
        )
        total += len(records)
    original = Backend.create
    monkeypatch.setattr(
        Backend,
        "create",
        lambda self, g, shards, search=True: original(self, g, shards, search=False),
    )
    monkeypatch.setattr(ingest, "config", lambda: config() | {"snapshot": str(fixture)})
    ingest.create_generation(g, "full")
    recovery.migrate(g)
    monkeypatch.setattr(recovery, "require_stage", lambda *a: None)
    # Exit after all sinks acknowledge, before any shard checkpoint is committed.
    program = """
import os,json
from openalex_api import ingest
from openalex_api.backend import Backend
with ingest.connect(GENERATION) as db: s=dict(db.execute("SELECT * FROM shards WHERE entity='works'").fetchone())
original=ingest.flush
def crash(*a,**k):
 original(*a,**k)
 os._exit(17)
ingest.flush=crash
ingest.import_shard(GENERATION,s,0,0,metadata_only=True)
""".replace("GENERATION", repr(g))
    result = subprocess.run([sys.executable, "-c", program], cwd=ROOT, timeout=120)
    assert result.returncode == 17
    with ingest.connect(g) as db:
        assert (
            db.execute(
                "SELECT metadata_line FROM shards WHERE entity='works'"
            ).fetchone()[0]
            == 0
        )
    ingest.run_import(g)
    with ingest.connect(g) as db:
        assert (
            db.execute("SELECT sum(metadata_line) FROM shards").fetchone()[0] == total
        )
    result = recovery.audit(g, force=True)
    assert result["passed"] and result["records_checked"] == total
    # Reusing ranges must never empty the expected tables or double-count confirmations.
    second = recovery.audit(g, force=True)
    assert second["passed"] and second["records_checked"] == total
    b = Backend(writer=True)
    try:
        for table in ("references_in", "references_out"):
            assert (
                int(
                    b.ch(
                        f"SELECT count() n FROM {database(g)}.{table} FINAL FORMAT JSONEachRow"
                    )[0]["n"]
                )
                == 100101
            )
        # Change table identity to invalidate exact-difference completion and detect loss.
        b.ch(
            f"RENAME TABLE {database(g)}.references_in TO {database(g)}.references_in_retained"
        )
        original(b, g, {}, search=False)
        failed = recovery.audit(g, force=True)
        assert not failed["passed"] and any(
            "references_in" in x for x in failed["errors"]
        )
    finally:
        b.close()
    atomic_json(
        ROOT / "reports" / ("integration-" + g + ".json"),
        dict(
            passed=True,
            generation=g,
            records=total,
            process_exit_replayed=True,
            exact_missing_edges_detected=True,
        ),
    )


def test_direct_search_and_bidirectional_versions(monkeypatch):
    from openalex_api import direct_search, search_build
    from openalex_api.backend import index

    reports = sorted(
        (ROOT / "reports").glob("integration-test_ranges_*.json"),
        key=lambda p: p.stat().st_mtime,
    )
    assert reports
    source = json.loads(reports[-1].read_text())["generation"]
    g = "test_search96_" + uuid.uuid4().hex[:10]
    p = generation_path(g)
    p.mkdir()
    meta = json.loads((generation_path(source) / "generation.json").read_text())
    meta.update(generation=g, metadata_frozen=True)
    atomic_json(p / "generation.json", meta)
    with ingest.connect(source) as old:
        import sqlite3

        with sqlite3.connect(p / "tasks.sqlite") as dest:
            old.backup(dest)
    b = Backend(writer=True)
    try:
        b.os(
            "PUT",
            "/_cluster/settings",
            {
                "persistent": {
                    "cluster.routing.allocation.disk.watermark.low": "700gb",
                    "cluster.routing.allocation.disk.watermark.high": "500gb",
                    "cluster.routing.allocation.disk.watermark.flood_stage": "300gb",
                }
            },
        )
        b.create(g, {})
        for table in ("entities", "audit", "identifiers", "references_out"):
            b.ch(
                f"INSERT INTO {database(g)}.{table} SELECT * FROM {database(source)}.{table}"
            )
        b.ch(
            f"INSERT INTO {database(g)}.references_in SELECT * FROM {database(source)}.references_out"
        )
        atomic_json(p / "replacement-indexes.json", {e: index(g, e) for e in ENTITIES})
        monkeypatch.setattr(
            recovery, "require_evidence", lambda *a, **k: {"passed": True}
        )
        monkeypatch.setattr(recovery, "require_stage", lambda *a, **k: None)
        direct_search.run(g)
        result = search_build.reconcile(g, False)
        assert (
            result["passed"]
            and not result["missing"]
            and not result["extra"]
            and not result["unexpected_versions"]
        )
        direct_search.run(g)
        b.os("DELETE", "/" + index(g, "works") + "/_doc/W1")
        missing = search_build.reconcile(g, False)
        assert not missing["passed"] and missing["missing"] == 1
        atomic_json(
            ROOT / "reports" / ("search-integration-" + g + ".json"),
            dict(
                passed=True,
                generation=g,
                checked=result["checked"],
                deleted_record_detected=True,
            ),
        )
    finally:
        b.close()


def test_archive_reuses_original_closed_topics_after_mapping_change(monkeypatch):
    from openalex_api import storage, resume
    from openalex_api.backend import index
    from openalex_api.model import mapping

    reports = sorted(
        (ROOT / "reports").glob("search-integration-test_search96_*.json"),
        key=lambda p: p.stat().st_mtime,
    )
    assert reports
    g = json.loads(reports[-1].read_text())["generation"]
    saved = storage.archive(g)
    assert saved["passed"] and saved["snapshot_uuid"]
    retired = resume.retire(g, "isolated snapshot probe test")
    b = Backend(writer=True)
    try:
        names = {e: index(g, e) for e in ENTITIES}
        new = names["topics"] + "-probe-replacement"
        b.os("PUT", "/" + new, mapping(1))
        names["topics"] = new
        atomic_json(generation_path(g) / "indexes.json", names)
        (generation_path(g) / "archive.json").rename(
            generation_path(g) / "archive-preserved-test.json"
        )
        repeated = storage.archive(g)
        assert repeated["snapshot_uuid"] == saved["snapshot_uuid"]
        assert repeated["restore_index"] == retired["names"]["topics"]
        assert b.os(
            "GET",
            "/_cat/indices/"
            + retired["names"]["topics"]
            + "?format=json&h=index,status",
        ) == [{"index": retired["names"]["topics"], "status": "close"}]
    finally:
        b.close()
