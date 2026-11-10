"""Actual backend failure/replay tests; fixtures are never production generations."""

import os, json, hashlib, uuid, gzip
import pytest
from openalex_api import ingest, recovery, search_build
from openalex_api.backend import Backend, database, index
from openalex_api.config import ROOT, ENTITIES, atomic_json, config

pytestmark = pytest.mark.skipif(
    os.getenv("OA_LIVE_TESTS") != "1", reason="real backend opt-in"
)


@pytest.fixture(scope="module")
def data():
    g = "test_recover_" + uuid.uuid4().hex[:10]
    root = ROOT / "var/test-fixtures" / g
    for entity in ENTITIES:
        records = [
            {
                "id": "https://openalex.org/" + entity + "/example",
                "display_name": entity,
            }
        ]
        if entity == "works":
            records = [
                {
                    "id": "https://openalex.org/W" + str(n),
                    "title": "recovery " + str(n),
                    "doi": "10.1000/" + str(n),
                    "referenced_works": ["https://openalex.org/W9999"],
                }
                for n in range(1, 2002)
            ]
        path = root / "data" / entity / "updated_date=2026-01-01/part_0000.gz"
        path.parent.mkdir(parents=True)
        with gzip.open(path, "wt") as out:
            for r in records:
                out.write(json.dumps(r) + "\n")
        atomic_json(
            path.parent.parent / "manifest",
            {
                "entries": [
                    {
                        "url": "s3://openalex/data/"
                        + entity
                        + "/updated_date=2026-01-01/part_0000.gz",
                        "meta": {
                            "content_length": path.stat().st_size,
                            "record_count": len(records),
                        },
                    }
                ]
            },
        )
    original = ingest.config
    ingest.config = lambda: config() | {"snapshot": str(root)}
    try:
        ingest.create_generation(g, "full")
    finally:
        ingest.config = original
    recovery.migrate(g)
    yield g
    ingest.cleanup(g, g)


def test_each_metadata_sink_replays_without_search(data, monkeypatch):
    g = data
    with ingest.connect(g) as db:
        s = dict(db.execute("SELECT * FROM shards WHERE entity='works'").fetchone())
    b = Backend(writer=True)
    records = []
    with gzip.open(s["path"], "rb") as f:
        for n, raw in enumerate(f, 1):
            records.append((n, s["ordinal"], s["relative"], raw, json.loads(raw)))
    original = b.insert
    for fail_at in range(1, 6):
        calls = 0

        def failing(*args):
            nonlocal calls
            original(*args)
            calls += 1
            if calls == fail_at:
                raise RuntimeError("injected after sink")

        b.insert = failing
        with pytest.raises(RuntimeError, match="injected"):
            ingest.flush(b, g, "works", records[:1000], metadata_only=True)
        b.insert = original
        b.bulk = lambda *a, **k: pytest.fail("metadata must not call search")
        ingest.flush(b, g, "works", records[:1000], metadata_only=True)
    n = b.ch(
        f"SELECT count() n FROM {database(g)}.entities FINAL WHERE entity='works' FORMAT JSONEachRow"
    )[0]["n"]
    assert n == 1000
    b.close()


def test_metadata_checkpoint_resume_and_full_audit(data, monkeypatch):
    g = data
    monkeypatch.setattr(recovery, "require_stage", lambda *a: None)
    # A real persisted checkpoint; one unconfirmed batch from the previous test may replay.
    with ingest.connect(g) as db:
        s = dict(db.execute("SELECT * FROM shards WHERE entity='works'").fetchone())
    with gzip.open(s["path"], "rb") as f:
        prefix = b"".join(next(f) for _ in range(1000))
    digest = hashlib.sha256(prefix).hexdigest()
    with ingest.connect(g) as db:
        db.execute(
            "UPDATE shards SET line=1000,records=1000,metadata_line=1000,metadata_prefix_sha256=?,prefix_sha256=? WHERE entity='works'",
            (digest, digest),
        )
    monkeypatch.setattr(
        Backend, "bulk", lambda *a, **k: pytest.fail("metadata touched search")
    )
    ingest.run_import(g)
    with ingest.connect(g) as db:
        assert (
            db.execute(
                "SELECT metadata_line FROM shards WHERE entity='works'"
            ).fetchone()[0]
            == 2001
        )
    result = recovery.metadata_complete(g)
    assert result["passed"]


def test_reconcile_and_repair_without_snapshot_reads(data, monkeypatch):
    g = data
    monkeypatch.setattr(recovery, "require_stage", lambda *a: None)
    report = search_build.reconcile(g, False)
    assert not report["passed"] and report["missing"] == 2021
    result = search_build.reconcile(g, True)
    assert result["passed"]
    b = Backend(writer=True)
    b.os("DELETE", "/" + index(g, "works") + "/_doc/W1")
    # Re-auditing must not reuse stale completion after index changes.
    report = search_build.reconcile(g, False)
    assert report["missing"] == 1
    report = search_build.reconcile(g, True)
    assert report["repaired"] == 1
    assert b.mget(g, "works", ["W1"])[0]["found"]
    b.close()


def test_derived_relation_repair_preserves_old_table(data):
    from openalex_api import resume
    g=data;b=Backend(writer=True)
    try:
        b.ch(f'TRUNCATE TABLE {database(g)}.references_in')
        result=resume.audit_metadata(g)
        assert result['passed']
        count=b.ch(f'SELECT count() n FROM {database(g)}.references_in FINAL FORMAT JSONEachRow')[0]['n']
        assert int(count)==2001
        journals=list((ROOT/'var/generations'/g).glob('derived-repair-*.json'))
        assert journals
        item=json.loads(journals[-1].read_text())['tables']['references_in']
        assert item['switched'] and item['old_uuid']!=item['new_uuid']
    finally:b.close()


def test_retirement_and_atomic_replacement(data, monkeypatch):
    from openalex_api import resume
    g=data
    monkeypatch.setattr(recovery,'require_evidence',lambda *a: {'passed':True,'snapshot':'test-only'})
    retired=resume.retire(g,'synthetic failure; fixture only')
    assert retired['state']=='retired'
    b=Backend(writer=True)
    try:
        resume.prepare_search(g)
        first=json.loads((ROOT/'var/generations'/g/'indexes.json').read_text())
        resume.prepare_search(g)
        assert json.loads((ROOT/'var/generations'/g/'indexes.json').read_text())==first
        assert len(first)==21 and not set(first.values()) & set(retired['names'].values())
        for name in first.values():
            assert b.os('GET','/'+name+'/_settings')[name]['settings']['index']['store']['type']=='niofs'
    finally:
        # Retention is mandatory for real data, but these are exclusively this fixture's indexes.
        for name in retired['names'].values():b.os('DELETE','/'+name)
        b.close()
