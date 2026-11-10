import hashlib
import json
import sqlite3
import threading
from queue import Queue
from unittest.mock import Mock
import orjson
import pytest
from openalex_api import batches, intervals, resources, proofs, ledger


def test_ranges_resume_preserves_confirmed_prefix_and_all_entities(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(intervals, "ENTITIES", ["works", "authors", "topics"])
    monkeypatch.setattr(resources, "checkpoint", lambda *a, **k: None)
    rows = {"authors": ["A3", "A4", "A5", "A6", "A7"], "topics": ["T1"]}

    def stream(query):
        entity = "authors" if "'authors'" in query else "topics"
        lower = query.split("AND id>")[1].split(" ORDER")[0].strip("'")
        return iter({"id": x} for x in rows[entity] if x > lower)

    b = Mock()

    # Respect the limited streaming source (one range at a time).
    def limited(query):
        return iter(list(stream(query))[:2])

    b.ch_stream = limited
    db = intervals.connect(tmp_path / "ranges.sqlite")
    legacy = {
        "entities": {"works": {"done": True}, "authors": {"last_id": "A2"}},
        "checked": 100,
    }
    intervals.plan(db, b, "test", legacy, size=2)
    first = [dict(r) for r in db.execute("SELECT * FROM tasks ORDER BY entity,lo")]
    assert [(r["lo"], r["hi"], r["expected"]) for r in first] == [
        ("A2", "A4", 2),
        ("A4", "A6", 2),
        ("A6", "A7", 1),
        ("", "T1", 1),
    ]
    b.ch_stream = lambda _: pytest.fail("completed planning must not reread")
    intervals.plan(db, b, "test", legacy, size=2)
    intervals.confirm(db, first[0], "A3", 1)
    db.close()
    db = intervals.connect(tmp_path / "ranges.sqlite")
    saved = dict(db.execute("SELECT * FROM tasks WHERE lo='A2'").fetchone())
    assert saved["checked"] == 1 and saved["last_id"] == "A3"
    with pytest.raises(RuntimeError, match="stale"):
        intervals.confirm(db, first[0], "A4", 2)
    intervals.confirm(db, saved, "A4", 1)
    assert db.execute("SELECT state FROM tasks WHERE lo='A2'").fetchone()[0] == "done"
    db.close()


def test_oversized_row_is_never_truncated():
    rows = [{"text": "small"}, {"text": "x" * 5000}, {"text": "last"}]
    chunks = list(batches.bounded(rows, max_rows=2, max_bytes=100))
    assert [len(x) for x in chunks] == [1, 1, 1]
    assert [r for batch in chunks for r in batch] == rows


def test_partial_edge_ack_does_not_return_checkpoint(monkeypatch):
    raw = orjson.dumps(
        {
            "id": "https://openalex.org/W1",
            "referenced_works": ["https://openalex.org/W2", "https://openalex.org/W3"],
        }
    )
    row = dict(
        id="W1", raw=raw.decode(), digest=hashlib.sha256(raw).hexdigest(), version=1
    )
    b = Mock()
    b.ch_stream = lambda _: (x for x in [row])
    b.insert.side_effect = [None, TimeoutError("injected second subbatch")]
    monkeypatch.setattr(intervals, "Backend", lambda **kw: b)
    monkeypatch.setattr(resources, "checkpoint", lambda *a, **k: None)
    monkeypatch.setattr(intervals, "config", lambda: {})
    real = intervals.bounded
    monkeypatch.setattr(
        intervals,
        "bounded",
        lambda rows, **kw: real(rows, **kw) if kw else real(rows, max_rows=1),
    )
    task = dict(entity="works", lo="", hi="W1", last_id="", checked=0, expected=1)
    with pytest.raises(TimeoutError):
        intervals.audit_batch("test", task, "edges", "ids")
    assert task["last_id"] == ""
    b.insert.side_effect = None
    last, n = intervals.audit_batch("test", task, "edges", "ids")
    assert (last, n) == ("W1", 1)


def test_range_count_mismatch_fails_without_checkpoint(tmp_path):
    db = intervals.connect(tmp_path / "state.sqlite")
    db.execute(
        'INSERT INTO tasks(entity,lo,hi,expected,last_id) VALUES("works","","W3",3,"")'
    )
    db.commit()
    task = dict(db.execute("SELECT * FROM tasks").fetchone())
    with pytest.raises(RuntimeError, match="coverage"):
        intervals.confirm(db, task, "W3", 2)
    assert db.execute("SELECT checked FROM tasks").fetchone()[0] == 0


def test_resource_watermarks_and_peak_admission():
    G = resources.GiB
    assert resources.decide(299 * G, 0) == "disk-emergency"
    assert resources.decide(499 * G, 0) == "disk-pause"
    assert resources.decide(699 * G, 0, large=True) == "disk-admission"
    assert (
        resources.decide(1100 * G, 0, large=True, additional=500 * G, temporary=200 * G)
        == "disk-admission"
    )
    assert resources.decide(2000 * G, 960 * G) == "memory-emergency"
    assert resources.decide(2000 * G, 896 * G) == "memory-pause"
    assert resources.decide(2000 * G, 0) is None


def test_auto_resume_only_disk_once_after_continuous_recovery():
    G = resources.GiB
    s = {"reason": "disk-pause"}
    assert not resources.disk_resume_ready(s, 1100 * G, 0, 0, 0)
    assert not resources.disk_resume_ready(s, 900 * G, 0, 0, 599)
    assert not resources.disk_resume_ready(s, 1100 * G, 0, 0, 600)
    assert resources.disk_resume_ready(s, 1100 * G, 0, 0, 1200)
    assert not resources.disk_resume_ready(s, 1100 * G, 0, 0, 2000)
    assert not resources.disk_resume_ready({"reason": "user"}, 1100 * G, 0, 0, 3000)


def test_ramp_requires_complete_batch_throughput_gain():
    sample = {"total_rss_bytes": 0, "disk_free_bytes": 2048 * resources.GiB}
    ramp = resources.Ramp([2, 4, 8], now=0)
    assert ramp.observe(9000, sample, False, now=899) == 2
    assert ramp.observe(10, sample, False, now=900) == 4
    assert ramp.observe(9000, sample, False, now=1800) == 2
    assert ramp.stopped


def test_numa_pool_prefers_complete_nodes(tmp_path):
    for i in range(3):
        p = tmp_path / f"node{i}"
        p.mkdir()
        (p / "cpulist").write_text(f"{i * 4}-{i * 4 + 3}")
    assert resources.cpu_set([0, 1, 4, 5, 6, 7, 8, 9, 10, 11], 8, tmp_path) == [
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
    ]


def test_prefetch_propagates_failure_without_hanging():
    def source():
        yield [1]
        raise OSError("ENOSPC")

    it = intervals.prefetch(source())
    assert next(it) == [1]
    with pytest.raises(OSError, match="ENOSPC"):
        next(it)


def test_coordinator_transaction_rollback(tmp_path, monkeypatch):
    from openalex_api import ingest

    path = tmp_path / "ledger.sqlite"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE checkpoints(n INTEGER)")
    db.close()
    monkeypatch.setattr(ingest, "connect", lambda _: sqlite3.connect(path))
    requests = Queue()
    responses = {}
    thread = threading.Thread(
        target=ledger.coordinate, args=("test", requests, responses)
    )
    thread.start()
    requests.put(
        ("bad", [("INSERT INTO checkpoints VALUES(?)", (1,)), ("INVALID SQL", ())])
    )
    requests.put(("good", [("INSERT INTO checkpoints VALUES(?)", (2,))]))
    requests.put(None)
    thread.join(timeout=5)
    assert not thread.is_alive() and responses["bad"] and responses["good"] is None
    db = sqlite3.connect(path)
    assert db.execute("SELECT * FROM checkpoints").fetchall() == [(2,)]


def test_semantic_identity_unaffected_by_execution_parameters(monkeypatch):
    monkeypatch.setattr(
        proofs, "read_generation", lambda _: dict(manifest_sha256="abc")
    )
    before = proofs.semantic("test")
    monkeypatch.setattr(
        proofs, "config", lambda: dict(audit_workers=16, batch_size=5000)
    )
    assert proofs.semantic("test") == before


def test_extracted_semantics_exactly_match_legacy_sinks():
    import importlib.util
    from openalex_api import ingest
    from openalex_api.config import ROOT

    spec = importlib.util.spec_from_file_location(
        "openalex_api.legacy_ingest", ROOT / "baseline/openalex_api/ingest.py"
    )
    legacy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(legacy)
    records = [
        {
            "id": "https://openalex.org/W1",
            "doi": "https://doi.org/10.123/a",
            "referenced_works": [
                "https://openalex.org/W2",
                "https://openalex.org/W2",
                "https://openalex.org/W3",
            ],
        },
        {
            "id": "https://openalex.org/W4",
            "ids": {"openalex": "https://openalex.org/W4", "doi": "10.123/b"},
        },
    ]
    rows = [(i, 2, "fixture", orjson.dumps(r), r) for i, r in enumerate(records, 1)]

    def capture(function):
        result = {}
        b = Mock()

        def insert(g, table, rows):
            result.setdefault(table, []).extend(rows)

        b.insert = insert
        function(b, "test", "works", rows, metadata_only=True)
        return {
            table: sorted(
                values, key=lambda v: orjson.dumps(v, option=orjson.OPT_SORT_KEYS)
            )
            for table, values in result.items()
        }

    assert capture(legacy.flush) == capture(ingest.flush)


def test_restore_probe_recloses_source_on_failure(tmp_path, monkeypatch):
    from openalex_api import storage, resume

    monkeypatch.setattr(storage, "generation_path", lambda _: tmp_path)
    b = Mock()
    b.os.return_value = [{"index": "old", "status": "close"}]
    close = Mock()
    monkeypatch.setattr(resume, "ensure_retired_index_closed", close)
    with pytest.raises(OSError):
        with storage.probe_source_open(b, "test", "old"):
            raise OSError("probe failure")
    close.assert_called_once_with(b, "old")
    assert (
        json.loads((tmp_path / "restore-probe-open.json").read_text())["state"]
        == "closed"
    )


def test_legacy_migration_is_idempotent_and_preserves_prefix(tmp_path, monkeypatch):
    from openalex_api import migration96, ingest

    monkeypatch.setattr(migration96, "generation_path", lambda _: tmp_path)
    monkeypatch.setattr(ingest, "connect", lambda _: open_db())
    monkeypatch.setattr(proofs, "semantic", lambda _: "new-semantic")

    def open_db():
        db = sqlite3.connect(tmp_path / "tasks.sqlite")
        db.row_factory = sqlite3.Row
        return db

    with open_db() as db:
        db.execute(
            "CREATE TABLE shards(relative TEXT,line INTEGER,state TEXT,metadata_line INTEGER,metadata_state TEXT,selected INTEGER)"
        )
        db.execute('INSERT INTO shards VALUES("a.gz",10,"running",10,"running",1)')
        shards = [dict(row) for row in db.execute("SELECT * FROM shards")]
    old = migration96.old_runhash(shards, "old-signature")
    cursor = dict(
        runhash=old,
        checked=9,
        entities={
            "works": {"done": True, "last_id": "W9"},
            "authors": {"last_id": "A2", "done": False},
        },
    )
    (tmp_path / "metadata-audit-cursor.json").write_text(json.dumps(cursor))
    tables = [
        {"name": name, "uuid": name + "-uuid"}
        for name in (
            "entities",
            "audit",
            "recovery_edges_" + old[:12],
            "recovery_ids_" + old[:12],
        )
    ]
    b = Mock()
    b.ch.return_value = tables
    first = migration96.migrate("test", b, "old-signature", {"example.py": "sha"})
    assert first["state"] == "complete"
    assert json.loads((tmp_path / "metadata-audit-cursor.json").read_text()) == cursor
    assert migration96.inherited_runhash("test", shards, b) == old
    assert migration96.migrate("test", b, "old-signature", {}) == first
    assert (tmp_path / "before-throughput96/tasks.sqlite").exists()
    b.ch.return_value = tables[:-1]
    with pytest.raises(RuntimeError, match="identity changed"):
        migration96.inherited_runhash("test", shards, b)


def test_sqlite_full_does_not_advance_interval(tmp_path):
    db = intervals.connect(tmp_path / "full.sqlite")
    db.execute(
        'INSERT INTO tasks(entity,lo,hi,expected,last_id) VALUES("works","","W1",1,"")'
    )
    db.execute("CREATE TABLE payload(data BLOB)")
    db.execute(
        "CREATE TRIGGER full_disk AFTER UPDATE ON tasks BEGIN INSERT INTO payload VALUES(zeroblob(1048576)); END"
    )
    db.commit()
    current = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute("PRAGMA max_page_count=" + str(current))
    task = dict(db.execute("SELECT * FROM tasks").fetchone())
    with pytest.raises(sqlite3.OperationalError, match="full"):
        intervals.confirm(db, task, "W1", 1)
    assert db.execute("SELECT last_id,checked FROM tasks").fetchone()[:] == ("", 0)
    db.close()


def test_large_stream_query_uses_post_body(monkeypatch):
    import httpx
    from openalex_api.backend import Backend

    seen = []

    def response(request):
        seen.append(request)
        return httpx.Response(200, content=b'{"id":"W1"}\n')

    b = Backend(writer=True)
    b.http.close()
    b.http = httpx.Client(transport=httpx.MockTransport(response))
    b.cfg["execution_profile"] = "test"
    query = "SELECT " + ("x" * 100000)
    try:
        assert list(b.ch_stream(query)) == [{"id": "W1"}]
    finally:
        b.close()
    assert seen[0].content == query.encode()
    assert "query=" not in str(seen[0].url)


def test_stream_preserves_unicode_line_separators(monkeypatch):
    import httpx
    from openalex_api.backend import Backend

    row = {"id": "W1", "raw": "x" * 65530 + "α\u0085β\u2028γ\u2029δ"}
    encoded = orjson.dumps(row) + b"\n" + orjson.dumps({"id": "W2"}) + b"\n"
    b = Backend(writer=True)
    b.http.close()
    b.http = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=encoded))
    )
    b.cfg["execution_profile"] = "test"
    try:
        assert list(b.ch_stream("SELECT fixture")) == [row, {"id": "W2"}]
    finally:
        b.close()


def test_storage_promotion_does_not_invalidate_canary_proof(tmp_path, monkeypatch):
    monkeypatch.setattr(proofs, "ROOT", tmp_path)
    monkeypatch.setattr(proofs, "data_identity", lambda _: "same-tables")
    monkeypatch.setattr(proofs, "semantic", lambda _: "data")
    monkeypatch.setattr(proofs, "file_hash", lambda path: str(path))
    (tmp_path / "var").mkdir()
    monkeypatch.setattr(proofs, "search_epoch", lambda _: "epoch")
    before = proofs.dependencies("test", "storage-verification")
    archive = proofs.dependencies("test", "archive")
    (tmp_path / "var/storage-profile.json").write_text('{"allow_mmap":false}')
    assert proofs.dependencies("test", "storage-verification") == before
    assert proofs.dependencies("test", "archive") == archive
    assert "storage_profile" in proofs.dependencies("test", "final-index-check")


def test_ramp_candidate_rolls_back_on_merge_pressure():
    sample = {"total_rss_bytes": 0, "disk_free_bytes": 2048 * resources.GiB}
    ramp = resources.Ramp([2, 4, 8], now=0)
    assert ramp.observe(9000, sample, False, now=900) == 4
    assert ramp.observe(100, sample, True, now=930) == 2
    assert ramp.stopped


def test_backend_error_does_not_log_raw_document():
    import httpx
    from openalex_api.backend import checked, BackendError

    response = httpx.Response(
        500,
        text='Code: 27. Cannot parse {"raw":"private full paper body"}',
        request=httpx.Request("POST", "http://example.test"),
    )
    with pytest.raises(BackendError) as error:
        checked(response)
    assert "code=27" in str(error.value)
    assert "private" not in str(error.value)


def test_disk_auto_resume_never_overrides_data_failure(tmp_path, monkeypatch):
    from openalex_api import config as settings, manage

    monkeypatch.setattr(resources, "ROOT", tmp_path)
    monkeypatch.setattr(
        settings, "config", lambda: {"execution_profile": "throughput96"}
    )
    monkeypatch.setattr(
        manage,
        "resource_status",
        lambda: {"disk_free_bytes": 2048 * resources.GiB, "total_rss_bytes": 0},
    )
    monkeypatch.setattr(manage, "identity", lambda _: None)
    start = Mock()
    monkeypatch.setattr(manage, "start", start)
    p = tmp_path / "var/generations/test"
    p.mkdir(parents=True)
    (p / "execution-run.json").write_text('{"until":"complete"}')
    (p / "pause-request.json").write_text('{"reason":"disk-pause","healthy_since":0}')
    (p / "control.json").write_text('{"state":"failed","reason":"checksum"}')
    resources.monitor_tick()
    start.assert_not_called()
    assert "auto_attempted" not in json.loads((p / "pause-request.json").read_text())


def test_import_cancellation_keeps_disk_pause_reason(tmp_path, monkeypatch):
    from openalex_api import config as settings, ingest, diagnostics

    monkeypatch.setattr(settings, "generation_path", lambda _: tmp_path)
    monkeypatch.setattr(resources, "generation_path", lambda _: tmp_path)
    (tmp_path / "pause-request.json").write_text('{"reason":"disk-pause"}')

    @diagnostics.guarded_stage
    def cancelled(g):
        raise ingest.ImportCancelled("import paused")

    assert cancelled("test") == {"paused": True, "reason": "disk-pause"}
    assert json.loads((tmp_path / "control.json").read_text())["state"] == "paused"


def test_disk_emergency_stops_native_project_writers_only(tmp_path, monkeypatch):
    from openalex_api import config as settings, manage

    monkeypatch.setattr(resources, "ROOT", tmp_path)
    monkeypatch.setattr(
        settings, "config", lambda: {"execution_profile": "throughput96"}
    )
    services = {
        name: {"running": True}
        for name in ("clickhouse", "opensearch", "canary", "recover-test", "vllm")
    }
    monkeypatch.setattr(
        manage,
        "resource_status",
        lambda: {
            "disk_free_bytes": 299 * resources.GiB,
            "total_rss_bytes": 0,
            "services": services,
        },
    )
    stop = Mock()
    monkeypatch.setattr(manage, "stop", stop)
    resources.monitor_tick()
    assert [call.args[0] for call in stop.call_args_list] == [
        "recover-test",
        "canary",
        "opensearch",
        "clickhouse",
    ]


def test_search_evidence_is_invalidated_by_allocation_change(tmp_path, monkeypatch):
    from openalex_api import recovery

    monkeypatch.setattr(proofs, "data_identity", lambda _: "same-tables")
    monkeypatch.setattr(proofs, "semantic", lambda _: "same-data")
    monkeypatch.setattr(proofs, "file_hash", lambda _: "same-code")
    monkeypatch.setattr(proofs, "search_epoch", lambda _: "before")
    monkeypatch.setattr(recovery, "generation_path", lambda _: tmp_path)
    recovery.evidence("test", "search-complete", passed=True, epoch="before")
    assert recovery.require_evidence("test", "search-complete")["passed"]
    monkeypatch.setattr(proofs, "search_epoch", lambda _: "after")
    with pytest.raises(RuntimeError, match="dependencies"):
        recovery.require_evidence("test", "search-complete")
    with pytest.raises(RuntimeError, match="allocation"):
        recovery.evidence("test", "search-complete", passed=True, epoch="before")


def test_metadata_proof_rejects_replaced_tables(tmp_path, monkeypatch):
    from openalex_api import recovery

    monkeypatch.setattr(proofs, "semantic", lambda _: "same-data")
    monkeypatch.setattr(proofs, "data_identity", lambda _: "original-uuids")
    monkeypatch.setattr(recovery, "generation_path", lambda _: tmp_path)
    recovery.evidence("test", "metadata-complete", passed=True)
    assert recovery.require_evidence("test", "metadata-complete")["passed"]
    monkeypatch.setattr(proofs, "data_identity", lambda _: "replacement-uuids")
    with pytest.raises(RuntimeError, match="dependencies"):
        recovery.require_evidence("test", "metadata-complete")
