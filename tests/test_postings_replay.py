import json

import pytest

from openalex_api import postings_replay as replay


def test_runtime_write_probe_binds_to_engine_pid(tmp_path):
    for directory, label in (("checksum-capture", "checksum"), ("merge-context", "merge")):
        path = tmp_path / directory / "agent-write-probe.txt"
        path.parent.mkdir()
        path.write_text(f"pid=123 label={label}\n")
    replay.require_write_probes(tmp_path, 123)
    with pytest.raises(RuntimeError, match="write probe"):
        replay.require_write_probes(tmp_path, 456)


def test_log_scan_is_incremental_and_catches_new_failure(tmp_path):
    log = tmp_path / "engine.log"
    log.write_text("healthy\n")
    cursor = {}
    assert replay.scan_logs([tmp_path], cursor) == len("healthy\n")
    assert replay.scan_logs([tmp_path], cursor) == 0
    with log.open("a") as stream:
        stream.write("checksum failed expected=1 actual=2\n")
    with pytest.raises(RuntimeError, match="storage failure"):
        replay.scan_logs([tmp_path], cursor)


def test_replay_is_pinned_to_frozen_generation():
    with pytest.raises(ValueError, match="pinned"):
        replay.run("other_generation")


def test_explicit_source_index_is_used_for_mget():
    controller = replay.Replay.__new__(replay.Replay)
    backend = type("BackendStub", (), {})()
    calls = []
    backend.os = lambda method, path, body: calls.append((method, path, body)) or {
        "docs": [{"_id": "W1", "_version": 1, "found": True}]
    }
    controller.backends = {"default": backend}
    assert controller.mget_index("default", ["W1"])[0]["_id"] == "W1"
    assert calls[0][1].startswith("/" + replay.SOURCE_INDEX_NAME + "/")


def test_clickhouse_pages_are_drained_before_slow_consumer(monkeypatch):
    monkeypatch.setattr(replay, "BATCH", 2)
    controller = replay.Replay.__new__(replay.Replay)
    controller.g = "full_20260908"
    backend = type("BackendStub", (), {})()
    queries, closed = [], []
    pages = iter([[{"id":"W1","version":1},{"id":"W2","version":1}],
                  [{"id":"W3","version":1}]])
    def stream(query):
        queries.append(query)
        try:
            yield from next(pages)
        finally:
            closed.append(query)
    backend.ch_stream = stream
    backend.close = lambda: None
    controller.backends = {"default": backend}
    batches = controller.row_batches("", "W9")
    assert len(next(batches)) == 2
    assert len(closed) == 1  # No HTTP response remains open while the consumer waits.
    assert len(next(batches)) == 1
    assert list(batches) == []
    assert len(queries) == len(closed) == 2
    assert all("ORDER BY id LIMIT 2" in query and "id<='W9'" in query for query in queries)
    assert "id>'W2'" in queries[1]


def test_clickhouse_paging_preserves_errors_and_closes_response():
    controller = replay.Replay.__new__(replay.Replay)
    controller.g = "full_20260908"
    closed = []
    def broken(query):
        try:
            yield {"id":"W1","version":1}
            raise RuntimeError("incomplete response")
        finally:
            closed.append(True)
    backend = type("BackendStub", (), {"ch_stream":staticmethod(broken)})()
    controller.backends = {"default":backend}
    with pytest.raises(RuntimeError, match="incomplete response"):
        next(controller.row_batches(""))
    assert closed == [True]


def test_search_page_uses_stable_id_sort_and_version():
    controller = replay.Replay.__new__(replay.Replay)
    calls = []
    backend = type("BackendStub", (), {})()
    backend.os = lambda method, path, body: calls.append((method, path, body)) or {
        "hits": {"hits": [{"_id": "W2", "_version": 2, "sort": ["W2"]}]},
        "_shards": {"failed": 0},
        "timed_out": False,
    }
    controller.backends = {"default": backend}
    hits = controller.search_page("default", "pit", "W1", 1)
    assert hits[0]["_id"] == "W2"
    body = calls[0][2]
    assert body["sort"] == [{"id": "asc"}]
    assert body["version"] is True and body["search_after"] == ["W1"]


def test_search_page_retains_arm_and_backend_failure_details():
    from openalex_api.backend import BackendError
    controller = replay.Replay.__new__(replay.Replay)
    controller.state = {"trial": 2}
    error = BackendError("Search shard failure")
    error.backend_context = {"shards": {"failed": 1}}
    def fail(*args): raise error
    controller.backends = {"c1": type("BackendStub", (), {"os": staticmethod(fail)})()}
    with pytest.raises(BackendError) as caught:
        controller.search_page("c1", "pit", "W12", 100)
    assert caught.value is error
    assert error.backend_context == {"shards":{"failed":1},"arm":"c1","after":"W12","size":100,"trial":2}


def test_acknowledge_uses_eight_bounded_reads_before_one_commit(monkeypatch):
    controller = replay.Replay.__new__(replay.Replay)
    controller.state = {"common_after": "", "common_checked": 0, "phase_started": 0, "expected": 4000}
    controller.backends = {"default": object(), "c1": object()}
    controller.trial = lambda: {"arms": {"default": {}, "c1": {}}}
    calls = []
    def mget(arm, keys):
        calls.append((arm, len(keys)))
        return [{"_id": key, "_version": 1, "found": True} for key in keys]
    controller.mget_index = mget
    controller.guard = lambda: None
    controller.save = lambda **values: None
    rows = [{"id": f"W{n:04d}", "version": 1} for n in range(4000)]
    controller.acknowledge(rows)
    assert sorted(calls) == sorted([(arm, 1000) for arm in replay.ARMS for _ in range(4)])
    assert controller.state["common_checked"] == 4000


def test_common_cursor_requires_both_arms():
    state = {"common_after": "W1", "common_checked": 1}
    rows = [{"id": "W2", "version": 2}]
    with pytest.raises(RuntimeError, match="both replay arms"):
        replay.commit_common(state, rows, {"default": True, "c1": False})
    assert state == {"common_after": "W1", "common_checked": 1}


def test_common_cursor_advances_only_after_matched_acknowledgements():
    state = {"common_after": "W1", "common_checked": 1}
    replay.commit_common(
        state,
        [{"id": "W2", "version": 2}, {"id": "W3", "version": 3}],
        {"default": True, "c1": True},
    )
    assert state["common_after"] == "W3"
    assert state["common_checked"] == 3


@pytest.mark.parametrize(
    "captured,disk,actual,software,jvm,stored,expected",
    [
        (b"read", b"disk", "aa", "aa", "aa", "bb", "transient-read-content"),
        (b"same", b"same", "aa", "bb", "bb", "bb", "checksum-execution-inconsistency"),
        (b"same", b"same", "aa", "aa", "aa", "bb", "runtime-disk-content-corruption"),
    ],
)
def test_failure_classification(captured, disk, actual, software, jvm, stored, expected):
    meta = {
        "captured": True,
        "complete": True,
        "error": "checksum failed expected=bb actual=" + actual,
        "software_crc32": software,
        "jvm_crc32_recomputed": jvm,
        "stored_crc32": stored,
    }
    assert replay._capture_classification(meta, captured, disk) == expected


def test_compound_slice_parses_vint_entry_count(tmp_path):
    def vint(value):
        out = bytearray()
        while value > 127:
            out.append((value & 127) | 128)
            value >>= 7
        out.append(value)
        return bytes(out)

    def string(value):
        data = value.encode()
        return vint(len(data)) + data

    member = b"known-postings-bytes"
    cfs = tmp_path / "_d7.cfs"
    cfs.write_bytes(b"x" * 7 + member)
    cfe = (
        bytes.fromhex("3fd76c17")
        + string("Lucene90CompoundEntries")
        + (0).to_bytes(4, "big")
        + b"i" * 16
        + string("")
        + vint(1)
        + string("_Lucene103_0.pos")
        + (7).to_bytes(8, "little", signed=True)
        + len(member).to_bytes(8, "little", signed=True)
    )
    (tmp_path / "_d7.cfe").write_bytes(cfe)
    assert replay.compound_slice(cfs, "_d7_Lucene103_0.pos") == member


def test_incomplete_capture_is_not_overclassified():
    assert replay._capture_classification({"captured": False}, None, b"disk") == "capture-incomplete"


def test_save_accepts_business_state_field(tmp_path, monkeypatch):
    monkeypatch.setattr(replay, "BASE", tmp_path / "reports")
    monkeypatch.setattr(replay, "generation_path", lambda g: tmp_path / "generation")
    state = replay.new_state("full_20260908")
    replay.save_state("full_20260908", state, state="running", phase="copying")
    assert json.loads(replay.progress_path("full_20260908").read_text())["state"] == "running"


def test_public_status_cannot_promote_formal_gate(tmp_path):
    state = replay.new_state("full_20260908")
    state.update(production_gate_passed=True, throughput_records_per_second=123, eta_seconds=45)
    status = replay.public_state(state, tmp_path / "progress.json")
    assert status["production_gate_passed"] is False
    assert status["formal_proofs_unchanged"] is True
    assert status["throughput_records_per_second"] == 123
    assert status["eta_seconds"] == 45


def test_launcher_keeps_agent_out_of_ergonomics_options(tmp_path, monkeypatch):
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "SOURCE", tmp_path / "source")
    (replay.SOURCE / "config").mkdir(parents=True)
    (replay.SOURCE / "config/jvm.options").write_text("")
    (replay.SOURCE / "config/log4j2.properties").write_text("")
    base = tmp_path / "arm"
    logs = base / "logs/session"
    replay.configure_arm(base, "default", logs)
    launcher = (base / "start-opensearch").read_text()
    assert f"path.logs: {logs}" in (base / "config/opensearch.yml").read_text()
    conf = (base / "config/opensearch.yml").read_text()
    for level,space in [("low","700gb"),("high","500gb"),("flood_stage","300gb")]:
        assert f"cluster.routing.allocation.disk.watermark.{level}: {space}" in conf
    assert (logs / "checksum-capture").is_dir()
    assert "JvmOptionsParser" in launcher
    assert "$OA_FINAL_JAVA_OPTS" in launcher
    assert "-javaagent:" not in launcher


def test_resume_rejects_failed_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(replay, "BASE", tmp_path / "reports")
    monkeypatch.setattr(replay, "validate_agent", lambda: {})
    monkeypatch.setattr(replay, "source_evidence", lambda: {})
    path = replay.progress_path("full_20260908")
    path.parent.mkdir(parents=True)
    state = replay.new_state("full_20260908")
    state["state"] = "failed"
    path.write_text(json.dumps(state))
    with pytest.raises(RuntimeError, match="immutable"):
        replay.Replay("full_20260908").prepare()


def test_new_attempt_preserves_failed_files_and_resume_selects_current_run(tmp_path, monkeypatch):
    monkeypatch.setattr(replay,"ROOT",tmp_path)
    monkeypatch.setattr(replay,"BASE",tmp_path/"reports/old")
    folder=tmp_path/"var/generations/full_20260908";folder.mkdir(parents=True)
    monkeypatch.setattr(replay,"generation_path",lambda _:folder)
    old=replay.progress_path("full_20260908");old.parent.mkdir(parents=True)
    old.write_text('{"state":"failed","common_checked":1856000}')
    pointer=folder/"postings-replay.json";pointer.write_text(json.dumps({"progress":str(old)}))
    assert replay.select_run("full_20260908")==old
    replay.select_run("full_20260908",new_attempt=True)
    fresh=replay.progress_path("full_20260908")
    assert fresh!=old and json.loads(old.read_text())["state"]=="failed"
    pointer.write_text(json.dumps({"progress":str(fresh)}))
    replay.select_run("full_20260908")
    assert replay.progress_path("full_20260908")==fresh


def test_saved_diagnostic_path_cannot_escape_project_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(replay,"ROOT",tmp_path)
    monkeypatch.setattr(replay,"generation_path",lambda _:tmp_path)
    (tmp_path/"postings-replay.json").write_text(json.dumps({"progress":"/tmp/other/progress.json"}))
    with pytest.raises(RuntimeError,match="invalid saved"):replay.select_run("full_20260908")
