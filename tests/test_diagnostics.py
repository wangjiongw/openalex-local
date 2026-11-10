from openalex_api.config import CODE_ROOT
import json
import os
from pathlib import Path
import subprocess
import zlib
from unittest.mock import Mock

import pytest
from openalex_api import diagnostics, manage, storage, config as settings


def test_incident_keeps_original_error_if_sampling_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(diagnostics, "ROOT", tmp_path)
    monkeypatch.setattr(
        diagnostics, "snapshot", Mock(side_effect=OSError("missing proc"))
    )
    result = diagnostics.incident("test")
    assert result is not None
    assert json.loads(Path(result).read_text())["durable"] is False
    records = list((tmp_path / "var/diagnostics").glob("*/incident.json"))
    assert len(records) == 1 and json.loads(records[0].read_text())["kind"] == "test"


def test_failure_bundle_unique_and_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr(diagnostics, "ROOT", tmp_path)
    monkeypatch.setattr(diagnostics, "snapshot", lambda: {"processes": [], "time": 1})
    logs = tmp_path / "var/log"
    logs.mkdir(parents=True)
    (logs / "test.log").write_bytes(b"x" * (400 * 1024))
    a = Path(diagnostics.incident("test"))
    b = Path(diagnostics.incident("test"))
    assert a != b
    assert (a / "test.log").stat().st_size == 256 * 1024
    assert json.loads((a / "resources.json").read_text())["processes"] == []


def test_stage_failure_retains_phase_and_exception(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "generation_path", lambda g: tmp_path)
    monkeypatch.setattr(diagnostics, "incident", lambda *a, **k: "/evidence")
    (tmp_path / "build.json").write_text(
        json.dumps({"stage": "checking-existing-indexes"})
    )
    error = RuntimeError("checksum failed")

    @diagnostics.guarded_stage
    def operation(g):
        raise error

    with pytest.raises(RuntimeError) as caught:
        operation("test")
    assert caught.value is error
    state = json.loads((tmp_path / "build.json").read_text())
    assert (
        state["stage"] == "failed"
        and state["failed_stage"] == "checking-existing-indexes"
    )
    assert state["diagnostic_path"] == "/evidence"


def test_memory_budget_selects_one_stable_workload():
    services = {
        name: dict(running=True, rss_bytes=size)
        for name, size in [
            ("clickhouse", 100),
            ("opensearch", 50),
            ("recover-test", 10),
            ("import-test", 20),
            ("monitor", 1),
        ]
    }
    result = manage.memory_victim({"services": services})
    assert result == "import-test"
    services["recover-test"]["rss_bytes"] = 200
    assert manage.memory_victim({"services": services}) == result
    services["import-test"]["running"] = False
    assert manage.memory_victim({"services": services}) == "recover-test"


def test_capture_required_not_only_clean_exit(tmp_path):
    log = tmp_path / "check.log"
    log.write_text("INTEGRITY_OK")
    with pytest.raises(RuntimeError):
        storage.require_capture(log)
    log.write_text(
        "CAPTURE_AGENT_TRANSFORMED\nINTEGRITY_OK\nCAPTURE_SUMMARY installed=true calls=0 failures=0 reserved=0"
    )
    with pytest.raises(RuntimeError):
        storage.require_capture(log)
    log.write_text(log.read_text() + "\nnumSegments=0")
    storage.require_capture(log)


@pytest.mark.skipif(os.environ.get("OA_LIVE_TESTS") != "1", reason="bundled JVM")
def test_actual_checksum_failure_bytes_and_original_exception(tmp_path):
    java, exports, agent, classes, cp = storage.prepare_index_checker()
    subprocess.run(
        [
            str(java / "javac"),
            "-cp",
            str(classes) + ":" + cp,
            "-d",
            str(tmp_path),
            str(CODE_ROOT / "tests/fixtures/CaptureSelfTest.java"),
        ],
        check=True,
        timeout=60,
    )
    evidence = tmp_path / "evidence"
    result = subprocess.run(
        [
            str(java / "java"),
            *exports,
            "-javaagent:" + str(agent),
            "-Doa.capture.dir=" + str(evidence),
            "-cp",
            str(tmp_path) + ":" + str(classes) + ":" + cp,
            "CaptureSelfTest",
            str(tmp_path / "fixture"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SELFTEST_OK" in result.stdout
    assert "calls=2 failures=1 reserved=0" in result.stderr
    report = next(evidence.glob("*.json"))
    data = report.with_suffix(".bin").read_bytes()
    r = json.loads(report.read_text())
    assert r["complete"] and data == (tmp_path / "fixture/test.bin").read_bytes()
    assert (
        zlib.crc32(data[:-8])
        == int(r["software_crc32"], 16)
        == int(r["jvm_crc32_recomputed"], 16)
    )
    assert r["stored_crc32"] != r["software_crc32"]


def test_storage_log_check_streams_rotated_errors(tmp_path, monkeypatch):
    import gzip

    (tmp_path / "engine.log").write_text("normal\n")
    with gzip.open(tmp_path / "engine-1.log.gz", "wt") as out:
        out.write("normal\nCHECKSUM FAILED in previous run\n")
    monkeypatch.setattr(
        Path, "read_text", Mock(side_effect=AssertionError("must stream"))
    )
    with pytest.raises(RuntimeError, match="engine-1.log.gz:2"):
        storage.assert_storage_logs_clean(tmp_path)


def test_canary_bulk_surfaces_engine_failure_before_retry(tmp_path):
    from openalex_api.backend import Backend
    b = object.__new__(Backend)
    b.os = Mock(return_value={"errors": True, "items": []})
    original = b.os
    (tmp_path / "engine.log").write_text("normal\nArrayIndexOutOfBoundsException: bad merge\n")
    storage.guard_canary_bulk(b, tmp_path)
    with pytest.raises(RuntimeError, match="engine.log:2"):
        b.os("POST", "/_bulk", content=b"x")
    assert original.call_count == 1


def test_canary_bulk_allows_transient_failure_without_fatal_logs(tmp_path):
    from openalex_api.backend import Backend
    b = object.__new__(Backend)
    expected = {"errors": True, "items": []}
    b.os = Mock(return_value=expected)
    (tmp_path / "engine.log").write_text("rejected execution: queue full\n")
    storage.guard_canary_bulk(b, tmp_path)
    assert b.os("POST", "/_bulk", content=b"x") == expected


def test_storage_runtime_modes_are_explicit_and_bound(tmp_path, monkeypatch):
    from openalex_api import proofs, manage
    monkeypatch.setattr(storage, "ROOT", tmp_path)
    assert storage.runtime_options() == []
    monkeypatch.setattr(proofs, "semantic", lambda g: "unchanged")
    before = proofs.dependencies("test", "offline-index-check")
    (tmp_path / "var").mkdir()
    runtime = tmp_path / "var/storage-runtime.json"
    runtime.write_text('{"mode":"c1"}')
    assert storage.runtime_options() == ["-XX:TieredStopAtLevel=1"]
    after = proofs.dependencies("test", "offline-index-check")
    assert before["jvm_runtime_options"] == []
    assert after["jvm_runtime_options"] == storage.runtime_options()
    assert before["semantic"] == after["semantic"]
    _, env = manage.command("opensearch", [])
    assert "-XX:TieredStopAtLevel=1" in env["OPENSEARCH_JAVA_OPTS"]
    monkeypatch.setattr(storage, "canary_config", lambda: tmp_path)
    _, env = manage.command("canary", [])
    assert "-XX:TieredStopAtLevel=1" in env["OPENSEARCH_JAVA_OPTS"]
    runtime.write_text('{"mode":"unexpected"}')
    with pytest.raises(RuntimeError, match="Unknown storage"):
        storage.runtime_options()
