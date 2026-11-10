import json
from pathlib import Path
from unittest.mock import Mock
import pytest
from openalex_api import storage_repair as repair, storage_finish as finish
from openalex_api.config import atomic_json


@pytest.fixture
def incident(tmp_path, monkeypatch):
    p = tmp_path / "generation"
    p.mkdir()
    r = tmp_path / "reports/storage-merge-20260913"
    r.mkdir(parents=True)
    monkeypatch.setattr(repair, "ROOT", tmp_path)
    monkeypatch.setattr(repair, "generation_path", lambda g: p)
    metadata = dict(signature="semantic", dependencies=dict(data_identity={"table": "uuid"}))
    monkeypatch.setattr(repair.recovery, "require_evidence", Mock(return_value=metadata))
    monkeypatch.setattr(repair.resources, "checkpoint", Mock())
    monkeypatch.setattr(repair, "mark", Mock())
    old = dict(phase="failed", error="RuntimeError('isolated replay failed: default')")
    atomic_json(p / "storage-repair.json", old)
    atomic_json(p / "metadata-complete.json", metadata)
    atomic_json(p / "storage-progress.json", dict(signature="semantic", dependencies=metadata["dependencies"], unique=1680000, last_id="W1516748039"))
    for label in repair.LABELS:
        atomic_json(r / ("validate-" + label + ".json"), dict(phase="failed", error="RuntimeError('merge timeout')", checked=1680000, expected=1680000, last="W1516748039", updated=123))
        atomic_json(r / ("force-" + label + ".json"), dict(task="same-node:" + label))
    return p, r, old


def test_timeout_migration_retains_full_ack_and_is_repeatable(incident):
    p, r, old = incident
    first = repair.migrate("full_20260908", old)
    second = repair.migrate("full_20260908", old)
    assert first["trials"] == second["trials"]
    assert first["trials"]["default"]["ack"]["checked"] == 1680000
    assert first["phase"] == "adopting"
    assert json.loads((r / "validate-default.json").read_text())["phase"] == "failed"


@pytest.mark.parametrize("error", ["CorruptIndexException", "RuntimeError('unknown failure')", "ArrayIndexOutOfBoundsException"])
def test_failed_replay_never_promotes_or_archives(incident, monkeypatch, error):
    p, r, old = incident
    f = r / "validate-default.json"
    atomic_json(f, json.loads(f.read_text()) | {"error": error})
    start = Mock()
    monkeypatch.setattr(repair.manage, "start", start)
    with pytest.raises(RuntimeError, match="isolated replay failed"):
        repair.run("full_20260908")
    start.assert_not_called()
    assert not (p / "storage-attempts").exists()


def test_different_metadata_identity_prevents_inheritance(incident):
    p, r, old = incident
    f = p / "storage-progress.json"
    value = json.loads(f.read_text())
    value["dependencies"] = {"data_identity": {"table": "replacement"}}
    atomic_json(f, value)
    with pytest.raises(RuntimeError, match="metadata identity"):
        repair.migrate("full_20260908", old)


def test_unknown_controller_state_fails_closed(incident):
    p, _, _ = incident
    atomic_json(p / "storage-repair.json", dict(version=2, phase="typo"))
    with pytest.raises(RuntimeError, match="unknown storage repair phase"):
        repair.run("full_20260908")


def test_archive_move_recovery_after_rename_before_checkpoint(tmp_path):
    src, dst = tmp_path / "source", tmp_path / "archive/target"
    src.write_text("precious evidence")
    state = {}
    calls = []
    def save():
        calls.append(json.loads(json.dumps(state)))
        if len(calls) == 2:
            raise OSError("simulated checkpoint failure")
    with pytest.raises(OSError):
        repair.move_once(src, dst, state, "data", save)
    resumed = calls[0]
    repair.move_once(src, dst, resumed, "data", lambda: None)
    assert dst.read_text() == "precious evidence"
    assert resumed["moves"]["data"]["complete"]
    assert not src.exists()


def test_archive_collision_never_overwrites(tmp_path):
    src, dst = tmp_path / "source", tmp_path / "target"
    src.write_text("original")
    dst.write_text("other")
    with pytest.raises(RuntimeError, match="collision"):
        repair.move_once(src, dst, {}, "data", lambda: None)
    assert src.read_text() == "original" and dst.read_text() == "other"


def test_archive_intent_enospc_does_not_move(tmp_path):
    src, dst = tmp_path / "source", tmp_path / "target"
    src.write_text("evidence")
    def full():
        raise OSError(28, "ENOSPC")
    with pytest.raises(OSError):
        repair.move_once(src, dst, {}, "data", full)
    assert src.exists() and not dst.exists()


def test_physical_manifest_detects_changed_commit(tmp_path):
    (tmp_path / "segments_1").write_text("commit")
    (tmp_path / "data").write_text("data")
    first = repair.commit_manifest(tmp_path)
    (tmp_path / "segments_1").write_text("other")
    assert repair.commit_manifest(tmp_path) != first


@pytest.mark.parametrize("docs", [[], [{"_id": "W2", "found": True, "_version": 7}], [{"_id": "W1", "found": False}], [{"_id": "W1", "found": True, "_version": 8}]])
def test_exact_ack_rejects_short_wrong_missing_or_versioned_response(docs):
    with pytest.raises(RuntimeError):
        finish.require_records([dict(id="W1", version=7)], docs)


def test_exact_ack_accepts_only_matching_sequence():
    finish.require_records([dict(id="W1", version=7)], [dict(_id="W1", found=True, _version=7)])


def test_generation_lock_is_reentrant_only_inside_owner(incident):
    @repair.exclusive
    def inner(g):
        return 42
    @repair.exclusive
    def outer(g):
        return inner(g)
    assert outer("full_20260908") == 42
    with repair.ingest.lock(repair.generation_path("full_20260908") / "execution-controller.lock"):
        with pytest.raises(RuntimeError, match="Another operation"):
            outer("full_20260908")


def test_stop_checks_creation_time_before_signalling(tmp_path, monkeypatch):
    atomic_json(tmp_path / "process.json", dict(pid=123, started=10))
    proc = Mock()
    proc.status.return_value = "running"
    proc.create_time.return_value = 20
    monkeypatch.setattr(repair.psutil, "Process", lambda pid: proc)
    with pytest.raises(RuntimeError, match="PID identity"):
        repair.replay_process(tmp_path, {"create_time": 10})


def test_live_original_canary_prevents_archive(incident, monkeypatch):
    p, _, _ = incident
    atomic_json(p / "storage-repair.json", dict(version=2, phase="archiving", state="running", physical_checks={str(i): {} for i in range(8)}, bounded_diagnostics_passed=True))
    monkeypatch.setattr(repair.manage, "identity", lambda *a, **k: object())
    with pytest.raises(RuntimeError, match="unexpectedly running"):
        repair.run("full_20260908")


def test_finish_rejects_short_soak_without_backend_calls(incident, monkeypatch):
    monkeypatch.setattr(finish, "generation_path", lambda g: incident[0])
    b = Mock()
    with pytest.raises(RuntimeError, match="production gate"):
        finish.finish("full_20260908", dict(unique=20000000, active_seconds=86399), b, "index")
    b.os.assert_not_called()


def test_until_reaches_repair_command(monkeypatch):
    monkeypatch.setattr(repair.manage, "config", lambda: {})
    cmd, _ = repair.manage.command("storage-repair-full_20260908", ["--until", "storage-verified"])
    assert cmd[-2:] == ["--until", "storage-verified"]


def test_bounded_run_resumes_physical_shards_and_archives_once(incident, monkeypatch):
    from openalex_api import proofs
    p, r, _ = incident
    for name, port in repair.LABELS.values():
        for i in range(4):
            shard = r / name / 'data/nodes/0/indices' / repair.UUID / str(i) / 'index'
            shard.mkdir(parents=True)
            (shard / 'segments_1').write_text('commit-'+str(i))
            (shard / '_0.cfs').write_text('data')
    original = repair.ROOT / 'var/storage-canary'
    original.mkdir(parents=True)
    (original / 'evidence').write_text('preserve')
    atomic_json(p / 'storage-verification.json', {'passed': False})
    monkeypatch.setattr(repair, 'observe', lambda label, row: dict(process={'pid': 1, 'create_time': 2}, merged_shards=['0','1']))
    monkeypatch.setattr(repair, 'stop_owned', lambda *a, **k: True)
    monkeypatch.setattr(repair, 'replay_process', lambda *a, **k: None)
    monkeypatch.setattr(repair.manage, 'identity', lambda *a, **k: None)
    monkeypatch.setattr(proofs, 'dependencies', lambda *a: {'engine': 'pinned'})
    checked = []
    def check(g, shard, logfile, flags):
        if len(checked) == 3 and not getattr(check, 'interrupted', False):
            check.interrupted = True
            raise repair.resources.Paused('test-interruption')
        checked.append(str(shard))
    monkeypatch.setattr(repair, 'check_one', check)
    metadata_before = (p / 'metadata-complete.json').read_bytes()
    with pytest.raises(repair.resources.Paused):
        repair.run('full_20260908', until='storage-verified')
    paused = json.loads((p / 'storage-repair.json').read_text())
    assert paused['phase'] == 'physical' and len(paused['physical_checks']) == 3
    result = repair.run('full_20260908', until='storage-verified')
    assert len(checked) == len(set(checked)) == 8
    assert result['phase'] == 'ready' and result['until'] == 'storage-verified'
    assert (p / 'storage-attempts/failed-20260913/canary/evidence').read_text() == 'preserve'
    assert not original.exists()
    assert (p / 'metadata-complete.json').read_bytes() == metadata_before
    assert repair.run('full_20260908')['phase'] == 'ready'
    assert len(checked) == 8


def test_formal_reconciliation_resume_does_not_recheck_confirmed_prefix(incident, monkeypatch):
    p, _, _ = incident
    monkeypatch.setattr(finish, 'generation_path', lambda g: p)
    monkeypatch.setattr(finish.manage, 'start', Mock())
    monkeypatch.setattr(finish.manage, 'stop', Mock())
    monkeypatch.setattr(finish.storage, 'assert_storage_logs_clean', Mock())
    checks = 0
    def checkpoint(g):
        nonlocal checks
        checks += 1
        if checks == 3:
            raise repair.resources.Paused('test-after-ack')
    monkeypatch.setattr(finish.resources, 'checkpoint', checkpoint)
    state = dict(validation_phase='reconciling', unique=20000000, active_seconds=86400,
                 initial_last='W9', recovery_after='W7', recovery_checked=19999998,
                 crash_recovered=True, index_uuid='same')
    b = Mock()
    b.os.return_value = [{'uuid': 'same'}]
    b.ch.return_value = [dict(id='W8', version=1), dict(id='W9', version=1)]
    b.mget.return_value = [dict(_id='W8', _version=1, found=True), dict(_id='W9', _version=1, found=True)]
    with pytest.raises(repair.resources.Paused):
        finish.finish('full_20260908', state, b, 'index')
    persisted = json.loads((p / 'storage-progress.json').read_text())
    assert persisted['recovery_checked'] == 20000000 and persisted['recovery_after'] == 'W9'
    assert b.mget.call_count == 1
    assert persisted['validation_phase'] == 'reconciling'


def test_user_pause_stops_adopted_replays_before_reporting_paused(incident, monkeypatch):
    p, _, old = incident
    state = repair.migrate('full_20260908', old)
    atomic_json(p / 'storage-repair.json', state)
    def checkpoint(g):
        raise repair.resources.Paused('user')
    monkeypatch.setattr(repair.resources, 'checkpoint', checkpoint)
    monkeypatch.setattr(repair, 'observe', lambda label,row: dict(process={'pid':1,'create_time':1}, merged_shards=['0','1']))
    stopped = Mock(return_value=True)
    monkeypatch.setattr(repair, 'stop_owned', stopped)
    with pytest.raises(repair.resources.Paused):
        repair.run('full_20260908')
    saved = json.loads((p / 'storage-repair.json').read_text())
    assert saved['state'] == 'paused' and saved['phase'] == 'physical'
    stopped.assert_called_once()


def test_disk_full_pauses_storage_without_failed_proof(incident, monkeypatch):
    from openalex_api import storage
    monkeypatch.setattr(storage, 'ROOT', repair.ROOT)
    monkeypatch.setattr(storage, 'generation_path', lambda g: incident[0])
    monkeypatch.setattr(storage.manage if hasattr(storage,'manage') else repair.manage, 'identity', lambda *a,**k:None)
    monkeypatch.setattr(repair.manage, 'stop', Mock())
    monkeypatch.setattr(storage.diagnostics, 'incident', Mock())
    failure = Mock(side_effect=OSError(28, 'ENOSPC'))
    monkeypatch.setattr(storage, '_verify', failure)
    proof = Mock()
    monkeypatch.setattr(storage.recovery, 'evidence', proof)
    with pytest.raises(repair.resources.Paused, match='disk-pause'):
        storage.verify('full_20260908')
    proof.assert_not_called()


def test_observer_failure_and_live_worker_are_reported_separately(incident, monkeypatch):
    monkeypatch.setattr(repair.manage, 'identity', lambda *a,**k:None)
    monkeypatch.setattr(repair, 'replay_process', lambda *a,**k:object())
    value=repair.runtime_status('full_20260908')
    assert value['orphan_work'] and not value['controller_running']
    assert all(v['running'] for v in value['replays'].values())


def test_legacy_archive_reuse_requires_exact_old_source_and_same_archive_functions(tmp_path, monkeypatch):
    from openalex_api import proofs
    import hashlib
    monkeypatch.setattr(proofs, 'ROOT', tmp_path)
    monkeypatch.setattr(proofs, 'CODE_ROOT', tmp_path)
    current = tmp_path / 'openalex_api/storage.py'
    current.parent.mkdir()
    baseline = 'def archive():\n return 1\ndef probe_source_open():\n return 2\n'
    old = tmp_path / 'var/generations/g/before-patch/openalex_api/storage.py'
    old.parent.mkdir(parents=True)
    old.write_text(baseline+'def verify():\n return 0\n')
    current.write_text(baseline+'def verify():\n return 3\n')
    before = dict(storage_code=hashlib.sha256(old.read_bytes()).hexdigest(), jdk='pinned')
    after = dict(storage_code='new', jdk='pinned', jvm_runtime_options=[])
    assert proofs.legacy_archive_compatible('g', 'archive', before, after)
    assert not proofs.legacy_archive_compatible('g', 'storage-verification', before, after)
    assert not proofs.legacy_archive_compatible('g', 'archive', before, after | {'jdk':'different'})
    assert not proofs.legacy_archive_compatible('g', 'archive', before, after | {'jvm_runtime_options':['c1']})
    current.write_text(baseline.replace('return 1','return 9'))
    assert not proofs.legacy_archive_compatible('g', 'archive', before, after)


def test_managed_cli_does_not_reject_its_own_supervisor(monkeypatch):
    def identity(path, child=False):
        if path.name == "recover-g.json":
            return Mock(pid=123 if child else 122)
        return None
    monkeypatch.setattr(repair.manage, "identity", identity)
    assert not repair.manage.controller_conflict("g", current_pid=123)
    assert repair.manage.controller_conflict("g", current_pid=999)


def test_another_controller_still_blocks_managed_child(monkeypatch):
    def identity(path, child=False):
        if path.name == "recover-g.json":
            return Mock(pid=123 if child else 122)
        if path.name == "build-g.json":
            return Mock(pid=456)
    monkeypatch.setattr(repair.manage, "identity", identity)
    assert repair.manage.controller_conflict("g", current_pid=123)


def test_managed_entry_before_child_record_is_fsynced(monkeypatch):
    monkeypatch.setattr(repair.manage.os, "getppid", lambda: 122)
    def identity(path, child=False):
        if path.name == "recover-g.json" and not child:
            return Mock(pid=122)
    monkeypatch.setattr(repair.manage, "identity", identity)
    assert not repair.manage.controller_conflict("g", current_pid=123)


def test_watch_reports_unavailable_diagnostics_and_pending_shutdown():
    from scripts.watch_full_build import classify
    row = dict(stage="storage-repair-adopting", controller_running=True, free_bytes=2*1024**4,
               resource_age_seconds=0, monitor_running=True, unchanged_seconds=0, completion_verified=False,
               storage_repair={"trials":{"default":{"observation_failures":3}}})
    assert "diagnostic_observation_unavailable" in classify(row)
    row["storage_repair"]["trials"]["default"] = {"stop_attention":"timeout"}
    assert "diagnostic_stop_pending" in classify(row)
    row["storage_repair"]["trials"]["default"]["stopped_at"] = 123
    assert "diagnostic_stop_pending" not in classify(row)


def test_mget_failure_keeps_bounded_cause_without_document_body(monkeypatch, caplog):
    from openalex_api.backend import Backend, BackendError
    b = Backend.__new__(Backend)
    b.os = Mock(return_value={"docs":[{"_id":"W1","error":{"type":"array_index_out_of_bounds_exception","reason":"Index -5","caused_by":{"type":"codec_error"}},"_source":{"title":"BODY_MUST_NOT_BE_LOGGED"}}]})
    with pytest.raises(BackendError,match="mget partial error for ID: W1") as caught:
        b.mget("g","works",["W1"])
    assert caught.value.backend_context["type"] == "array_index_out_of_bounds_exception"
    assert "codec_error" in caplog.text and "Index -5" in caplog.text
    assert "BODY_MUST_NOT_BE_LOGGED" not in caplog.text
