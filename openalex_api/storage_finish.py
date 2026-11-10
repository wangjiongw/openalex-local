"""Persistent crash/reconciliation/physical phases for a completed storage workload."""
import json
import os
import signal
import time

from . import manage, recovery, resources, storage
from .config import ROOT, atomic_json, generation_path
from .backend import database, sql_string


def require_records(rows, docs):
    def fail(message, **context):
        error = RuntimeError(message)
        error.backend_context = context
        raise error
    if len(rows) != len(docs):
        fail("acknowledged-write response length mismatch", category="response_count",
             requested_count=len(rows), returned_count=len(docs))
    for row, doc in zip(rows, docs):
        category = ("response_id" if doc.get("_id") != row["id"] else
                    "missing_record" if not doc.get("found") else
                    "version_mismatch" if doc.get("_version") != int(row["version"]) else None)
        if category:
            fail("acknowledged ID/version mismatch: " + row["id"], category=category,
                 requested_id=row["id"], response_id=doc.get("_id"),
                 expected_version=int(row["version"]), actual_version=doc.get("_version"))


def finish(g, state, b, name):
    from .storage_repair import check_one, commit_manifest
    p = generation_path(g) / "storage-progress.json"
    def save(phase=None):
        if phase:
            state["validation_phase"] = phase
        state["updated"] = time.time()
        atomic_json(p, state)
    phase = state.get("validation_phase", "writing")
    if phase not in {"writing", "crash-prepared", "crash-issued", "reconciling", "physical", "passed"}:
        raise RuntimeError("unknown storage validation phase")
    if state["unique"] < 20000000 or state.get("active_seconds", 0) < 86400:
        raise RuntimeError("storage workload is below the production gate")
    resources.checkpoint(g)
    if phase in {"writing", "crash-prepared"}:
        # An intent without a durable issued receipt is repeated with a fresh target.
        manage.start("canary")
        b.os("POST", "/" + name + "/_flush?wait_if_ongoing=true")
        proc = manage.identity(manage.state_path("canary"), child=True)
        if not proc:
            raise RuntimeError("Canary process disappeared")
        info = b.os("GET", "/_cat/indices/" + name + "?format=json&h=uuid")[0]
        state.update(index_uuid=info["uuid"], crash_attempt=state.get("crash_attempt", 0) + 1,
                     crash_target=dict(pid=proc.pid, create_time=proc.create_time()), crash_prepared_at=time.time())
        save("crash-prepared")
        os.killpg(proc.pid, signal.SIGKILL)
        state["crash_issued_at"] = time.time()
        save("crash-issued")
        phase = "crash-issued"
    if phase == "crash-issued":
        if not state.get("crash_issued_at") or not state.get("crash_target"):
            raise RuntimeError("missing crash execution evidence")
        manage.stop("canary")
        manage.start("canary")
        deadline = time.monotonic() + 600
        while b.os("GET", "/_cluster/health/" + name)["status"] != "green":
            resources.checkpoint(g)
            if time.monotonic() > deadline:
                raise resources.Paused("crash-recovery-observation-timeout")
            time.sleep(5)
        info = b.os("GET", "/_cat/indices/" + name + "?format=json&h=uuid")[0]
        if info["uuid"] != state["index_uuid"]:
            raise RuntimeError("canary index identity changed after crash")
        state.update(crash_recovered=True, recovery_checked=0, recovery_after="")
        save("reconciling")
        phase = "reconciling"
    if phase == "reconciling":
        if not state.get("crash_recovered"):
            raise RuntimeError("missing crash recovery evidence")
        manage.start("canary")
        info = b.os("GET", "/_cat/indices/" + name + "?format=json&h=uuid")[0]
        if info["uuid"] != state["index_uuid"]:
            raise RuntimeError("canary index identity changed")
        upper = state["initial_last"]
        while True:
            resources.checkpoint(g)
            rows = b.ch(f"SELECT id,version FROM {database(g)}.entities FINAL WHERE entity='works' AND id>{sql_string(state['recovery_after'])} AND id<={sql_string(upper)} AND cityHash64(id)%4=0 ORDER BY id LIMIT 4000 FORMAT JSONEachRow")
            if not rows:
                break
            docs = b.mget("storage_" + g, "works", [r["id"] for r in rows])
            require_records(rows, docs)
            storage.assert_storage_logs_clean(ROOT / "var/storage-canary/logs")
            state["recovery_checked"] += len(rows)
            if state["recovery_checked"] > state["unique"]:
                raise RuntimeError("canary recovery range exceeds acknowledged prefix")
            state["recovery_after"] = rows[-1]["id"]
            save()
        if state["recovery_checked"] != state["unique"] or state["recovery_after"] != upper:
            raise RuntimeError("canary recovery prefix incomplete")
        b.os("POST", "/" + name + "/_flush?wait_if_ongoing=true")
        manage.stop("canary")
        save("physical")
        phase = "physical"
    if phase in {"physical", "passed"}:
        if manage.identity(manage.state_path("canary")) or manage.identity(manage.state_path("canary"), child=True):
            raise RuntimeError("canary must be stopped for physical checks")
        paths = sorted((ROOT / "var/storage-canary/data/nodes/0/indices" / state["index_uuid"]).glob("*/index"))
        if {d.parent.name for d in paths} != {"0", "1", "2", "3"}:
            raise RuntimeError("expected all four physical canary shards")
        epoch = {d.parent.name: commit_manifest(d) for d in paths}
        if state.get("physical_epoch") is not None and state["physical_epoch"] != epoch:
            raise RuntimeError("immutable canary commit changed after recovery verification")
        if "physical_epoch" not in state:
            state["physical_epoch"] = epoch
            save("physical")
        checks = state.setdefault("physical_checks", {})
        for shard in paths:
            resources.checkpoint(g)
            key = shard.parent.name
            manifest = commit_manifest(shard)
            binding = dict(manifest=manifest, dependencies=state["dependencies"])
            if checks.get(key, {}).get("binding") == binding:
                continue
            logfile = ROOT / "reports" / ("canary-check-" + key + ".log")
            check_one(g, shard, logfile, storage.runtime_options())
            checks[key] = dict(binding=binding, passed=True, checked_at=time.time())
            save("physical")
        storage.assert_storage_logs_clean(ROOT / "var/storage-canary/logs")
        save("passed")
        return recovery.evidence(g, "storage-verification", passed=True, mode="niofs", unique=state["unique"],
            seconds=state["active_seconds"], wall_seconds=time.time()-state["started"],
            crash_recovered=True, physical_check=True, attempt=state.get("attempt"), recovery_checked=state["recovery_checked"])
