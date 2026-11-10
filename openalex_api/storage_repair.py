"""Resumable, bounded incident diagnostics; never certify the failed storage attempt."""
import contextlib
import contextvars
import functools
import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path

import httpx
import psutil
from . import ingest, recovery, resources, storage, manage
from .config import ROOT, atomic_json, generation_path
from .build import mark

_OWNER = contextvars.ContextVar("storage_controller", default=None)
LABELS = {"default": ("replay", 19203), "c1": ("replay-c1", 19204)}
UUID = "fSPfBzCHQlefzgTgPUZMfA"
INDEX = "oa-storage-full-20260908-works"
PHASES = {"adopting", "stopping", "physical", "archiving", "ready"}
TIMEOUTS = {"RuntimeError('merge timeout')", "RuntimeError('diagnostic observation timeout')"}


def exclusive(fn):
    @functools.wraps(fn)
    def wrapped(g, *args, **kwargs):
        if _OWNER.get() == g:
            return fn(g, *args, **kwargs)
        if _OWNER.get() is not None:
            raise RuntimeError("controller generation changed")
        with ingest.lock(generation_path(g) / "execution-controller.lock"):
            token = _OWNER.set(g)
            try:
                return fn(g, *args, **kwargs)
            finally:
                _OWNER.reset(token)
    return wrapped


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def incident_root():
    return ROOT / "reports/storage-merge-20260913"


def needed(g):
    return (generation_path(g) / "storage-repair.json").exists()


def validate_ack(row, prior):
    if row.get("error") and row["error"] not in TIMEOUTS:
        raise RuntimeError("isolated replay failed: " + str(row.get("error")))
    if row.get("phase") not in ("failed", "passed", "waiting-for-merge"):
        raise RuntimeError("unconfirmed legacy replay phase")
    if not (row.get("checked") == row.get("expected") == prior.get("unique") == 1680000
            and row.get("last") == prior.get("last_id") == "W1516748039"):
        raise RuntimeError("legacy acknowledged prefix identity mismatch")
    return {k: row[k] for k in ("checked", "expected", "last", "updated")}


def replay_process(base, expected=None):
    record = read(base / "process.json")
    if not record:
        raise RuntimeError("missing replay process identity")
    try:
        proc = psutil.Process(record["pid"])
        if proc.status() == psutil.STATUS_ZOMBIE:
            return None
    except psutil.NoSuchProcess:
        return None
    birth = (expected or {}).get("create_time", record.get("create_time", record["started"]))
    tolerance = 0.01 if expected or "create_time" in record else 10
    if abs(proc.create_time() - birth) > tolerance:
        raise RuntimeError("replay PID identity changed")
    if proc.environ().get("OPENSEARCH_PATH_CONF") != str(base / "config"):
        raise RuntimeError("replay config identity changed")
    if os.getpgid(proc.pid) != proc.pid:
        raise RuntimeError("replay process group identity changed")
    return proc


def observe(label, row):
    name, port = LABELS[label]
    base = incident_root() / name
    proc = replay_process(base, row.get("process"))
    if not proc:
        raise RuntimeError("replay disappeared before controlled stop")
    with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10, trust_env=False) as c:
        def get(path):
            response = c.get(path)
            response.raise_for_status()
            return response.json()
        settings = get("/" + INDEX + "/_settings")[INDEX]["settings"]["index"]
        if settings["uuid"] != UUID:
            raise RuntimeError("replay index identity changed")
        task = read(incident_root() / ("force-" + label + ".json"))
        if task.get("port") != port:
            raise RuntimeError("replay task endpoint mismatch")
        done = get("/_tasks/" + task["task"])
        if done.get("error") or done.get("response", {}).get("_shards", {}).get("failed"):
            raise RuntimeError("server merge task failed")
        stats = get("/" + INDEX + "/_stats/merge,segments?level=shards")["indices"][INDEX]["shards"]
        health = get("/_cluster/health/" + INDEX)
        if health["status"] != "green":
            raise RuntimeError("replay cluster is not green")
    shards = {}
    for shard, copies in stats.items():
        primary = next(x for x in copies if x["routing"]["primary"])
        shards[shard] = dict(segments=primary["segments"]["count"], merges=primary["merges"])
    if set(shards) != {"0", "1", "2", "3"}:
        raise RuntimeError("expected exactly four replay shards")
    complete = [s for s, v in shards.items() if v["segments"] == 1 and not v["merges"]["current"] and v["merges"]["total"] > 0]
    storage.assert_storage_logs_clean(base / "logs")
    return dict(process=dict(pid=proc.pid, create_time=proc.create_time()), endpoint=port,
                index_uuid=UUID, task=task["task"], task_completed=bool(done.get("completed")),
                shards=shards, merged_shards=complete, observed_at=time.time(),
                io=proc.io_counters()._asdict(), cpu=proc.cpu_times()._asdict())


def migrate(g, old):
    if g != "full_20260908":
        raise ValueError("legacy incident belongs to full_20260908")
    p = generation_path(g)
    prior = read(p / "storage-progress.json")
    metadata = recovery.require_evidence(g, "metadata-complete")
    if prior.get("signature") != metadata["signature"] or prior.get("dependencies", {}).get("data_identity") != metadata["dependencies"]["data_identity"]:
        raise RuntimeError("frozen metadata identity changed")
    backup = p / "storage-repair-migration-v2"
    backup.mkdir(exist_ok=True)
    sources = [p / "storage-repair.json", p / "storage-progress.json", p / "metadata-complete.json"]
    for label in LABELS:
        sources += [incident_root() / ("validate-" + label + ".json"), incident_root() / ("force-" + label + ".json")]
    for source in sources:
        target = backup / source.name
        data = source.read_bytes()
        if target.exists():
            if json.loads(target.read_bytes()) != json.loads(data):
                raise RuntimeError("legacy migration backup changed")
        else:
            atomic_json(target, json.loads(data))
    trials = {label: dict(ack=validate_ack(read(incident_root() / ("validate-" + label + ".json")), prior)) for label in LABELS}
    if old.get("phase") == "failed" and old.get("error") not in TIMEOUTS | {"RuntimeError('isolated replay failed: default')", "RuntimeError('isolated replay failed: c1')"}:
        raise RuntimeError("unknown legacy controller failure")
    if old.get("phase") not in ("failed", "waiting-diagnostics"):
        raise RuntimeError("unsupported legacy controller phase")
    return dict(version=2, phase="adopting", state="running", trials=trials,
                metadata_dependencies=metadata["dependencies"], started=time.time(),
                inherited_from=str(backup), physical_checks={}, moves={})


def commit_manifest(shard):
    files = sorted(f for f in shard.iterdir() if f.is_file() and f.name != "write.lock")
    commits = [f for f in files if f.name.startswith("segments_")]
    if len(commits) != 1:
        raise RuntimeError("expected one stable committed segments file")
    return dict(commit=commits[0].name, commit_sha256=hashlib.sha256(commits[0].read_bytes()).hexdigest(),
                files={f.name: [f.stat().st_size, f.stat().st_mtime_ns] for f in files})


def stop_owned(g, state, save, force=False):
    pending = False
    for label, (name, _) in LABELS.items():
        row = state["trials"][label]
        base = incident_root() / name
        proc = replay_process(base, row.get("process"))
        if proc is None:
            if not row.get("stop_requested"):
                raise RuntimeError("replay exited without a recorded stop intent")
            row.setdefault("stopped_at", time.time())
            continue
        if not row.get("stop_requested"):
            storage.assert_storage_logs_clean(base / "logs")
            row["stop_requested"] = time.time()
            save()
        # Repeat SIGTERM after an interruption between intent and signal is safe.
        if force or not row.get("stop_signal_sent"):
            os.killpg(proc.pid, signal.SIGKILL if force else signal.SIGTERM)
            row["stop_signal_sent"] = time.time()
        if time.time() - row["stop_requested"] > 120:
            row["stop_attention"] = "graceful-stop-exceeds-120s"
        pending = True
        save()
        break
    save()
    return not pending


def check_one(g, shard, logfile, flags, copy_to=None):
    java, exports, agent, classes, cp = storage.prepare_index_checker()
    before = commit_manifest(shard)
    with logfile.open("w") as out:
        proc = subprocess.Popen([str(java / "java"), "-Xmx8g", "-XX:ActiveProcessorCount=4", *flags,
             *exports, "-javaagent:" + str(agent), "-Doa.capture.dir=" + str(logfile.parent / (logfile.stem + "-capture")),
             "-cp", str(classes) + ":" + cp, "ReadOnlyIndexCheck", str(shard),
             *([str(copy_to)] if copy_to is not None else [])], stdout=out, stderr=subprocess.STDOUT)
        started = time.monotonic()
        try:
            while proc.poll() is None:
                resources.checkpoint(g)
                if time.monotonic() - started > 1800:
                    raise resources.Paused("physical-check-timeout")
                time.sleep(1)
            if proc.returncode:
                raise RuntimeError("physical integrity check failed: " + str(logfile))
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
    storage.require_capture(logfile)
    if commit_manifest(shard) != before:
        raise RuntimeError("shard changed during physical check")
    return before


def move_once(source, target, state, key, save):
    moves = state.setdefault("moves", {})
    item = moves.get(key)
    if item is None:
        if not source.exists():
            raise RuntimeError("missing archive source: " + str(source))
        stat = source.stat()
        item = moves[key] = dict(source=str(source), target=str(target), device=stat.st_dev, inode=stat.st_ino,
                                sha256=hashlib.sha256(source.read_bytes()).hexdigest() if source.is_file() else None)
        save()
    def matches(path):
        s = path.stat()
        return (s.st_dev, s.st_ino) == (item["device"], item["inode"]) and (item["sha256"] is None or hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"])
    if target.exists():
        if source.exists() or not matches(target):
            raise RuntimeError("archive identity collision")
    else:
        if not source.exists() or not matches(source):
            raise RuntimeError("archive source identity changed")
        target.parent.mkdir(parents=True, exist_ok=True)
        source.rename(target)
        for parent in {source.parent, target.parent}:
            fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    item["complete"] = True
    save()


@exclusive
def run(g, until="complete"):
    p = generation_path(g)
    journal = p / "storage-repair.json"
    state = read(journal)
    def save():
        state.update(updated=time.time(), until=until)
        atomic_json(journal, state)
    recovery.require_evidence(g, "metadata-complete")
    if state.get("version") != 2:
        state = migrate(g, state)
        save()
    if state.get("phase") not in PHASES:
        raise RuntimeError("unknown storage repair phase")
    if state.get("state") == "failed":
        raise RuntimeError("storage repair requires investigation: " + state.get("error", "unknown"))
    current_metadata = recovery.require_evidence(g, "metadata-complete")
    if state.get("metadata_dependencies") not in (None, current_metadata["dependencies"]):
        raise RuntimeError("repair metadata identity changed")
    if state["phase"] == "ready":
        return state
    try:
        resources.checkpoint(g)
        state["state"] = "running"
        save()
        mark(g, "storage-repair-" + state["phase"])
        if state["phase"] == "adopting":
            for label, row in state["trials"].items():
                while True:
                    resources.checkpoint(g)
                    try:
                        row.update(observe(label, row))
                        row.pop("observation_error", None)
                        row["observation_failures"] = 0
                        save()
                        break
                    except (httpx.HTTPError, OSError) as error:
                        row["observation_error"] = type(error).__name__
                        row["observation_failures"] = row.get("observation_failures", 0) + 1
                        save()
                        time.sleep(20)
                if len(row["merged_shards"]) < 2:
                    raise resources.Paused("insufficient-completed-merge-evidence")
            state["phase"] = "stopping"
            save()
        if state["phase"] == "stopping":
            while not stop_owned(g, state, save):
                mark(g, "pausing", reason="controlled-diagnostic-stop", resume_phase="physical")
                state["state"] = "pausing"
                save()
                time.sleep(20)
            resources.checkpoint(g)
            state.update(phase="physical", state="running")
            save()
        if state["phase"] == "physical":
            from .proofs import dependencies
            identity = dependencies(g, "storage-verification")
            for label, (name, _) in LABELS.items():
                base = incident_root() / name
                if replay_process(base, state["trials"][label]["process"]):
                    raise RuntimeError("cannot check a live replay")
                storage.assert_storage_logs_clean(base / "logs")
                paths = sorted((base / "data/nodes/0/indices" / UUID).glob("*/index"))
                if {x.parent.name for x in paths} != {"0", "1", "2", "3"}:
                    raise RuntimeError("expected four physical shards")
                for shard in paths:
                    key = label + "-" + shard.parent.name
                    flags = ["-XX:TieredStopAtLevel=1"] if label == "c1" else []
                    binding = dict(dependencies=identity, runtime=flags, manifest=commit_manifest(shard))
                    if state["physical_checks"].get(key, {}).get("binding") == binding:
                        continue
                    resources.checkpoint(g)
                    mark(g, "storage-repair-physical", shard=key, completed=len(state["physical_checks"]))
                    logfile = incident_root() / ("bounded-check-" + key + ".log")
                    check_one(g, shard, logfile, flags)
                    state["physical_checks"][key] = dict(binding=binding, passed=True, time=time.time(), log=str(logfile))
                    save()
            state.update(phase="archiving", bounded_diagnostics_passed=True)
            save()
        if state["phase"] == "archiving":
            if len(state["physical_checks"]) != 8 or not state.get("bounded_diagnostics_passed"):
                raise RuntimeError("missing complete bounded diagnostics")
            if manage.identity(manage.state_path("canary")) or manage.identity(manage.state_path("canary"), child=True):
                raise RuntimeError("original canary unexpectedly running")
            resources.checkpoint(g)
            archive = p / "storage-attempts/failed-20260913"
            for key, source in [("canary", ROOT / "var/storage-canary"), ("storage-progress.json", p / "storage-progress.json"), ("storage-verification.json", p / "storage-verification.json")]:
                move_once(source, archive / key, state, key, save)
            runtime = read(ROOT / "var/storage-runtime.json")
            if runtime and runtime.get("mode") != "default":
                raise RuntimeError("runtime changed; refuse automatic candidate selection")
            atomic_json(ROOT / "var/storage-runtime.json", dict(mode="default", generation=g, candidate=True,
                        reason="bounded diagnostics passed; original merge root cause remains unresolved"))
            state.update(phase="ready", state="complete", archive=str(archive), ready_at=time.time())
            save()
        return state
    except resources.Paused as error:
        # Capture the identity and already-completed work before stopping an adoption.
        if state["phase"] == "adopting":
            for label, row in state["trials"].items():
                if not row.get("process"):
                    row.update(observe(label, row))
            if all(len(row.get("merged_shards", [])) >= 2 for row in state["trials"].values()):
                state["phase"] = "stopping"
            else:
                state.update(state="failed", error="insufficient diagnostics at pause; manual recovery required")
            save()
        if any(row.get("process") for row in state.get("trials", {}).values()):
            while not stop_owned(g, state, save):
                state["state"] = "pausing"
                save()
                time.sleep(20)
        if state["phase"] == "stopping":
            state["phase"] = "physical"
        if state.get("state") != "failed":
            state.update(state="paused", reason=str(error))
        save()
        mark(g, "paused", reason=str(error))
        raise
    except OSError as error:
        if error.errno not in (28, 122):
            state.update(state="failed", error=repr(error))
            try:
                save()
            except OSError:
                pass
            raise
        # Stop writers even when the shared disk cannot accept a pause journal.
        from .diagnostics import incident
        state.update(state="pausing", reason="disk-pause")
        def best_effort_save():
            try:
                save()
            except OSError:
                pass
        while not stop_owned(g, state, best_effort_save):
            time.sleep(20)
        if state["phase"] == "stopping":
            state["phase"] = "physical"
        state.update(state="paused", reason="disk-pause")
        best_effort_save()
        incident("storage-repair-space", {"generation": g, "phase": state["phase"], "error": repr(error)})
        raise resources.Paused("disk-pause") from error
    except BaseException as error:
        state.update(state="failed", error=repr(error))
        try:
            save()
        except OSError:
            from .diagnostics import incident
            incident("storage-repair-journal", {"generation": g, "phase": state.get("phase"), "error": repr(error)})
        raise


def force_stop(g):
    state = read(generation_path(g) / "storage-repair.json")
    if not state:
        return
    if g != "full_20260908":
        raise RuntimeError("unknown diagnostic ownership")
    for label, (name, _) in LABELS.items():
        base = incident_root() / name
        proc = replay_process(base, state.get("trials", {}).get(label, {}).get("process"))
        if proc:
            state.update(state="failed", error="forced diagnostic stop requires recovery verification", updated=time.time())
            atomic_json(generation_path(g) / "storage-repair.json", state)
            os.killpg(proc.pid, signal.SIGKILL)


def runtime_status(g):
    """Read-only ownership-aware status, including work whose observer exited."""
    state = read(generation_path(g) / "storage-repair.json")
    if not state:
        return {}
    result = dict(controller_running=any(manage.identity(manage.state_path(prefix + g))
                  for prefix in ("recover-", "build-", "storage-repair-")),
                  phase=state.get("phase"), state=state.get("state", state.get("phase")),
                  physical_completed=len(state.get("physical_checks", {})), physical_total=8,
                  eta_seconds=None, replays={})
    for label, (name, _) in LABELS.items():
        row = state.get("trials", {}).get(label, {})
        try:
            proc = replay_process(incident_root() / name, row.get("process"))
            active = bool(proc)
            error = None
        except (RuntimeError, OSError, psutil.Error) as ex:
            active, error = None, str(ex)
        result["replays"][label] = dict(running=active, identity_error=error,
            checked=row.get("ack", {}).get("checked", row.get("checked", 0)),
            merged_shards=row.get("merged_shards"), observed_at=row.get("observed_at"),
            stop_attention=row.get("stop_attention"), observation_error=row.get("observation_error"))
    result["orphan_work"] = not result["controller_running"] and any(x["running"] for x in result["replays"].values())
    return result
