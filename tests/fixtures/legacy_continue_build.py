"""One-shot handoff: successful diagnostic, fresh 24h qualification, existing full workflow."""
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time

import psutil
from openalex_api import continuous_storage as continuous, manage, recovery, resources
from openalex_api.config import ROOT, atomic_json, config, generation_path
from openalex_api.storage_repair import exclusive

G = "full_20260908"
REPORT = Path(__file__).parent
BASE = ROOT / "reports/storage-qualification-20261102"


def diagnostic_ready(document):
    if document.get("state") in ("failed", "paused"):
        raise RuntimeError("diagnostic did not complete successfully; retained evidence requires investigation")
    if document.get("state") != "complete":
        return False
    trials = document.get("trials", {})
    if (document.get("classification", {}).get("kind") != "not-reproduced"
            or not document.get("formal_proofs_unchanged") or document.get("production_gate_passed")
            or set(trials) != {"1", "2", "3"}
            or any(row.get("state") != "complete" or len(row.get("physical", {})) != 8 for row in trials.values())):
        raise RuntimeError("diagnostic completion lacks paired physical checks")
    return True


def pause(*args):
    continuous.stop_requested = True


def save(state, **extra):
    atomic_json(REPORT / "continuation.json", dict(state=state, updated=time.time(), **extra))


def diagnostic_progress():
    pointer_path = generation_path(G) / "postings-replay.json"
    path = pointer_path
    for attempt in range(5):
        try:
            pointer = json.loads(pointer_path.read_text())
            path = Path(pointer["progress"]).resolve()
            if not path.is_relative_to(ROOT / "reports"):
                raise RuntimeError("invalid diagnostic path")
            document = json.loads(path.read_text())
            if document.get("generation") != G or document.get("profile") != "postings-v1":
                raise RuntimeError("diagnostic state belongs to a different run")
            return pointer, path, document
        except FileNotFoundError as error:
            if attempt == 4:
                raise RuntimeError(f"diagnostic state unavailable: {path}") from error
            if continuous.stop_requested:
                raise resources.Paused("user")
            time.sleep(1)


@exclusive
def qualify(g):
    atomic_json(manage.state_path("verify-" + g), dict(
        pid=os.getpid(), create_time=psutil.Process().create_time(), external_diagnostic=True))
    recovery.require_evidence(g, "recovery-tests")
    BASE.mkdir(exist_ok=True)
    original = ROOT / "reports/continuous-storage-20260915"
    old = json.loads((original / "progress.json").read_text())
    source = original / "merge-capture.jar"
    if continuous.digest(source) != old["capture_agent_sha256"]:
        raise RuntimeError("preserved qualification agent identity changed")
    target = BASE / "merge-capture.jar"
    if not target.exists(): shutil.copy2(source, target)
    if continuous.digest(target) != continuous.digest(source):
        raise RuntimeError("qualification capture copy differs")
    save("qualifying-storage", progress=str(BASE / "progress.json"), minimum_seconds=86400)
    continuous.Qualification(g, BASE).run()


def main():
    signal.signal(signal.SIGTERM, pause)
    signal.signal(signal.SIGINT, pause)
    os.sched_setaffinity(0, config()["cpu_affinity"])
    with __import__("openalex_api.ingest", fromlist=["lock"]).lock(REPORT / "continuation.lock"):
        atomic_json(manage.state_path("watch-release-continuation-" + G), dict(
            pid=os.getpid(), create_time=psutil.Process().create_time(), external_diagnostic=True))
        try:
            while True:
                if continuous.stop_requested: raise resources.Paused("user")
                resources.checkpoint(G)
                pointer, path, document = diagnostic_progress()
                ready = diagnostic_ready(document)
                running = manage.identity(manage.state_path("diagnose-storage-" + G))
                if ready and not running:
                    break
                if not ready and not running:
                    raise RuntimeError("diagnostic controller stopped before completion")
                save("waiting-for-diagnostic", progress=str(path), phase=pointer.get("phase"))
                time.sleep(30)
            qualify(G)
            save("complete")
        except BaseException as error:
            save("paused" if isinstance(error, resources.Paused) else "failed", error=repr(error))
            raise


if __name__ == "__main__":
    if "--self-check" in sys.argv:
        assert not diagnostic_ready({"state":"running"})
        good = dict(state="complete", classification={"kind":"not-reproduced"},
                    formal_proofs_unchanged=True, production_gate_passed=False,
                    trials={str(n):dict(state="complete",physical={str(i):{} for i in range(8)}) for n in range(1,4)})
        assert diagnostic_ready(good)
        for bad in [dict(good,state="failed"), dict(good,trials={}), dict(good,production_gate_passed=True)]:
            try: diagnostic_ready(bad)
            except RuntimeError: pass
            else: raise AssertionError("invalid diagnostic must not start qualification")
        print("handoff self-check passed")
    else:
        main()
