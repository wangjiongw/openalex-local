"""Read-only full-build watchdog; persistent local status, no automatic restart."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse, json, logging, logging.handlers, os, shutil, time, fcntl
import psutil
from openalex_api.config import ROOT, generation_path, atomic_json
from openalex_api import manage, ingest


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def classify(r):
    issues = []
    if r["stage"] == "failed":
        issues.append("build_failed")
    elif not r["controller_running"] and r["stage"] not in (
        "complete",
        "paused",
        "pausing",
    ):
        issues.append("controller_stopped")
    if r["free_bytes"] < 300 * 1024**3:
        issues.append("disk_below_300GiB")
    elif r["free_bytes"] < 700 * 1024**3:
        issues.append("disk_below_700GiB")
    if r["resource_age_seconds"] > 90:
        issues.append("resource_monitor_stale")
    if not r["monitor_running"]:
        issues.append("monitor_stopped")
    if r["stage"] == "auditing-existing-metadata" and r["unchanged_seconds"] > 1800:
        issues.append("audit_progress_unchanged_30min_check_queries")
    if r["stage"] == "complete" and not r["completion_verified"]:
        issues.append("completion_not_verified")
    trials = r.get("storage_repair", {}).get("trials", {}).values()
    if any(v.get("observation_failures", 0) >= 3 for v in trials):
        issues.append("diagnostic_observation_unavailable")
    if any(v.get("stop_attention") and not v.get("stopped_at") for v in r.get("storage_repair", {}).get("trials", {}).values()):
        issues.append("diagnostic_stop_pending")
    return issues


def sample(g, previous):
    now = time.time()
    p = generation_path(g)
    build = read(p / "build.json")
    cursor = read(p / "metadata-audit-cursor.json")
    ranges = read(p / "audit-progress.json")
    if ranges:
        cursor["checked"] = ranges.get("confirmed", cursor.get("checked", 0))
    control = read(p / "control.json")
    if control.get("state") in ("pausing", "paused"):
        build["stage"] = control["state"]
    with ingest.connect(g) as conn:
        row = conn.execute(
            "SELECT sum(metadata_line) confirmed,sum(expected) expected FROM shards WHERE selected=1"
        ).fetchone()
    exact = read(p / "exact-progress.json")
    repair = read(p / "storage-repair.json")
    storage_progress = read(p / "storage-progress.json")
    token = [build.get("stage"), cursor.get("checked"), row["confirmed"],
             [exact.get(k) for k in ("left", "right", "entity", "total", "done", "phase")],
             [repair.get("phase"), len(repair.get("physical_checks", {})),
              {k: [v.get("ack", {}).get("checked", v.get("checked", 0)), v.get("merged_shards", []), v.get("stopped_at")] for k,v in repair.get("trials",{}).items()}],
             [storage_progress.get(k) for k in ("attempt", "unique", "updates", "validation_phase", "recovery_checked")],
             len(storage_progress.get("physical_checks", {}))]
    changed = (
        previous.get("progress_changed_at", now)
        if previous.get("progress_token") == token
        else now
    )
    resource = read(ROOT / "var/run/resource-latest.json")
    stage = build.get("stage", "unknown")
    verified = False
    if stage == "complete" and build.get("complete"):
        from openalex_api.resume import verify_published_api

        try:
            verify_published_api(g)
            verified = True
        except Exception:
            pass
    r = dict(
        time=now,
        generation=g,
        stage=stage,
        error=build.get("error"),
        diagnostic_path=build.get("diagnostic_path"),
        controller_running=any(manage.identity(manage.state_path(prefix + g)) for prefix in ("recover-", "build-", "storage-repair-")),
        monitor_running=bool(manage.identity(manage.state_path("monitor"))),
        audit_checked=cursor.get("checked", 0),
        confirmed_records=row["confirmed"],
        expected_records=row["expected"],
        free_bytes=shutil.disk_usage(ROOT).free,
        resource_age_seconds=now - resource.get("time", 0),
        aggregate_rss_bytes=resource.get("aggregate_rss"),
        progress_token=token,
        storage_repair=repair,
        storage_progress={k: storage_progress.get(k) for k in ("attempt", "unique", "updates", "active_seconds", "validation_phase", "recovery_checked", "updated")},
        progress_changed_at=changed,
        unchanged_seconds=now - changed,
        completion_verified=verified,
    )
    if repair:
        from openalex_api.storage_repair import runtime_status
        r["storage_repair_runtime"] = runtime_status(g)
    r["alerts"] = classify(r)
    if r.get("storage_repair_runtime", {}).get("orphan_work"):
        r["alerts"].append("diagnostic_work_without_controller")
    r["status"] = (
        "complete"
        if verified and not r["alerts"]
        else ("attention" if r["alerts"] else "running")
    )
    return r


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("generation")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    g = args.generation
    generation_path(g)  # validated name
    latest = ROOT / "var/run" / ("watch-" + g + "-status.json")
    with (ROOT / "var/run" / ("watch-" + g + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger = logging.getLogger("watch")
        logger.setLevel(logging.INFO)
        handler = logging.handlers.RotatingFileHandler(
            ROOT / "var/log" / ("watch-" + g + ".jsonl"),
            maxBytes=8 * 1024**2,
            backupCount=4,
        )
        logger.addHandler(handler)
        proc = psutil.Process()
        atomic_json(
            ROOT / "var/run" / ("watch-" + g + "-process.json"),
            dict(pid=os.getpid(), create_time=proc.create_time()),
        )
        previous = read(latest)
        while True:
            try:
                result = sample(g, previous)
            except Exception as exc:
                result = dict(
                    time=time.time(),
                    generation=g,
                    status="attention",
                    alerts=["watch_sample_failed"],
                    error=repr(exc),
                )
            try:
                atomic_json(latest, result)
                logger.info(json.dumps(result))
            except OSError:
                atomic_json(Path("/tmp") / ("openalex-watch-" + g + ".json"), result)
            previous = result
            if args.once or result.get("status") == "complete":
                break
            time.sleep(60)


if __name__ == "__main__":
    main()
