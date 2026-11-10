"""Project-wide soft admission limits. No claim of cgroup isolation."""

import json
import os
import time
from pathlib import Path
from .config import ROOT, atomic_json, generation_path

GiB = 1024**3
PROFILE = dict(
    cpus=96,
    memory_gib=1024,
    disk_budget_gib=None,
    clickhouse_memory_gib=512,
    opensearch_heap_gib=31,
    opensearch_processors=32,
    workers=32,
    audit_workers=2,
    audit_max_workers=16,
    audit_batch_rows=5000,
    import_batch_rows=5000,
    import_batch_bytes=32 * 1024**2,
    edge_batch_rows=100000,
    edge_batch_bytes=32 * 1024**2,
    search_bulk_bytes=5 * 1024**2,
    search_workers=4,
    search_bulk_concurrency=2,
    ch_read_threads=4,
    ch_write_threads=2,
    compatibility_mode=False,
    execution_profile="throughput96",
)


class Paused(RuntimeError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def decide(free, rss, *, large=False, additional=0, temporary=0):
    if min(additional, temporary) < 0:
        raise ValueError("negative capacity estimate")
    if free < 300 * GiB:
        return "disk-emergency"
    if rss >= 960 * GiB:
        return "memory-emergency"
    if free < 500 * GiB:
        return "disk-pause"
    if rss >= 896 * GiB:
        return "memory-pause"
    if large and (free < 700 * GiB or free < additional + temporary + 500 * GiB):
        return "disk-admission"
    return None


def checkpoint(g, **requirements):
    from .manage import resource_status

    p = generation_path(g) / "pause-request.json"
    if p.exists():
        raise Paused(json.loads(p.read_text())["reason"])
    sample = resource_status()
    from .config import config
    cfg = config()
    if cfg.get("execution_profile") == "community":
        additional = requirements.get("additional", 0) + requirements.get("temporary", 0)
        if additional < 0:
            raise ValueError("negative capacity estimate")
        reason = ("disk-reserve" if sample["disk_free_bytes"] < additional + cfg["min_free_gib"] * GiB
                  else "memory-budget" if sample["total_rss_bytes"] >= cfg["memory_gib"] * GiB else None)
    else:
        reason = decide(sample["disk_free_bytes"], sample["total_rss_bytes"], **requirements)
    if sample.get("python_rss_bytes", 0) >= 128 * GiB:
        reason = reason or "memory-python-budget"
    if reason:
        request_pause(g, reason)
        raise Paused(reason)
    return sample


def request_pause(g, reason="user"):
    value = dict(reason=reason, requested=time.time(), state="pausing")
    prior = generation_path(g) / "auto-resume.json"
    if prior.exists():
        value["auto_attempted"] = True
    atomic_json(generation_path(g) / "pause-request.json", value)
    atomic_json(generation_path(g) / "control.json", value)
    return value


def cpu_set(allowed, count=96, root=Path("/sys/devices/system/node")):
    groups = []
    for path in sorted(root.glob("node*/cpulist")):
        group = set()
        for part in path.read_text().strip().split(","):
            ends = [int(x) for x in part.split("-")]
            group.update(range(ends[0], ends[-1] + 1))
        groups.append(sorted(group & set(allowed)))
    # Fill the largest available nodes first, retaining deterministic order.
    result = [
        cpu for group in sorted(groups, key=lambda g: (-len(g), g)) for cpu in group
    ]
    result += sorted(set(allowed) - set(result))
    if len(result) < count:
        raise RuntimeError("fewer than 96 authorized CPUs available")
    return result[:count]


def configure():
    from .config import config
    from .ingest import lock

    with lock(ROOT / "var/run/profile.lock"):
        c = config()
        p = ROOT / "var/run/throughput96-cpus.json"
        cpus = (
            json.loads(p.read_text())["cpus"]
            if p.exists()
            else cpu_set(os.sched_getaffinity(0))
        )
        if not set(cpus) <= os.sched_getaffinity(0):
            raise RuntimeError("persisted CPU pool unavailable to this process")
        atomic_json(
            p, dict(cpus=cpus, memory_enforcement="software", shared_volume=True)
        )
        atomic_json(ROOT / "config/local.json", c | PROFILE | {"cpu_affinity": cpus})
    os.sched_setaffinity(0, cpus)
    from . import manage

    for state in (ROOT / "var/run").glob("*.json"):
        proc = manage.identity(state)
        if proc:
            try:
                for child in [proc] + proc.children(recursive=True):
                    for thread in child.threads():
                        os.sched_setaffinity(thread.id, cpus)
            except (ProcessLookupError, __import__("psutil").NoSuchProcess):
                pass
    manage.engine_configs()
    if manage.health("clickhouse"):
        from .backend import Backend

        backend = Backend(writer=True)
        try:
            backend.ch("SYSTEM RELOAD CONFIG")
        finally:
            backend.close()
    return cpus


class Ramp:
    """A level needs 15 minutes of actual complete batches; no timer-only promotion."""

    def __init__(self, levels, now=None):
        self.levels = levels
        self.index = 0
        self.started = time.monotonic() if now is None else now
        self.records = 0
        self.previous_rate = None
        self.stopped = False

    @property
    def workers(self):
        return self.levels[self.index]

    def observe(self, records, sample, merge_backlog, correct=True, now=None):
        now = time.monotonic() if now is None else now
        self.records += records
        safe = (
            correct
            and not merge_backlog
            and sample["total_rss_bytes"] < 768 * GiB
            and sample["disk_free_bytes"] >= 1024 * GiB
        )
        if not safe:
            self.started, self.records = now, 0
            if self.index > 0:
                self.index -= 1
                self.stopped = True
            return self.workers
        if self.stopped or now - self.started < 900:
            return self.workers
        rate = self.records / (now - self.started)
        if self.previous_rate is not None and rate < self.previous_rate * 1.1:
            self.index = max(0, self.index - 1)
            self.stopped = True
        elif self.index + 1 < len(self.levels) and rate > 0:
            self.previous_rate = rate
            self.index += 1
        self.started, self.records = now, 0
        return self.workers


def disk_resume_ready(state, free, additional, temporary, now):
    """Only one automatic attempt, only for disk pressure, after 10 continuous minutes."""
    if not state.get("reason", "").startswith("disk-") or state.get("auto_attempted"):
        return False
    if free < max(1024 * GiB, additional + temporary + 500 * GiB):
        state.pop("healthy_since", None)
        return False
    state.setdefault("healthy_since", now)
    if now - state["healthy_since"] < 600:
        return False
    state["auto_attempted"] = True
    return True


def phase(g, name, additional=0, temporary=0):
    from .config import config
    from . import manage
    from .backend import Backend

    c = config()
    if c.get("execution_profile") != "throughput96":
        return
    if additional == 0 and temporary == 0:
        needs = estimate(g, name)
        additional = needs["additional"]
        temporary = needs["temporary"]
    checkpoint(g, large=True, additional=additional, temporary=temporary)
    limit = 256 if name == "search" else 512
    if c["clickhouse_memory_gib"] != limit:
        atomic_json(ROOT / "config/local.json", c | {"clickhouse_memory_gib": limit})
        manage.engine_configs()
        b = Backend(writer=True)
        try:
            b.ch("SYSTEM RELOAD CONFIG")
        finally:
            b.close()
    atomic_json(
        generation_path(g) / "resource-phase.json",
        dict(
            phase=name,
            clickhouse_gib=limit,
            python_queue_budget_gib=128,
            project_gib=1024,
            enforcement="software",
            additional_bytes=additional,
            temporary_bytes=temporary,
            updated=time.time(),
        ),
    )


def monitor_tick():
    """Only owned project workloads are paused/stopped; one disk-only recovery attempt."""
    from . import manage
    from .config import config

    if config().get("execution_profile") != "throughput96":
        return
    sample = manage.resource_status()
    reason = decide(sample["disk_free_bytes"], sample["total_rss_bytes"])
    if reason == "disk-emergency":
        # Native snapshots and engine merges continue after a Python controller exits.
        # Stop only identity-registered project services at the emergency floor.
        for name, row in sample.get("services", {}).items():
            if row.get("running") and name.startswith(
                ("recover-", "build-", "import-", "audit-", "verify-", "diagnose-storage-")
            ):
                generation = (
                    name[len("diagnose-storage-") :]
                    if name.startswith("diagnose-storage-")
                    else name.split("-", 1)[1]
                )
                if (ROOT / "var/generations" / generation / "generation.json").exists():
                    request_pause(generation, reason)
                manage.stop(name)
        for name in ("canary", "opensearch", "clickhouse"):
            if sample.get("services", {}).get(name, {}).get("running"):
                manage.stop(name)
    for p in (ROOT / "var/generations").glob("*/execution-run.json"):
        g = p.parent.name
        controller = manage.identity(manage.state_path("recover-" + g))
        request = p.parent / "pause-request.json"
        if reason and controller and not request.exists():
            request_pause(g, reason)
        if reason in ("disk-emergency", "memory-emergency") and controller:
            manage.stop("recover-" + g)
        if not request.exists() or controller:
            continue
        control = p.parent / "control.json"
        if (
            not control.exists()
            or json.loads(control.read_text()).get("state") != "paused"
        ):
            continue
        state = json.loads(request.read_text())
        profile = p.parent / "resource-phase.json"
        needs = json.loads(profile.read_text()) if profile.exists() else {}
        if disk_resume_ready(
            state,
            sample["disk_free_bytes"],
            needs.get("additional_bytes", 0),
            needs.get("temporary_bytes", 0),
            time.time(),
        ):
            atomic_json(request, state)
            # Preserve consumed permission across a second disk pause.
            atomic_json(p.parent / "auto-resume.json", state)
            request.unlink()
            until = json.loads(p.read_text()).get("until", "complete")
            manage.start("recover-" + g, ["--until", until])
        else:
            atomic_json(request, state)

    # A diagnostic pause has its own state machine and may make one disk-only
    # retry. User pauses and correctness failures never enter this branch.
    for summary in (ROOT / "var/generations").glob("*/postings-replay.json"):
        g = summary.parent.name
        diagnostic = json.loads(summary.read_text())
        request = summary.parent / "pause-request.json"
        controller = manage.identity(manage.state_path("diagnose-storage-" + g))
        if controller or diagnostic.get("state") != "paused" or not request.exists():
            continue
        state = json.loads(request.read_text())
        full = ROOT / "reports/postings-replay-20260916" / g / "progress.json"
        details = json.loads(full.read_text()) if full.exists() else {}
        additional = int(details.get("source_bytes", 0)) * 2
        if disk_resume_ready(
            state,
            sample["disk_free_bytes"],
            additional,
            0,
            time.time(),
        ):
            atomic_json(request, state)
            atomic_json(summary.parent / "auto-resume.json", state)
            request.unlink()
            manage.start("diagnose-storage-" + g, ["--profile", "postings-v1"])
        else:
            atomic_json(request, state)


def merge_backlog(b, g):
    from .backend import database, sql_string

    rows = b.ch(
        f"SELECT max(n) n FROM (SELECT count() n FROM system.parts WHERE active AND database={sql_string(database(g))} GROUP BY table,partition_id) FORMAT JSONEachRow"
    )
    # Conservative admission signal; no OPTIMIZE FINAL or mutation is issued.
    return int(rows[0]["n"]) >= 300 if rows else True


def estimate(g, name, b=None):
    """Conservative admission estimate from actual parts and confirmed source coverage."""
    from .backend import Backend, database, sql_string
    from .ingest import connect

    own = b is None
    if own:
        b = Backend(writer=True)
    try:
        parts = b.ch(
            f"SELECT sum(bytes_on_disk) bytes FROM system.parts WHERE active AND database={sql_string(database(g))} AND table IN ('entities','audit','identifiers','references_in','references_out') FORMAT JSONEachRow"
        )
        used = int(parts[0]["bytes"])
        with connect(g) as db:
            row = db.execute(
                "SELECT coalesce(sum(metadata_line),0) confirmed,coalesce(sum(expected),0) expected FROM shards WHERE selected=1"
            ).fetchone()
        if row["expected"] and not row["confirmed"]:
            raise RuntimeError(
                "capacity estimate needs a measured pilot or confirmed prefix"
            )
        full = used * row["expected"] / max(1, row["confirmed"])
        additional = (
            max(0, full - used) * 1.3
            if name == "metadata"
            else full * 1.5
            if name == "search"
            else used * 0.5
        )
        temporary = max(
            64 * GiB, full * 0.3 if name == "metadata" else additional * 0.5
        )
        result = dict(additional=int(additional), temporary=int(temporary))
        atomic_json(
            generation_path(g) / ("space-estimate-" + name + ".json"),
            dict(
                result,
                measured_bytes=used,
                confirmed=row["confirmed"],
                expected=row["expected"],
                time=time.time(),
                method="parts/confirmed coverage with explicit conservative multipliers",
                shared_volume=True,
            ),
        )
        return result
    finally:
        if own:
            b.close()
