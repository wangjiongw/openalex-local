import argparse, json, logging, sys, time
from pathlib import Path
from .config import ROOT, init, config, ENTITIES, atomic_json


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str), flush=True)


def main():
    p = argparse.ArgumentParser(
        prog="oa", description="Local OpenAlex import and service management"
    )
    s = p.add_subparsers(dest="command", required=True)
    s.add_parser("init")
    s.add_parser("inventory")
    s.add_parser("doctor")
    q = s.add_parser("diagnostics")
    q.add_argument("--stacks", action="store_true")
    for name in ["start", "stop", "restart"]:
        q = s.add_parser(name)
        q.add_argument(
            "service",
            choices=["all", "clickhouse", "opensearch", "api", "monitor", "canary"],
        )
    q = s.add_parser("status")
    q.add_argument("--generation")
    q = s.add_parser("logs")
    q.add_argument("service")
    q.add_argument("--lines", type=int, default=80)
    q.add_argument("--follow", action="store_true")
    q = s.add_parser("create")
    q.add_argument("generation")
    q.add_argument("--mode", choices=["pilot", "full"], default="pilot")
    q.add_argument("--works", type=int, default=1000000)
    q.add_argument("--shards-json", default="{}")
    for name in ["import", "validate", "capacity"]:
        q = s.add_parser(name)
        q.add_argument("generation")
        if name == "import":
            q.add_argument("--background", action="store_true")
    for name in ["recover", "audit", "verify-storage", "repair-index", "resume"]:
        q = s.add_parser(name)
        q.add_argument("generation")
        if name in ["recover", "audit", "verify-storage"]:
            q.add_argument("--background", action="store_true")
        if name == "recover":
            from .resume import STAGES
            q.add_argument("--until", choices=STAGES, default="complete")
        if name == "repair-index":
            q.add_argument("--apply", action="store_true")
            q.add_argument("--rebuild-entity", choices=ENTITIES)
        if name == "resume":
            q.add_argument("--stage", choices=["metadata", "search"], required=True)
    q = s.add_parser("build")
    q.add_argument("generation")
    q = s.add_parser("build-stop")
    q.add_argument("generation")
    q = s.add_parser("import-stop")
    q.add_argument("generation")
    q = s.add_parser("publish")
    q.add_argument("generation")
    q.add_argument("--allow-partial", action="store_true")
    q = s.add_parser("rollback")
    q.add_argument("generation")
    q.add_argument("--allow-partial", action="store_true")
    q = s.add_parser("cleanup")
    q.add_argument("generation")
    q.add_argument("--confirm", required=True)
    q = s.add_parser("_supervise")
    q.add_argument("service")
    q.add_argument("extra", nargs=argparse.REMAINDER)
    a = p.parse_args()
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        from . import ingest, manage

        if a.command == "init":
            emit(
                {
                    "config": str(init()),
                    "message": "API key is stored in this mode-0600 file; send it as X-API-Key",
                }
            )
            return
        config()
        if a.command == "inventory":
            inv = ingest.inventory(config()["snapshot"])
            atomic_json(ROOT / "reports/inventory.json", inv)
            emit({k: v for k, v in inv.items() if k != "files"})
        elif a.command == "diagnostics":
            from .diagnostics import incident
            emit({"diagnostic_path": incident("manual", stacks=a.stacks)})
        elif a.command == "doctor":
            import fcntl, platform, os

            with ingest.lock(ROOT / "var/run/doctor.lock"):
                with ingest.local_lock_path(ROOT / "var/run/doctor.lock").open(
                    "a"
                ) as f:
                    try:
                        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        locked = False
                    except BlockingIOError:
                        locked = True
            emit(
                dict(
                    architecture=platform.machine(),
                    python=sys.version,
                    cpus=len(os.sched_getaffinity(0)),
                    vm_max_map_count=Path("/proc/sys/vm/max_map_count")
                    .read_text()
                    .strip(),
                    file_lock_exclusion=locked,
                    resources=manage.resource_status(),
                    backends={v: manage.health(v) for v in manage.SERVICES},
                )
            )
        elif a.command in ["start", "stop", "restart"]:
            names = list(manage.SERVICES) if a.service == "all" else [a.service]
            if a.service == "all" and a.command in ["stop", "restart"]:
                names += [
                    name
                    for name, state in manage.resource_status()["services"].items()
                    if name.startswith(
                        (
                            "import-",
                            "build-",
                            "recover-",
                            "verify-",
                            "audit-",
                            "validation-api-",
                        )
                    )
                    and state["running"]
                ]
            if (
                a.service == "all"
                and a.command in ["stop", "restart"]
                and manage.health("canary")
            ):
                names.append("canary")
            if a.command in ["stop", "restart"]:
                for name in reversed(names):
                    emit(manage.stop(name))
            if a.command in ["start", "restart"]:
                for name in names:
                    emit(manage.start(name))
        elif a.command == "status":
            if a.generation:
                emit(ingest.status(a.generation))
            else:
                pointer = ROOT / "var/active.json"
                active_data = (
                    json.loads(pointer.read_text()) if pointer.exists() else None
                )
                if active_data:
                    meta = ingest.read_generation(active_data["generation"])
                    active_data.update(
                        complete=meta.get("complete", False), mode=meta["mode"]
                    )
                emit(
                    manage.resource_status()
                    | {
                        "health": {v: manage.health(v) for v in manage.SERVICES},
                        "active_generation": active_data,
                    }
                )
        elif a.command == "logs":
            from collections import deque

            manage.state_path(a.service)
            with (ROOT / "var/log" / f"{a.service}.log").open() as f:
                print("".join(deque(f, maxlen=max(1, min(a.lines, 10000)))), end="")
                while a.follow:
                    line = f.readline()
                    if line:
                        print(line, end="", flush=True)
                    else:
                        time.sleep(0.5)
                        import os

                        path = ROOT / "var/log" / f"{a.service}.log"
                        if (
                            path.exists()
                            and path.stat().st_ino != os.fstat(f.fileno()).st_ino
                        ):
                            f.close()
                            f = path.open()
        elif a.command == "create":
            shards = json.loads(a.shards_json)
            if any(
                k not in ENTITIES or type(v) is not int or v < 1 or v > 1024
                for k, v in shards.items()
            ):
                raise ValueError("invalid shard map")
            if a.mode == "full":
                capacities = list((ROOT / "var/generations").glob("*/capacity.json"))
                if not capacities:
                    raise RuntimeError(
                        "run a pilot and capacity before creating full generation"
                    )
                cap = json.loads(
                    max(capacities, key=lambda x: x.stat().st_mtime).read_text()
                )
                if (
                    not cap.get("validated")
                    or cap["entities"]["works"]["imported_records"] < 1000000
                    or any(v["imported_records"] == 0 for v in cap["entities"].values())
                ):
                    raise RuntimeError(
                        "capacity gate requires a validated pilot with >=1 million works and all 21 entity types"
                    )
                if (
                    cap.get("manifest_sha256")
                    != ingest.inventory(config()["snapshot"])["manifest_sha256"]
                ):
                    raise RuntimeError(
                        "capacity report belongs to a different snapshot"
                    )
                if not cap.get("storage_settled", False):
                    raise RuntimeError(
                        "wait for old index segments to release and rerun capacity before full build"
                    )
                if (
                    cap["estimated_initial_peak_bytes"]
                    > config()["disk_budget_gib"] * 1024**3
                ):
                    raise RuntimeError(
                        "pilot projects above configured space budget; review capacity before expansion"
                    )
                for entity, metrics in cap["entities"].items():
                    minimum = metrics["recommended_shards"]
                    if entity in shards and shards[entity] < minimum:
                        raise ValueError(
                            f"{entity} requires at least {minimum} shards per measured size/doc limits"
                        )
                    shards.setdefault(entity, minimum)
            emit(ingest.create_generation(a.generation, a.mode, a.works, shards))
        elif a.command == "import":
            emit(
                manage.start("import-" + a.generation)
                if a.background
                else ingest.run_import(a.generation)
            )
        elif a.command in (
            "recover",
            "audit",
            "verify-storage",
            "repair-index",
            "resume",
        ):
            from . import recovery, search_build, storage, workflow

            if a.command == "recover":
                emit(
                    manage.start("recover-" + a.generation, ["--until", a.until])
                    if a.background
                    else workflow.recover(a.generation, until=a.until)
                )
            elif a.command == "audit":
                emit(
                    manage.start("audit-" + a.generation)
                    if a.background
                    else recovery.audit(a.generation)
                )
            elif a.command == "verify-storage":
                emit(
                    manage.start("verify-" + a.generation)
                    if a.background
                    else storage.verify(a.generation)
                )
            elif a.command == "repair-index":
                if a.rebuild_entity:
                    if not a.apply:
                        raise ValueError("--rebuild-entity requires --apply")
                    emit(search_build.replace_entity(a.generation, a.rebuild_entity))
                emit(search_build.reconcile(a.generation, a.apply))
            else:
                emit(recovery.release(a.generation, a.stage))
        elif a.command == "build":
            emit(manage.start("build-" + a.generation))
        elif a.command == "build-stop":
            for prefix in (
                "recover-",
                "verify-",
                "audit-",
                "build-",
                "import-",
                "validation-api-",
            ):
                emit(manage.stop(prefix + a.generation))
            owner = ROOT / "var/storage-canary/owner.json"
            if (
                owner.exists()
                and json.loads(owner.read_text())["generation"] == a.generation
            ):
                emit(manage.stop("canary"))
        elif a.command == "import-stop":
            emit(manage.stop("import-" + a.generation))
        elif a.command == "validate":
            emit(ingest.validate(a.generation))
        elif a.command == "capacity":
            emit(ingest.capacity(a.generation))
        elif a.command in ["publish", "rollback"]:
            emit(ingest.publish(a.generation, a.allow_partial))
        elif a.command == "cleanup":
            emit(ingest.cleanup(a.generation, a.confirm))
        elif a.command == "_supervise":
            manage.supervise(a.service, a.extra)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        if a.command == "_supervise":
            raise
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
