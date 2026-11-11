import argparse, json, logging, sys, time, os
from pathlib import Path
# Select the workspace before importing modules that bind their state paths.
_bootstrap = argparse.ArgumentParser(add_help=False)
_bootstrap.add_argument("--workspace")
_workspace, _ = _bootstrap.parse_known_args()
if _workspace.workspace:
    os.environ["OPENALEX_HOME"] = str(Path(_workspace.workspace).expanduser().resolve())
from .config import ROOT, init, config, ENTITIES, atomic_json


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str), flush=True)


def main():
    p = argparse.ArgumentParser(
        prog="oa", description="Local OpenAlex import and service management"
    )
    p.add_argument("--workspace", help="local data/config workspace; overrides OPENALEX_HOME")
    s = p.add_subparsers(dest="command", required=True)
    q = s.add_parser('arxiv', help='independent optional arXiv source workflow')
    q.add_argument('action', choices=['plan','run','resume','status','stop','validate','publish'])
    q.add_argument('--release', required=True)
    q.add_argument('--input')
    q.add_argument('--input-format', choices=['jsonl','duckdb'], default='jsonl')
    q.add_argument('--min-year', type=int, default=2010)
    q.add_argument('--generation')
    q.add_argument('--until', choices=['ingest','link','store','index','validate'], default='validate')
    q = s.add_parser('export')
    q.add_argument('action', choices=['create', 'list', 'status', 'cancel', 'resume'])
    q.add_argument('--request', help='JSON export request file')
    q.add_argument('--job')
    q = s.add_parser('catalogs')
    q.add_argument('action', choices=['build', 'status', 'validate', 'publish', 'stop'])
    q.add_argument('--generation', required=True)
    q.add_argument('--build', required=True)
    q.add_argument('--parent')
    q.add_argument('--background', action='store_true')
    q = s.add_parser("init")
    q.add_argument("--snapshot")
    q = s.add_parser("snapshot")
    q.add_argument("action", choices=["verify"])
    q.add_argument("--target", help="existing local snapshot path; defaults to configured snapshot")
    q.add_argument("--deep", action="store_true")
    q = s.add_parser("engines")
    q.add_argument("action", choices=["ensure"])
    q.add_argument("--offline", action="store_true")
    q.add_argument("--cache")
    q.add_argument("--search", action="store_true", help="also install Manticore for optional text indexing")
    q.add_argument("--profile", choices=["standard", "armv8-compat"], default="standard")
    q = s.add_parser("adopt")
    q.add_argument("--source-workspace", required=True)
    q.add_argument("--generation", required=True)
    q.add_argument("--build", required=True)
    q.add_argument("--port", type=int, default=8088)
    q = s.add_parser("pipeline")
    q.add_argument("action", choices=["plan", "run", "resume", "status"])
    q.add_argument("--generation", required=True)
    q.add_argument("--profile", choices=["metadata", "earth_sciences"], default="metadata")
    q.add_argument("--until", choices=["metadata", "collections", "index", "validated", "published"], default=None)
    for name in ["verify", "restore"]:
        q = s.add_parser(name)
        q.add_argument("--generation", required=True)
        q.add_argument("--build")
        q.add_argument("--output", required=True)
        q.add_argument("--base-url")
        q.add_argument("--remote-client", action="store_true")
    s.add_parser("inventory")
    s.add_parser("doctor")
    q = s.add_parser("manticore")
    q.add_argument("args", nargs=argparse.REMAINDER)
    for name in ("search-qualify", "search-build", "search-validate", "search-publish", "search-disable", "search-status"):
        q = s.add_parser(name)
        q.add_argument("generation")
        if name in ("search-build", "search-validate", "search-publish"):
            q.add_argument("build_id")
        if name in ("search-qualify", "search-build"):
            q.add_argument("--background", action="store_true")
        if name == "search-qualify":
            q.add_argument("--attempt", required=True)
            q.add_argument("--continue-build")
        if name == "search-build":
            q.add_argument("--scope")
            q.add_argument("--publish", action="store_true")
    for name in ("search-catalog", "search-pipeline"):
        q = s.add_parser(name)
        q.add_argument("generation")
        q.add_argument("--background", action="store_true")
    for name in ("earth-export", "earth-export-status", "earth-export-stop", "earth-export-verify"):
        q = s.add_parser(name)
        q.add_argument("generation")
        q.add_argument("--collection", default="earth_scope_v1")
        if name == "earth-export":
            q.add_argument("--background", action="store_true")
            q.add_argument("--follow", action="store_true")
            q.add_argument("--max-blocks", type=int)
    for name in ("earth-prepare", "earth-audit"):
        q = s.add_parser(name)
        q.add_argument("generation")
        q.add_argument("--collection", default="earth_scope_v1")
        q.add_argument("--output", required=True)
    q = s.add_parser("earth-check")
    q.add_argument("--cases", required=True)
    q.add_argument("--output", required=True)
    q.add_argument("--base-url")
    q.add_argument("--phase", choices=("auto", "metadata", "search", "all"), default="auto")
    q.add_argument("--load-seconds", type=int, default=0)
    q = s.add_parser("earth-audit-finalize")
    q.add_argument("--output", required=True)
    q.add_argument("--reviews", required=True)
    q.add_argument("--journal-reviews", required=True)
    q = s.add_parser("diagnostics")
    q.add_argument("--stacks", action="store_true")
    for name in ["start", "stop", "restart"]:
        q = s.add_parser(name)
        q.add_argument(
            "service",
            choices=["all", "clickhouse", "opensearch", "manticore", "api", "monitor", "canary", "export-worker"],
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
    for name in ["recover", "audit", "verify-storage", "diagnose-storage", "repair-index", "resume"]:
        q = s.add_parser(name)
        q.add_argument("generation")
        if name in ["recover", "audit", "verify-storage", "diagnose-storage"]:
            q.add_argument("--background", action="store_true")
        if name == "recover":
            from .resume import STAGES

            q.add_argument("--until", choices=STAGES, default=None)
            q.add_argument("--profile", choices=["throughput96"])
            q.add_argument("--compatibility", action="store_true")
        if name == "repair-index":
            q.add_argument("--apply", action="store_true")
            q.add_argument("--rebuild-entity", choices=ENTITIES)
        if name == "diagnose-storage":
            q.add_argument("--profile", choices=["postings-v1"], default="postings-v1")
            q.add_argument("--new-attempt", action="store_true")
        if name == "resume":
            q.add_argument("--stage", choices=["metadata", "search"], required=True)
    q = s.add_parser("build")
    q.add_argument("generation")
    q = s.add_parser("build-stop")
    q.add_argument("generation")
    q.add_argument("--force", action="store_true")
    q = s.add_parser("import-stop")
    q.add_argument("generation")
    q = s.add_parser("publish")
    q.add_argument("generation")
    q.add_argument("--allow-partial", action="store_true")
    q.add_argument("--metadata-only", action="store_true")
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
            path = init()
            if a.snapshot:
                value = config()
                value["snapshot"] = str(Path(a.snapshot).expanduser().resolve())
                atomic_json(path, value)
            emit(
                {
                    "config": str(init()),
                    "message": "API key is stored in this mode-0600 file; send it as X-API-Key",
                }
            )
            return
        if a.command in ("snapshot", "engines", "pipeline", "verify", "restore", "adopt"):
            from .community_cli import execute
            emit(execute(a))
            return
        if a.command == 'arxiv':
            from . import arxiv_pipeline as arxiv
            if a.action == 'plan':
                if not a.input:
                    raise ValueError('--input is required')
                emit(arxiv.plan(a.release,a.input,a.input_format,a.min_year,a.generation))
            elif a.action == 'status':
                emit(arxiv.read(a.release))
            elif a.action == 'stop':
                atomic_json(arxiv.path(a.release)/'stop.json', dict(stop=True))
                emit(dict(state='stop_requested'))
            elif a.action == 'publish':
                emit(arxiv.publish(a.release))
            else:
                if a.action == 'resume':
                    (arxiv.path(a.release)/'stop.json').unlink(missing_ok=True)
                if a.action == 'validate':
                    # Recheck stored/indexed artifacts even after an earlier validation.
                    from .backend import Backend
                    from .manticore_engine import Client
                    from .ingest import lock
                    with lock(arxiv.path(a.release)/'worker.lock'):
                        m=arxiv.read(a.release)
                        b,c=Backend(),Client(timeout=600)
                        try:
                            arxiv.validate(m,b,c)
                            arxiv.save(m)
                        finally:
                            b.close(); c.close()
                        emit(m)
                else:
                    emit(arxiv.run(a.release,a.until))
            return
        if a.command == 'export':
            from . import exports, api
            if a.action == 'create':
                if not a.request:
                    raise ValueError('--request is required')
                emit(exports.create(json.loads(Path(a.request).read_text()), api.generation()))
            elif a.action == 'list':
                emit(exports.listing())
            else:
                if not a.job:
                    raise ValueError('--job is required')
                emit(exports.get(a.job) if a.action == 'status' else exports.control(a.job, a.action))
            return
        if a.command == 'catalogs':
            from . import catalogs
            if a.action != 'status' and config().get('adopted_from'):
                raise ValueError('adopted workspaces cannot build, qualify, stop or publish source catalogs')
            if a.action == 'stop':
                emit(manage.stop('build-catalogs-' + a.generation))
            elif a.action == 'build' and a.background:
                emit(manage.start('build-catalogs-' + a.generation, ['--build', a.build] + (['--parent', a.parent] if a.parent else [])))
            else:
                if a.action == 'build':
                    emit(catalogs.build(a.generation, a.build, a.parent))
                else:
                    emit(getattr(catalogs, {'status':'status','validate':'qualify','publish':'publish'}[a.action])(a.generation,a.build))
            return
        cfg = config()
        if cfg.get("adopted_from"):
            if a.command not in ("start", "stop", "restart", "status", "logs", "_supervise"):
                raise ValueError("adopted workspace is read-only; use a separate workspace for data processing")
            if a.command in ("start", "stop", "restart", "_supervise") and a.service not in ("api", "export-worker"):
                raise ValueError("adopted workspace only manages its own API and export worker")
        if a.command in ("search-build", "search-validate", "search-publish") and config().get("search_engine") == "manticore":
            if getattr(a, "background", False) or getattr(a, "scope", None):
                raise ValueError("use the community pipeline entrypoint; legacy background/scope flags are not supported")
            from .community_pipeline import search_action
            emit(search_action(a.command, a.generation, a.build_id))
            return
        if a.command == "manticore":
            from .manticore_build import main as manticore_main
            sys.argv = ["oa manticore", *a.args]
            manticore_main()
        elif a.command.startswith("earth-export"):
            from . import earth_collections, earth_control
            from .config import generation_name
            generation_name(a.generation); generation_name(a.collection)
            service = "export-earth-" + a.generation
            if a.command == "earth-export-status":
                emit(earth_control.status(a.generation, a.collection))
            elif a.command == "earth-export-stop":
                emit(manage.stop(service))
            elif a.command == "earth-export-verify":
                emit(earth_collections.verify(a.generation, a.collection))
            elif a.background:
                extra = ["--collection", a.collection] + (["--follow"] if a.follow else [])
                if a.max_blocks is not None:
                    extra += ["--max-blocks", str(a.max_blocks)]
                emit(manage.start(service, extra))
            else:
                import signal
                from .resources import Paused
                def stop_export(signum, frame):
                    raise Paused("signal-stop")
                signal.signal(signal.SIGTERM, stop_export)
                signal.signal(signal.SIGINT, stop_export)
                if a.max_blocks is not None and a.max_blocks < 1:
                    raise ValueError("max-blocks must be positive")
                emit(earth_control.run(a.generation, a.collection, a.follow, a.max_blocks))
        elif a.command in ("earth-prepare", "earth-check", "earth-audit", "earth-audit-finalize"):
            from . import earth_delivery, earth_audit
            if a.command == "earth-prepare":
                emit(earth_delivery.prepare(a.generation, a.collection, a.output))
            elif a.command == "earth-audit":
                emit(earth_audit.prepare(a.generation, a.collection, a.output))
            elif a.command == "earth-audit-finalize":
                emit(earth_audit.finalize(a.output, a.reviews, a.journal_reviews))
            else:
                if a.load_seconds < 0:
                    raise ValueError("load-seconds must be nonnegative")
                result = earth_delivery.check(a.cases, a.output, a.base_url, a.phase, a.load_seconds)
                emit(dict(status=result['status'], functional_passed=result['functional_passed'],
                          performance_passed=result['performance_passed'], cases=len(result['results']),
                          pending=len(result['pending']), output=a.output))
                raise SystemExit(0 if result['status'] == 'passed' else 2 if result['status'] == 'pending' else 1)
        elif a.command.startswith("search-"):
            from . import search_versions
            from .config import generation_name, generation_path
            generation_name(a.generation)
            if a.command in ("search-catalog", "search-pipeline", "search-qualify", "search-build"):
                import signal
                from .resources import Paused
                def stop_search(signum, frame):
                    raise Paused("signal-stop")
                signal.signal(signal.SIGTERM, stop_search)
            if a.command in ("search-catalog", "search-pipeline"):
                from .search_scopes import build_catalog
                from .search_incremental import pipeline
                operation = build_catalog if a.command == "search-catalog" else pipeline
                if a.background:
                    emit(manage.start(("catalog-search-" if a.command == "search-catalog" else "pipeline-search-") + a.generation))
                else:
                    emit(operation(a.generation))
            elif a.command == "search-qualify":
                from .search_storage import Qualification
                extra = ["--attempt", a.attempt] + (["--continue-build", a.continue_build] if a.continue_build else [])
                if a.background:
                    emit(manage.start("verify-search-" + a.generation, extra))
                else:
                    emit(Qualification(a.generation, a.attempt).run())
                    if a.continue_build:
                        emit(manage.start("build-search-" + a.generation, [a.continue_build, "--publish"]))
            elif a.command == "search-build":
                if a.background:
                    emit(manage.start("build-search-" + a.generation, [a.build_id] + (["--scope", a.scope] if a.scope else []) + (["--publish"] if a.publish else [])))
                else:
                    from .search_storage import require_qualification
                    require_qualification(a.generation)
                    manage.start("opensearch")
                    if a.scope:
                        from .search_incremental import build
                        emit(build(a.generation, a.build_id, a.scope))
                    else:
                        emit(search_versions.build(a.generation, a.build_id))
                    emit(search_versions.validate(a.generation, a.build_id))
                    if a.publish:
                        emit(search_versions.publish(a.generation, a.build_id))
                        emit(search_versions.observe(a.generation, a.build_id))
            elif a.command == "search-validate":
                emit(search_versions.validate(a.generation, a.build_id))
            elif a.command == "search-publish":
                emit(search_versions.publish(a.generation, a.build_id))
            elif a.command == "search-disable":
                emit(search_versions.disable(a.generation))
            else:
                progress = generation_path(a.generation) / "search-qualification-progress.json"
                emit(dict(active=search_versions.active(a.generation),
                          qualification=json.loads(progress.read_text()) if progress.exists() else None,
                          progress={name:json.loads((generation_path(a.generation)/name).read_text()) for name in ("search-catalog.json","search-pipeline.json","search-background.json","search-profile.json") if (generation_path(a.generation)/name).exists()}))
        elif a.command == "inventory":
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
            if a.service == "all" and a.command in ("start", "restart"):
                names.remove("opensearch" if config().get("search_engine") == "manticore" else "manticore")
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
                            "diagnose-storage-",
                            "validation-api-",
                            "export-earth-",
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
                    if a.service == "all" and name == ("opensearch" if config().get("search_engine") == "manticore" else "manticore"):
                        continue
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
                    config()["disk_budget_gib"] is not None
                    and cap["estimated_initial_peak_bytes"]
                    > config()["disk_budget_gib"] * 1024**3
                ):
                    raise RuntimeError(
                        "pilot projects above configured space budget; review capacity before expansion"
                    )
                if config().get("execution_profile") == "throughput96":
                    from .resources import decide
                    import psutil

                    reason = decide(
                        psutil.disk_usage(str(ROOT)).free,
                        0,
                        large=True,
                        additional=cap["estimated_initial_peak_bytes"],
                    )
                    if reason:
                        raise RuntimeError(reason)
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
            "diagnose-storage",
            "repair-index",
            "resume",
        ):
            from . import recovery, search_build, storage, workflow

            if a.command == "recover":
                from .config import generation_path
                previous_run = generation_path(a.generation) / "execution-run.json"
                a.until = a.until or (json.loads(previous_run.read_text()).get("until", "complete") if previous_run.exists() else "complete")
                if manage.controller_conflict(a.generation):
                    emit({"already_running": True, "generation": a.generation})
                    return
                if a.profile:
                    if manage.identity(manage.state_path("recover-" + a.generation)):
                        emit(
                            {
                                "already_running": True,
                                "profile_change_requires_paused_controller": True,
                            }
                        )
                        return
                    from .config import generation_path
                    from .resources import configure

                    checkpoint_path = (
                        generation_path(a.generation) / "metadata-audit-cursor.json"
                    )
                    migration_path = (
                        generation_path(a.generation) / "throughput96-migration.json"
                    )
                    if checkpoint_path.exists() and not migration_path.exists():
                        from .migration96 import migrate, legacy_signature
                        from .backend import Backend

                        client = Backend(writer=True)
                        try:
                            migrate(
                                a.generation, client, legacy_signature(a.generation), {}
                            )
                        finally:
                            client.close()
                    configure()
                if a.compatibility:
                    atomic_json(
                        ROOT / "config/local.json",
                        config()
                        | {
                            "audit_workers": 2,
                            "audit_max_workers": 2,
                            "workers": 4,
                            "search_workers": 4,
                            "search_bulk_concurrency": 2,
                            "compatibility_mode": True,
                        },
                    )
                if not manage.identity(manage.state_path("recover-" + a.generation)):
                    from .config import generation_path

                    pause = generation_path(a.generation) / "pause-request.json"
                    if pause.exists():
                        atomic_json(
                            generation_path(a.generation) / "last-pause.json",
                            json.loads(pause.read_text()),
                        )
                        pause.unlink()
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
            elif a.command == "diagnose-storage":
                from . import postings_replay

                if manage.controller_conflict(a.generation):
                    emit({"already_running": True, "generation": a.generation})
                    return
                emit(
                    manage.start("diagnose-storage-" + a.generation,
                                 ["--profile", a.profile] + (["--new-attempt"] if a.new_attempt else []))
                    if a.background
                    else postings_replay.run(a.generation, a.profile, new_attempt=a.new_attempt)
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
            if not a.force:
                from .resources import request_pause

                emit(request_pause(a.generation))
                return
            from .resources import request_pause

            request_pause(a.generation, "user-forced")
            for prefix in (
                "verify-search-",
                "build-search-",
                "recover-",
                "storage-repair-",
                "verify-",
                "diagnose-storage-",
                "audit-",
                "build-",
                "import-",
                "validation-api-",
            ):
                emit(manage.stop(prefix + a.generation, force=True))
            for service in ("verify-search-engine", "validation-api-search"):
                path = manage.state_path(service)
                if path.exists() and json.loads(path.read_text()).get("generation") == a.generation:
                    emit(manage.stop(service, force=True))
            from .storage_repair import force_stop
            force_stop(a.generation)
            from .postings_replay import force_stop as force_stop_postings
            for receipt in force_stop_postings(a.generation):
                emit(receipt)
            owner = ROOT / "var/storage-canary/owner.json"
            if (
                owner.exists()
                and json.loads(owner.read_text())["generation"] == a.generation
            ):
                emit(manage.stop("canary", force=True))
            from .config import generation_path

            atomic_json(
                generation_path(a.generation) / "control.json",
                dict(state="paused", reason="user-forced", updated=time.time()),
            )
        elif a.command == "import-stop":
            emit(manage.stop("import-" + a.generation))
        elif a.command == "validate":
            emit(ingest.validate(a.generation))
        elif a.command == "capacity":
            emit(ingest.capacity(a.generation))
        elif a.command in ["publish", "rollback"]:
            emit(ingest.publish(a.generation, a.allow_partial,
                                metadata_only=getattr(a, "metadata_only", False)))
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
