from .config import CODE_ROOT
"""Unprivileged process supervision with PID identity checks and rotating logs."""

import json, logging, logging.handlers, os, signal, subprocess, sys, threading, time
from urllib.parse import urlparse
from xml.sax.saxutils import escape
import httpx, psutil
from .config import ROOT, config, atomic_json
from .ingest import lock
from . import diagnostics

SERVICES = ["clickhouse", "opensearch", "manticore", "api", "monitor"]


def engine_configs():
    c = config()
    data = ROOT / "var/data"
    logs = ROOT / "var/log"
    conf = ROOT / "config"
    for p in [
        data / "clickhouse",
        data / "clickhouse/tmp",
        data / "clickhouse/user_files",
        data / "opensearch",
    ]:
        p.mkdir(parents=True, exist_ok=True)
    chport = urlparse(c["ch_url"]).port
    xml = f"""<clickhouse>
 <logger><level>information</level><console>true</console></logger>
 <listen_host>127.0.0.1</listen_host><http_port>{chport}</http_port><tcp_port>{c.get("ch_tcp_port", 19000)}</tcp_port>
 <path>{data}/clickhouse/</path><tmp_path>{data}/clickhouse/tmp/</tmp_path><user_files_path>{data}/clickhouse/user_files/</user_files_path>
 <users_config>{conf}/clickhouse-users.xml</users_config>
 <max_server_memory_usage>{c["clickhouse_memory_gib"] * 1024**3}</max_server_memory_usage>
 <memory_worker_use_cgroup>{str(c.get("ch_memory_worker_use_cgroup", c.get("execution_profile") != "community")).lower()}</memory_worker_use_cgroup>
 <max_thread_pool_size>512</max_thread_pool_size><background_pool_size>8</background_pool_size>
 <background_schedule_pool_size>8</background_schedule_pool_size><background_move_pool_size>2</background_move_pool_size>
 <background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size>
 <max_concurrent_queries>0</max_concurrent_queries>
 <mark_cache_size>4294967296</mark_cache_size><uncompressed_cache_size>8589934592</uncompressed_cache_size>
 <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>4</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>4</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
</clickhouse>"""
    users = f"""<clickhouse><profiles><default><max_threads>8</max_threads></default>
 <ingest><async_insert>0</async_insert><max_threads>8</max_threads><max_memory_usage>34359738368</max_memory_usage></ingest>
 <reader><readonly>1</readonly><max_threads>4</max_threads><max_memory_usage>8589934592</max_memory_usage><use_uncompressed_cache>1</use_uncompressed_cache><max_execution_time>{c["query_timeout"]}</max_execution_time></reader>
 </profiles><users>
 <ingest><password>{escape(c["ch_admin_password"])}</password><networks><ip>127.0.0.1</ip></networks><profile>ingest</profile><quota>default</quota><access_management>1</access_management></ingest>
 <api_reader><password>{escape(c["ch_reader_password"])}</password><networks><ip>127.0.0.1</ip></networks><profile>reader</profile><quota>default</quota></api_reader>
 </users><quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas></clickhouse>"""
    for name, body in [("clickhouse.xml", xml), ("clickhouse-users.xml", users)]:
        p = conf / name
        p.write_text(body)
        p.chmod(0o600)
    if c.get("search_engine") == "manticore":
        return
    (ROOT / "var/backups/opensearch").mkdir(parents=True, exist_ok=True)
    storage_profile = ROOT / "var/storage-profile.json"
    allow_mmap = (
        json.loads(storage_profile.read_text())["allow_mmap"]
        if storage_profile.exists()
        else True
    )
    search_data = data / "opensearch"
    search_logs = logs / "opensearch-engine"
    pointer = ROOT / "var/active.json"
    if pointer.exists():
        from .search_storage import qualified_options
        if qualified_options(json.loads(pointer.read_text())["generation"]) is not None:
            allow_mmap = False
            search_data = data / "search-v2"
            search_logs = logs / "search-v2"
    search_data.mkdir(exist_ok=True)
    osconf = conf / "opensearch"
    osconf.mkdir(exist_ok=True)
    body = f"""cluster.name: openalex-local
node.name: openalex-node
network.host: 127.0.0.1
http.port: {urlparse(c["os_url"]).port}
transport.port: {c.get("os_transport_port", 19300)}
discovery.type: single-node
path.data: {search_data}
path.logs: {search_logs}
plugins.security.disabled: true
node.processors: {min(c["cpus"], c.get("opensearch_processors", 16))}
bootstrap.memory_lock: false
indices.memory.index_buffer_size: 15%
path.repo: ["{ROOT}/var/backups/opensearch"]
node.store.allow_mmap: {str(allow_mmap).lower()}
cluster.routing.allocation.disk.watermark.low: 700gb
cluster.routing.allocation.disk.watermark.high: 500gb
cluster.routing.allocation.disk.watermark.flood_stage: 300gb
"""
    (osconf / "opensearch.yml").write_text(body)
    origin = ROOT / "var/engines/opensearch-3.3.2/config"
    for name in ["jvm.options", "log4j2.properties"]:
        if (origin / name).exists():
            (osconf / name).write_bytes((origin / name).read_bytes())


def identity(p, child=False):
    try:
        r = json.loads(p.read_text())
        proc = psutil.Process(r["child_pid" if child else "pid"])
        if (
            abs(proc.create_time() - r["child_create_time" if child else "create_time"])
            > 0.01
            or proc.status() == psutil.STATUS_ZOMBIE
        ):
            return None
        return proc
    except (OSError, ValueError, KeyError, psutil.Error):
        return None


def state_path(service):
    if service not in SERVICES + ["canary"] and not service.startswith(
        (
            "catalog-search-",
            "pipeline-search-",
            "export-earth-", "export-graph-enrichment-",
            "import-",
            "build-",
            "verify-",
            "recover-",
            "audit-",
            "validation-api-",
            "watch-",
            "storage-repair-",
            "diagnose-storage-",
        )
    ):
        raise ValueError("unknown service")
    if "/" in service or ".." in service:
        raise ValueError("invalid service name")
    return ROOT / "var/run" / f"{service}.json"


def resource_status():
    records = {}
    seen = set()
    rss = 0
    python_rss = 0
    for path in (ROOT / "var/run").glob("*.json"):
        proc = identity(path)
        try:
            saved = json.loads(path.read_text())
        except (OSError, ValueError):
            # A state/cache file can disappear after glob on the shared filesystem.
            # One missing observation must not terminate every service supervisor.
            continue
        if "pid" not in saved:
            continue
        orphan = identity(path, child=True) if not proc else None
        record = {
            "orphan_child": orphan.pid if orphan else None,
            "running": bool(proc),
            "pid": saved.get("pid"),
            "exit_code": saved.get("exit_code"),
            "started": saved.get("started"),
            "state": saved.get("state"),
            "stop_pending": saved.get("stop_pending", False),
            "unclean_stop": saved.get("unclean_stop", False),
        }
        owner = proc or orphan
        if owner:
            try:
                children = [owner] + owner.children(recursive=True)
                uptime = time.time() - owner.create_time()
            except psutil.Error:
                children, uptime = [], 0
            mem = 0
            for child in children:
                try:
                    if child.pid not in seen:
                        seen.add(child.pid)
                        m = child.memory_info().rss
                        rss += m
                        if "python" in child.name().lower():
                            python_rss += m
                        mem += m
                except psutil.Error:
                    pass
            record.update(rss_bytes=mem, uptime_seconds=uptime)
        records[path.stem] = record
    # Foreground controllers also count, even before a supervisor has registered them.
    own = psutil.Process()
    for child in [own] + own.children(recursive=True):
        try:
            if child.pid not in seen:
                seen.add(child.pid)
                rss += child.memory_info().rss
                if "python" in child.name().lower():
                    python_rss += child.memory_info().rss
        except psutil.Error:
            pass
    return {
        "services": records,
        "total_rss_bytes": rss,
        "python_rss_bytes": python_rss,
        "memory_enforcement": "software",
        "memory_budget_bytes": config()["memory_gib"] * 1024**3,
        "disk_free_bytes": psutil.disk_usage(str(ROOT)).free,
        "disk_warning": psutil.disk_usage(str(ROOT)).free < 1024 * 1024**3,
        "shared_volume_note": "Other workloads can consume the reserved free space.",
    }


def memory_victim(status):
    """Select one owned workload before considering an engine; no simultaneous shutdown."""
    running = [
        (name, row)
        for name, row in status["services"].items()
        if row.get("running") and row.get("rss_bytes", 0) > 0
    ]
    jobs = [
        (name, row)
        for name, row in running
        if name.startswith(("import-", "build-", "recover-", "verify-", "audit-", "storage-repair-", "diagnose-storage-", "catalog-search-", "pipeline-search-", "export-earth-", "export-graph-enrichment-"))
        or name == "canary"
    ]
    choices = jobs or [(name, row) for name, row in running if name != "monitor"]
    return min(choices, key=lambda item: item[0])[0] if choices else None


def health(service):
    c = config()
    try:
        with httpx.Client(timeout=3, trust_env=False) as h:
            if service == "clickhouse":
                r = h.get(c["ch_url"] + "/ping")
            elif service == "manticore":
                r = h.post(c.get("manticore_url", "http://127.0.0.1:19308") + "/sql?mode=raw", data={"query": "SHOW TABLES"})
            elif service in ("opensearch", "canary"):
                if service == "canary":
                    c = dict(c, os_url="http://127.0.0.1:19201")
                r = h.get(c["os_url"] + "/_cluster/health")
                return r.is_success and r.json()["status"] != "red"
            elif service.startswith("validation-api-"):
                r = h.get("http://127.0.0.1:18088/healthz")
            elif service == "api":
                r = h.get(f"http://127.0.0.1:{c['port']}/healthz")
            else:
                return bool(identity(state_path(service)))
            return r.is_success
    except (httpx.HTTPError, ValueError):
        return False


def start(service, extra=None):
    if config().get("adopted_from") and service in ("clickhouse", "manticore", "opensearch"):
        raise ValueError("adopted read-only workspaces cannot manage shared backends")
    with lock(ROOT / "var/run" / f"{service}.control.lock"):
        p = state_path(service)
        if identity(p):
            saved = json.loads(p.read_text())
            if saved.get("stop_pending"):
                raise RuntimeError("Service is still stopping; inspect stop_pending before restart")
            return {
                "service": service,
                "already_running": True,
                "healthy": health(service),
            }
        if identity(p, child=True):
            raise RuntimeError(
                "Verified orphan service process is still running; use stop before start"
            )
        if service in ["clickhouse", "opensearch"]:
            engine_configs()
        proc = subprocess.Popen(
            [sys.executable, "-m", "openalex_api.cli", "_supervise", service]
            + (extra or []),
            cwd=ROOT,
            env=os.environ | {"PYTHONPATH": str(CODE_ROOT), "OPENALEX_HOME": str(ROOT)},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.monotonic() + (
            600 if service in ["clickhouse", "opensearch", "canary"] else 20
        )
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                saved = json.loads(p.read_text()) if p.exists() else {}
                if (
                    service.startswith(
                        (
                            "import-",
                            "build-",
                            "verify-",
                            "recover-",
                            "storage-repair-",
                            "diagnose-storage-",
                            "audit-",
                            "validation-api-",
                            "export-earth-", "export-graph-enrichment-",
                        )
                    )
                    and saved.get("exit_code") == 0
                ):
                    return {"service": service, "completed": True}
                raise RuntimeError(
                    f"{service} supervisor exited; see var/log/{service}.log"
                )
            if (
                identity(p)
                and health(service)
                and (
                    not service.startswith(
                        (
                            "import-",
                            "build-",
                            "verify-",
                            "recover-",
                            "storage-repair-",
                            "diagnose-storage-",
                            "audit-",
                            "validation-api-",
                            "export-earth-", "export-graph-enrichment-",
                        )
                    )
                    or time.time() - json.loads(p.read_text())["started"] > 3
                )
            ):
                return {"service": service, "pid": proc.pid, "healthy": True}
            time.sleep(0.5)
        raise RuntimeError(
            f"{service} did not become healthy; inspect ./oa logs {service}"
        )


def stop(service, force=False):
    with lock(ROOT / "var/run" / f"{service}.control.lock"):
        p = state_path(service)
        proc = identity(p)
        if force:
            child = identity(p, child=True)
            if child:
                atomic_json(ROOT / "var/run/stop-receipts" / f"{service}.json",
                            dict(service=service, pid=child.pid, create_time=child.create_time(),
                                 unclean_stop=True, reason="explicit-force", time=time.time()))
                os.killpg(child.pid, signal.SIGKILL)
                if not proc:
                    child.wait(timeout=40)
                    return {"service": service, "forced": True, "orphan_stopped": True}
        if not proc:
            child = identity(p, child=True)
            if not child:
                return {"service": service, "already_stopped": True}
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=40)
            except psutil.TimeoutExpired:
                raise RuntimeError("Orphan child still stopping")
            return {"service": service, "orphan_stopped": True}
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=40)
        except psutil.TimeoutExpired:
            raise RuntimeError(
                "Graceful stop is still pending; inspect process/logs before retrying"
            )
        return {"service": service, "stopped": True}


def command(service, extra):
    c = config()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(CODE_ROOT)
    env["OPENALEX_HOME"] = str(ROOT)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONFAULTHANDLER"] = "1"
    if service == "manticore":
        return [sys.executable, "-m", "openalex_api.manticore_runtime"], env
    if service.startswith("export-graph-enrichment-"):
        raise ValueError("Graph enrichment belongs to a separate downstream project")
    if service.startswith("verify-manticore-speed-"):
        return [sys.executable, "-m", "openalex_api.manticore_monitor", *(extra or [])], env
    if service.startswith(("build-manticore-", "verify-manticore-")):
        return [sys.executable, "-m", "openalex_api.manticore_build", *(extra or [])], env
    if service == "clickhouse":
        return [
            str(ROOT / "var/engines/clickhouse-compat/clickhouse"),
            "server",
            "--config-file=" + str(ROOT / "config/clickhouse.xml"),
        ], env
    if service == "opensearch":
        from .storage import runtime_options
        from .continuous_storage import qualified_capture_options
        from .search_storage import qualified_options
        pointer = ROOT / "var/active.json"
        qualified = qualified_options(json.loads(pointer.read_text())["generation"]) if pointer.exists() else None

        home = ROOT / "var/engines/opensearch-3.3.2"
        env.update(
            OPENSEARCH_PATH_CONF=str(ROOT / "config/opensearch"),
            OPENSEARCH_JAVA_HOME=str(home / "jdk"),
            OPENSEARCH_JAVA_OPTS=f"-Xms{c['opensearch_heap_gib']}g -Xmx{c['opensearch_heap_gib']}g -XX:ActiveProcessorCount={min(c['cpus'], c.get('opensearch_processors', 16))}" + " " + " ".join(qualified if qualified is not None else runtime_options() + qualified_capture_options()),
        )
        return [str(home / "bin/opensearch")], env
    if service.startswith("storage-repair-"):
        return [sys.executable, str(CODE_ROOT / "scripts/storage_merge_recover.py"), service[len("storage-repair-"):], *(extra or [])], env
    for prefix, command in (("export-earth-", "earth-export"), ("verify-search-", "search-qualify"), ("build-search-", "search-build"), ("catalog-search-", "search-catalog"), ("pipeline-search-", "search-pipeline")):
        if service.startswith(prefix):
            return [sys.executable, "-m", "openalex_api.cli", command, service[len(prefix):], *(extra or [])], env
    if service.startswith("diagnose-storage-"):
        return [sys.executable, "-m", "openalex_api.postings_replay",
                service[len("diagnose-storage-"):], *(extra or [])], env
    if service.startswith("watch-"):
        return [
            sys.executable,
            str(CODE_ROOT / "scripts/watch_full_build.py"),
            service[6:],
        ], env
    if service == "monitor":
        return [sys.executable, "-m", "openalex_api.monitor"], env
    if service == "canary":
        from .storage import canary_config, runtime_options

        home = ROOT / "var/engines/opensearch-3.3.2"
        env.update(
            OPENSEARCH_PATH_CONF=str(canary_config()),
            OPENSEARCH_JAVA_HOME=str(home / "jdk"),
            OPENSEARCH_JAVA_OPTS="-Xms8g -Xmx8g -XX:ActiveProcessorCount=4 " + " ".join(runtime_options()),
        )
        return [str(home / "bin/opensearch")], env
    for prefix, operation in [
        ("recover-", "recover"),
        ("verify-", "verify-storage"),
        ("audit-", "audit"),
    ]:
        if service.startswith(prefix):
            return [
                sys.executable,
                "-m",
                "openalex_api.cli",
                operation,
                service[len(prefix) :],
            ] + (extra if operation == "recover" else []), env
    if service == "api" or service.startswith("validation-api-"):
        validation = service != "api"
        if c.get("adopted_from"):
            env["OPENALEX_GENERATION"] = c["adopted_from"]["generation"]
            env["OPENALEX_SEARCH_BUILD"] = c["adopted_from"]["build"]
        if validation:
            env["OPENALEX_GENERATION"] = service[len("validation-api-") :]
        workers = int(c.get("api_workers", 1))
        if workers < 1:
            raise ValueError("api_workers must be positive")
        if workers > 1 and (c["qps"] > 0 or c["concurrency"] > 0):
            raise ValueError("Multiple API workers require qps=0 and concurrency=0; caller limits are process-local")
        return [
            sys.executable,
            "-m",
            "uvicorn",
            "openalex_api.api:app",
            "--host",
            "127.0.0.1" if validation else c["host"],
            "--port",
            "18088" if validation else str(c["port"]),
            "--workers",
            str(workers),
            "--no-access-log",
        ], env
    if service.startswith("build-"):
        return [sys.executable, "-m", "openalex_api.build", service[6:]], env
    if service.startswith("import-"):
        return [sys.executable, "-m", "openalex_api.cli", "import", service[7:]], env
    raise ValueError("unknown service")


def supervise(service, extra=None):
    with lock(ROOT / "var/run" / f"{service}.supervisor.lock"):
        logger = logging.getLogger("supervisor")
        logger.setLevel(logging.INFO)
        handler = logging.handlers.RotatingFileHandler(
            ROOT / "var/log" / f"{service}.log", maxBytes=32 * 1024**2, backupCount=5
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
        p = state_path(service)
        own = psutil.Process()
        state = dict(
            pid=os.getpid(),
            create_time=own.create_time(),
            started=time.time(),
            exit_code=None,
        )
        atomic_json(p, state)
        cfg = config()
        cpus = own.cpu_affinity()
        own.cpu_affinity(cfg.get("cpu_affinity", cpus[: cfg["cpus"]]))
        child = None
        try:
            cmd, env = command(service, extra or [])
            child = subprocess.Popen(
                cmd,
                cwd=ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            state["child_pid"] = child.pid
            state["child_create_time"] = psutil.Process(child.pid).create_time()
            atomic_json(p, state)
            stopping = False

            def terminate(signum=None, frame=None):
                nonlocal stopping
                stopping = True
                if child.poll() is None:
                    try:
                        os.killpg(child.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass

            signal.signal(signal.SIGTERM, terminate)
            signal.signal(signal.SIGINT, terminate)

            def drain():
                for raw in iter(child.stdout.readline, b""):
                    logger.info(raw.decode("utf-8", errors="replace").rstrip())

            thread = threading.Thread(target=drain, daemon=True)
            thread.start()
            stop_at = None
            while child.poll() is None:
                resources = resource_status()
                if (
                    not stopping
                    and resources["total_rss_bytes"]
                    > (
                        960
                        if cfg.get("execution_profile") == "throughput96"
                        else cfg["memory_gib"]
                    )
                    * 1024**3
                    and memory_victim(resources) == service
                ):
                    logger.error(
                        "Project aggregate RSS exceeds memory budget; stopping service"
                    )
                    state["stop_reason"] = "aggregate-rss-budget"
                    state["diagnostic_path"] = diagnostics.incident(
                        "memory-budget",
                        {"service": service, "resources": resources},
                        stacks=True,
                    )
                    atomic_json(p, state)
                    terminate()
                if stopping:
                    stop_at = stop_at or time.monotonic()
                    if time.monotonic() - stop_at > 30 and not state.get("stop_pending"):
                        state.update(stop_pending=True, state="stop_pending", stop_requested_at=time.time())
                        emergency = state.get("stop_reason") == "aggregate-rss-budget"
                        if emergency:
                            state.update(unclean_stop=True, force_reason="aggregate-rss-budget")
                        atomic_json(p, state)
                        if emergency:
                            logger.error("Emergency memory stop timed out; killing owned child process group")
                            try:
                                os.killpg(child.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                        else:
                            logger.warning("Graceful stop still pending; owned child retained")
                time.sleep(1)
            thread.join(timeout=2)
            if child.returncode != 0 and not state.get("diagnostic_path"):
                state["diagnostic_path"] = diagnostics.incident(
                    "service-exit", {"service": service, "returncode": child.returncode}
                )
            state.update(exit_code=child.returncode, stopped=time.time(), stop_pending=False, state="stopped")
            atomic_json(p, state)
            logger.info("Service exited with code %s", child.returncode)
        except Exception:
            logger.exception("Supervisor failed")
            state.update(exit_code=1, stopped=time.time())
            atomic_json(p, state)
            if child and child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            raise


def controller_conflict(g, current_pid=None):
    """Exclude this supervisor's own child when entering the managed CLI."""
    current_pid = os.getpid() if current_pid is None else current_pid
    for prefix in ("recover-", "build-", "storage-repair-", "diagnose-storage-", "verify-search-", "build-search-"):
        path = state_path(prefix + g)
        child = identity(path, child=True)
        parent = identity(path)
        if (child and child.pid == current_pid) or (parent and parent.pid == os.getppid()):
            # The supervisor may not yet have fsynced child_pid on a slow filesystem.
            continue
        if child or parent:
            return True
    return False
