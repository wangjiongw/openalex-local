"""Restore the published snapshots into an empty engine without touching production."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import httpx

from openalex_api.config import ROOT, CODE_ROOT, atomic_json, config
from openalex_api.earth_delivery import check
from openalex_api.manticore_build import file_hash, restore as restore_segment
from openalex_api.manticore_engine import Client, runtime_root
from openalex_api.manticore_formats import engine_env
from openalex_api.search_versions import active, read, build_path


def prepare_home(source, home, engine_port, api_port):
    source, home = source.resolve(), home.resolve()
    if (home == source or home.is_relative_to(source) or source.is_relative_to(home)
            or home == runtime_root().resolve() or any(home.iterdir())):
        raise ValueError("recovery home must be a new empty directory outside production")
    for name in ("config", "var/run", "var/log", "engine/data", "engine/binlog"):
        (home / name).mkdir(parents=True, exist_ok=True)
    for name in ("var/engines", "var/generations", "exports"):
        (home / name).symlink_to(source / name, target_is_directory=True)
    if (source / "var/active.json").exists():
        shutil.copy2(source / "var/active.json", home / "var/active.json")
    cfg = json.loads((source / "config/local.json").read_text())
    cfg.update(manticore_url=f"http://127.0.0.1:{engine_port}",
               manticore_runtime=str(home / "engine"), host="127.0.0.1", port=api_port)
    atomic_json(home / "config/local.json", cfg)
    return os.environ | {"OPENALEX_HOME": str(home), "PYTHONPATH": str(CODE_ROOT), "PYTHONDONTWRITEBYTECODE": "1"}


def inventory(path, manifest):
    records = []
    for segment in manifest["segments"]:
        for kind, entries, directory in (
            ("snapshot", segment["snapshot"]["files"], path / segment["snapshot"]["path"]),
            ("receipt", segment["batches"], path),
        ):
            for item in entries:
                file = (directory / item["name" if kind == "snapshot" else "path"]).resolve()
                roots = [ROOT.resolve()]
                adopted = config().get("adopted_from")
                if adopted:
                    roots.append(Path(adopted["workspace"]).resolve())
                if not any(file.is_relative_to(root) for root in roots):
                    raise ValueError("artifact is outside persistent project: " + str(file))
                size = file.stat().st_size
                if "bytes" in item and size != item["bytes"]:
                    raise ValueError("artifact size mismatch: " + str(file))
                records.append(dict(kind=kind, segment=segment["index"], path=str(file), bytes=size,
                                    sha256=item["sha256"]))
    return records


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def restore(generation, build, output, allow_sealed=False):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / "result.json").exists():
        raise ValueError("use a new output directory; previous recovery evidence is retained")
    manifest = read(generation, build) if allow_sealed else active(generation)
    if not manifest or manifest["build"] != build or manifest["engine"] != "manticore":
        raise ValueError("restore only the currently published Manticore build")
    path = build_path(generation, build)
    artifacts = inventory(path, manifest)
    snapshot_bytes = sum(x["bytes"] for x in artifacts if x["kind"] == "snapshot")
    if shutil.disk_usage(tempfile.gettempdir()).free < snapshot_bytes * 2:
        raise RuntimeError("isolated restore requires twice the snapshot size in free temporary space")
    atomic_json(output / "artifacts.json", artifacts)
    frozen = {str(p): file_hash(p) if p.exists() else None for p in (ROOT / "var/active.json",
              path.parent.parent / "search-active.json", path / "build.json")}
    home = Path(tempfile.mkdtemp(prefix="openalex-full-restore-"))
    engine_port, api_port = free_port(), free_port()
    while api_port == engine_port:
        api_port = free_port()
    env = prepare_home(ROOT, home, engine_port, api_port)
    env.update(OPENALEX_GENERATION=generation, OPENALEX_SEARCH_BUILD=build, OPENALEX_SEARCH_VALIDATION="1" if allow_sealed else "0")
    conf = home / "config/manticore.conf"
    conf.write_text("searchd {\n" + f" listen = 127.0.0.1:{engine_port}:http\n"
                    f" log = {home}/engine/searchd.log\n pid_file = {home}/engine/searchd.pid\n"
                    f" data_dir = {home}/engine/data\n binlog_path = {home}/engine/binlog\n"
                    " binlog_flush = 1\n threads = 8\n max_packet_size = 64M\n buddy_path =\n}\n")
    state = dict(state="restoring", passed=False, generation=generation, build=build,
                 started=time.time(), runtime=str(home), snapshot_bytes=snapshot_bytes,
                 engine_url=f"http://127.0.0.1:{engine_port}", api_url=f"http://127.0.0.1:{api_port}",
                 restored=[], expected_segments=len(manifest["segments"]), frozen=frozen,
                 limitations=["fresh local engine; no physical Pod deletion", "existing metadata server reused read-only"])
    processes = []
    client = Client(state["engine_url"], timeout=600)
    try:
        with (output / "engine.log").open("ab") as log:
            engine = subprocess.Popen([str(ROOT / "var/engines/manticore-29.9.0/usr/bin/searchd"),
                                       "--config", str(conf), "--nodetach"], env=engine_env(), stdout=log, stderr=log)
            processes.append(engine)
        state["engine_pid"] = engine.pid
        atomic_json(output / "result.json", state)
        for _ in range(240):
            if engine.poll() is not None:
                raise RuntimeError("isolated engine exited during startup")
            try:
                assert client.rows("SHOW TABLES") == [], "fresh engine is not empty"
                break
            except httpx.HTTPError:
                time.sleep(.25)
        else:
            raise RuntimeError("isolated engine startup timed out")
        client.sql("SET GLOBAL max_threads_per_query=4")
        for segment in manifest["segments"]:
            started = time.monotonic()
            print("Restoring " + segment["index"], flush=True)
            restore_segment(client, path, segment)
            state["restored"].append(dict(index=segment["index"], entity=segment["entity"],
                                          count=segment["count"], seconds=time.monotonic()-started))
            state["updated"] = time.time()
            atomic_json(output / "result.json", state)
        state.update(state="checking_api", restore_seconds=time.time()-state["started"])
        atomic_json(output / "result.json", state)
        with (output / "api.log").open("ab") as log:
            api = subprocess.Popen([sys.executable, "-m", "uvicorn", "openalex_api.api:app", "--host",
                                    "127.0.0.1", "--port", str(api_port), "--no-access-log"],
                                   env=env, stdout=log, stderr=log)
            processes.append(api)
        with httpx.Client(base_url=state["api_url"], trust_env=False, timeout=20) as http:
            for _ in range(240):
                if api.poll() is not None:
                    raise RuntimeError("isolated API exited during startup")
                try:
                    http.get("/healthz").raise_for_status()
                    break
                except httpx.HTTPError:
                    time.sleep(.25)
            else:
                raise RuntimeError("isolated API startup timed out")
        report = check(path / "qualification/cases.json", output / "functional", state["api_url"], "all")
        if not report["passed"]:
            raise RuntimeError("restored API failed functional acceptance")
        if any((file_hash(Path(p)) if Path(p).exists() else None) != digest for p, digest in frozen.items()):
            raise RuntimeError("production pointer or manifest changed during restore")
        state.update(state="complete", passed=True, completed=time.time(),
                     functional_checks=len(report["results"]), snapshot_hashes_verified=True,
                     all_receipts_and_members_verified=True)
    except BaseException as error:
        state.update(state="failed", error=type(error).__name__ + ": " + str(error), completed=time.time())
        raise
    finally:
        client.close()
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
        state["isolated_processes_stopped"] = all(p.poll() is not None for p in processes)
        atomic_json(output / "result.json", state)
    return state
