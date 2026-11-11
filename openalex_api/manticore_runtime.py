"""Run the local engine and restore the published build after loss of Pod storage."""
import signal
import subprocess
import time

from .config import ROOT, generation_path, atomic_json, config
from .earth_control import read_json
from .manticore_engine import Client, engine_config
from .manticore_build import restore
from .search_versions import active, build_path


def configure_queries(client):
    threads = config().get("manticore_max_threads_per_query", 0)
    if type(threads) is not int or threads < 0:
        raise ValueError("manticore_max_threads_per_query must be a nonnegative integer")
    # Bound per-query fan-out so concurrent searches can share the engine worker pool.
    client.sql("SET GLOBAL max_threads_per_query=" + str(threads))



def configure_workers(path):
    cfg = config()
    threads = cfg.get("manticore_workers", min(32, cfg["cpus"]))
    if type(threads) is not int or threads < 1:
        raise ValueError("manticore_workers must be a positive integer")
    # Worker scheduling is runtime-only; the versioned index projection stays unchanged.
    original = f" threads = {min(32, cfg['cpus'])}\n"
    text = path.read_text()
    if text.count(original) != 1:
        raise RuntimeError("generated Manticore worker setting is missing or ambiguous")
    path.write_text(text.replace(original, f" threads = {threads}\n"))


def recover_published():
    pointer = ROOT / "var/active.json"
    if not pointer.exists():
        return
    g = read_json(pointer)["generation"]
    if not (generation_path(g) / "search-active.json").exists():
        return
    manifest = active(g)
    if not manifest or manifest.get("engine") != "manticore":
        return
    state_path = ROOT / "var/run/manticore-recovery.json"
    state = dict(state="restoring", build=manifest["build"], started=time.time(), restored=0)
    atomic_json(state_path, state)
    client = Client(timeout=600)
    try:
        for segment in manifest["segments"]:
            restore(client, build_path(g, manifest["build"]), segment)
            state.update(restored=state["restored"] + 1, updated=time.time())
            atomic_json(state_path, state)
        state.update(state="complete", completed=time.time())
        atomic_json(state_path, state)
    except Exception as error:
        state.update(state="failed", error=str(error), updated=time.time())
        atomic_json(state_path, state)
        raise
    finally:
        client.close()


def main():
    from .manticore_formats import engine_env
    conf, binary = engine_config()
    configure_workers(conf)
    env = engine_env()
    version = subprocess.check_output([str(binary), "--version"], env=env, text=True)
    if "(secondary " not in version or "(columnar " not in version:
        raise RuntimeError("Manticore secondary/columnar modules did not load")
    proc = subprocess.Popen([str(binary), "--config", str(conf), "--nodetach"], env=env)
    def stop(signum, frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        client = Client(timeout=1)
        try:
            for _ in range(240):
                if proc.poll() is not None:
                    raise RuntimeError("Manticore exited during startup")
                try:
                    client.rows("SHOW TABLES")
                    break
                except Exception:
                    time.sleep(.25)
            else:
                raise RuntimeError("Manticore startup timed out")
            configure_queries(client)
        finally:
            client.close()
        recover_published()
        from .arxiv_pipeline import recover as recover_arxiv
        recover_arxiv()
        raise SystemExit(proc.wait())
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=30)


if __name__ == "__main__":
    main()
