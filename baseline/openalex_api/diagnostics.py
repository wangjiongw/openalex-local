"""Bounded, credential-free resource history and best-effort failure evidence."""
import functools
import json
import logging
import logging.handlers
import os
from pathlib import Path
import subprocess
import tempfile
import time
import traceback

import psutil
from .config import ROOT, atomic_json

LOG = logging.getLogger(__name__)


def read_text(path, limit=65536):
    try:
        with open(path) as f:
            return f.read(limit)
    except OSError as ex:
        return {"unavailable": type(ex).__name__}


def snapshot():
    processes, seen = [], set()
    # No command line/environment, open file paths or connection peers: avoid credentials.
    seeds = [("caller", psutil.Process())]
    for state in (ROOT / "var/run").glob("*.json"):
        try:
            data = json.loads(state.read_text())
            for key in ("pid", "child_pid"):
                if key not in data:
                    continue
                proc = psutil.Process(data[key])
                birth = data.get("child_create_time" if key == "child_pid" else "create_time", -1)
                if abs(proc.create_time() - birth) < .01:
                    seeds.append((state.stem, proc))
        except (OSError, ValueError, psutil.Error):
            continue
    for service, parent in seeds:
        try:
            family = [parent] + parent.children(recursive=True)
        except psutil.Error:
            continue
        for proc in family:
            if proc.pid in seen:
                continue
            seen.add(proc.pid)
            try:
                with proc.oneshot():
                    row = dict(service=service, pid=proc.pid, created=proc.create_time(), name=proc.name(),
                        rss=proc.memory_info().rss, vms=proc.memory_info().vms,
                        threads=proc.num_threads(), fds=proc.num_fds(),
                        nofile=list(proc.rlimit(psutil.RLIMIT_NOFILE)),
                        cpu_seconds=proc.cpu_times()._asdict(), io=proc.io_counters()._asdict(),
                        affinity=proc.cpu_affinity())
                processes.append(row)
            except (OSError, psutil.Error):
                continue
    # Resolve this process's cgroup v2, rather than assuming host root is its limit.
    cg = read_text('/proc/self/cgroup')
    path = Path('/sys/fs/cgroup')
    if isinstance(cg, str):
        for line in cg.splitlines():
            if line.startswith('0::'):
                candidate = path / line[3:].lstrip('/')
                if candidate.exists():
                    path = candidate
    cgroup = {name: read_text(path / name) for name in (
        'memory.current', 'memory.max', 'memory.events', 'memory.stat',
        'cpu.max', 'cpu.stat', 'pids.current', 'pids.max', 'io.stat',
        'memory.pressure', 'cpu.pressure', 'io.pressure')}
    disk = psutil.disk_usage(str(ROOT))
    fs = os.statvfs(ROOT)
    return dict(time=time.time(), processes=processes,
        aggregate_rss=sum(p['rss'] for p in processes), rss_note='RSS can double-count shared pages; not PSS',
        host_memory=psutil.virtual_memory()._asdict(), swap=psutil.swap_memory()._asdict(),
        load=os.getloadavg(), disk=disk._asdict(), free_inodes=fs.f_favail,
        pressure={k:read_text('/proc/pressure/'+k) for k in ('cpu','memory','io')},
        file_nr=read_text('/proc/sys/fs/file-nr'), cgroup_path=str(path), cgroup=cgroup)


def backend_snapshot():
    """Read only bounded engine counters; no user queries/documents or credentials in output."""
    import httpx
    from .config import config
    c = config()
    paths = {
        "opensearch": (c["os_url"] + "/_nodes/stats/jvm,process,thread_pool,breaker,indices?filter_path=nodes.*.jvm.mem,nodes.*.jvm.gc,nodes.*.process.open_file_descriptors,nodes.*.process.max_file_descriptors,nodes.*.thread_pool,nodes.*.breakers,nodes.*.indices.merges,nodes.*.indices.indexing", {}),
        "clickhouse": (c["ch_url"], {"auth":("api_reader", c["ch_reader_password"]), "params":{"query":
            "SELECT metric,value FROM system.metrics WHERE metric IN ('MemoryTracking','Query','Merge','BackgroundMergesAndMutationsPoolTask','OpenFileForRead','OpenFileForWrite') FORMAT JSONEachRow"}})
    }
    result = {}
    with httpx.Client(timeout=3, trust_env=False) as client:
        for name, (url, options) in paths.items():
            try:
                with client.stream('GET', url, **options) as response:
                    body = bytearray()
                    for chunk in response.iter_bytes():
                        body.extend(chunk)
                        if len(body) > 1024*1024:
                            raise ValueError('counter response exceeds 1 MiB')
                    result[name] = {'status':response.status_code, 'body':body.decode('utf-8', errors='replace')}
            except Exception as ex:
                result[name] = {'unavailable':type(ex).__name__}
    return result


def tail(path, size=256*1024):
    with open(path, 'rb') as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell()-size))
        return f.read(size)


def incident(kind, context=None, stacks=False):
    """Never mask the original failure. Unique evidence, no engine mutation or heap dump."""
    try:
        base = ROOT / 'var/diagnostics'
        base.mkdir(parents=True, exist_ok=True)
        path = Path(tempfile.mkdtemp(prefix=time.strftime('%Y%m%dT%H%M%S')+'-', dir=base))
        atomic_json(path/'incident.json', dict(kind=kind, time=time.time(), context=context or {}, traceback=traceback.format_exc()))
        resources = snapshot()
        atomic_json(path/'resources.json', resources)
        # Bounded tails survive subsequent service log rotation; exclude config and raw data.
        logs = sorted((ROOT/'var/log').glob('*.log'))[:12]
        logs += [ROOT/'var/log/resources.jsonl']
        logs += list((ROOT/'var/log/opensearch-engine').glob('*.log'))[:4]
        logs += [ROOT/'var/engines/opensearch-3.3.2/logs/gc.log']
        logs += list((ROOT/'var/engines/opensearch-3.3.2/logs').glob('hs_err_pid*.log'))[-3:]
        for log in logs:
            try:
                (path/log.name).write_bytes(tail(log))
            except OSError:
                pass
        if stacks:
            atomic_json(path/'backends.json', backend_snapshot())
            java = ROOT/'var/engines/opensearch-3.3.2/jdk/bin/jcmd'
            for proc in [p for p in resources['processes'] if p['name']=='java'][:3]:
                try:
                    live = psutil.Process(proc['pid'])
                    if abs(live.create_time()-proc['created']) > .01:
                        continue
                    with (path/f"jvm-{proc['pid']}-threads.txt").open('w') as output:
                        subprocess.run([str(java),str(proc['pid']),'Thread.print'], stdout=output,
                            stderr=subprocess.STDOUT, timeout=8, check=False)
                except (OSError, psutil.Error, subprocess.TimeoutExpired) as ex:
                    (path/f"jvm-{proc['pid']}-error.txt").write_text(type(ex).__name__)
        LOG.error('Diagnostic evidence: %s', path)
        return str(path)
    except Exception:
        LOG.exception('Diagnostic capture incomplete; preserving original failure')
        return None


def guarded_stage(function):
    @functools.wraps(function)
    def wrapped(g, *args, **kwargs):
        try:
            return function(g, *args, **kwargs)
        except Exception as ex:
            from .config import generation_path
            p = generation_path(g)/'build.json'
            try:
                previous = json.loads(p.read_text()) if p.exists() else {}
                evidence = previous.get("diagnostic_path") if previous.get("stage") == "failed" else None
                evidence = evidence or incident(function.__name__, {'generation':g, 'error':repr(ex), 'previous':previous}, stacks=True)
                atomic_json(p, dict(generation=g, stage='failed', failed_stage=previous.get('failed_stage') or previous.get('stage'),
                    updated=time.time(), error=repr(ex), diagnostic_path=evidence))
            except Exception:
                LOG.exception('Could not record failed stage')
            raise
    return wrapped


def sample_loop(stop):
    logger = logging.getLogger('resource-history')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.handlers.RotatingFileHandler(ROOT/'var/log/resources.jsonl', maxBytes=16*1024**2, backupCount=8)
    logger.addHandler(handler)
    next_backend = 0
    try:
        while not stop.is_set():
            try:
                data = snapshot()
                if time.monotonic() >= next_backend:
                    data["backends"] = backend_snapshot()
                    next_backend = time.monotonic() + 60
                logger.info(json.dumps(data, separators=(',', ':')))
                atomic_json(ROOT/'var/run/resource-latest.json', data)
            except Exception:
                LOG.exception('Resource sample failed; sampling will retry')
            stop.wait(15)
    finally:
        logger.removeHandler(handler)
        handler.close()
