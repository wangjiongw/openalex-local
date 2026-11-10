"""Kill only a freshly owned isolated engine, then verify acknowledged writes and replay."""
import argparse
import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from openalex_api.config import ROOT, atomic_json
from openalex_api.manticore_engine import Client, ENGINE_VERSION, project
from openalex_api.manticore_build import create, write_batch, verify_segment, seal, file_hash, signature
from openalex_api.semantics import digest

from openalex_api.manticore_formats import engine_env
parser = argparse.ArgumentParser()
parser.add_argument("--layout", choices=("legacy", "native_v2"), default="legacy")
parser.add_argument("--bulk-workers", type=int, choices=(1, 2, 4), default=1)
args = parser.parse_args()
if args.layout == "native_v2":
    from openalex_api.manticore_formats import create, project_native as project
root = Path(tempfile.mkdtemp(prefix="openalex-manticore-crash-"))
for name in ("data", "binlog"):
    (root / name).mkdir()
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
conf = root / "searchd.conf"
conf.write_text(f"searchd {{\n listen = 127.0.0.1:{port}:http\n log = {root}/engine.log\n"
                f" pid_file = {root}/pid\n data_dir = {root}/data\n binlog_path = {root}/binlog\n"
                " binlog_flush = 1\n threads = 4\n buddy_path =\n}\n")
binary = ROOT / "var/engines/manticore-29.9.0/usr/bin/searchd"
c = Client(f"http://127.0.0.1:{port}")
log = (root / "stdout.log").open("ab")


def start():
    proc = subprocess.Popen([str(binary), "--config", str(conf), "--nodetach"], stdout=log, stderr=log, env=engine_env())
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError("isolated engine exited: " + str(root))
        try:
            c.rows("SHOW TABLES")
            return proc
        except Exception:
            time.sleep(.1)
    proc.terminate()
    proc.wait(timeout=10)
    raise RuntimeError("isolated engine did not start")


proc = start()
try:
    create(c, "crash_fixture")
    segment = dict(index="crash_fixture", entity="works", count=0, batches=[], physical_layout=args.layout)
    rows = []
    for n in range(1, 201):
        r = dict(id="W" + str(n), title="Ocean circulation and climate change", publication_year=2020,
                 open_access=dict(is_oa=True))
        rows.append(dict(id=r["id"], raw=json.dumps(r), version=1, digest=digest(r)))
    write_batch(c, None, root, segment, rows, bulk_workers=args.bulk_workers)
    # This engine was created above; no production process or shared runtime is killed.
    proc.kill()
    assert proc.wait(timeout=20) != 0
    proc = start()
    verify_segment(c, root, segment)
    c.bulk("crash_fixture", [project("works", r, "", []) for r in rows])
    verify_segment(c, root, segment)
    seal(c, root, segment)
    atomic_json(ROOT / ("reports/manticore-release-20261108/crash-recovery-native.json" if args.layout == "native_v2" else
                        "reports/manticore-implementation-20261106/crash-recovery.json"),
                dict(passed=True, time=time.time(), engine_version=ENGINE_VERSION, binary_sha256=file_hash(binary),
                     projection_signature=signature(args.layout), bulk_workers=args.bulk_workers, records=200, process_exit="SIGKILL",
                     acknowledged_writes_survived=True, idempotent_replay=True, snapshot_restored=True,
                     evidence_path=str(root), limitations=["isolated engine; does not simulate Pod loss or 20M load"]))
finally:
    c.close()
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=30)
    log.close()
