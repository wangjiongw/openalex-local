"""Evidence-backed snapshot preservation and isolated real-data storage soak."""

import json
import gzip
import logging
import os
import shutil
import subprocess
import time
import tempfile
import hashlib
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import orjson
from .config import ROOT, config, generation_path, atomic_json
from .backend import Backend, database, index, index_expression, sql_string
from .model import mapping, project
from . import ingest, recovery, diagnostics

LOG = logging.getLogger(__name__)


def archive(g, final=False):
    from . import manage

    p = generation_path(g)
    with ingest.lock(p / "import.lock"):
        report_name = "archive-stable" if final else "archive"
        try:
            return recovery.require_evidence(g, report_name)
        except RuntimeError:
            pass
        ingest.guard_capacity(config(), 0, 0)
        # Restart loads the repository path; it does not switch the production storage mode.
        b = Backend(writer=True)
        try:
            nodes = b.os("GET", "/_nodes/settings")
            ready = all(
                str(ROOT / "var/backups/opensearch")
                in json.dumps(v.get("settings", {}))
                for v in nodes["nodes"].values()
            )
            if not ready:
                b.os("POST", "/" + index_expression(g) + "/_flush?wait_if_ongoing=true")
        finally:
            b.close()
        if not ready:
            manage.stop("opensearch")
            manage.start("opensearch")
        b = Backend(writer=True)
        try:
            b.os(
                "PUT",
                "/_snapshot/openalex-recovery",
                {
                    "type": "fs",
                    "settings": {
                        "location": str(ROOT / "var/backups/opensearch"),
                        "compress": True,
                    },
                },
            )
            b.os("POST", "/_snapshot/openalex-recovery/_verify", {})
            name = ("stable-" if final else "before-recovery-") + g
            probe = b.http.get(b.cfg["os_url"] + "/_snapshot/openalex-recovery/" + name)
            if probe.status_code == 404:
                b.os("POST", "/" + index_expression(g) + "/_flush?wait_if_ongoing=true")
                b.os(
                    "PUT",
                    "/_snapshot/openalex-recovery/" + name,
                    {
                        "indices": index_expression(g),
                        "include_global_state": False,
                        "partial": False,
                    },
                )
            elif not probe.is_success:
                probe.raise_for_status()
            while True:
                snap = b.os("GET", "/_snapshot/openalex-recovery/" + name)["snapshots"][
                    0
                ]
                LOG.info("Snapshot %s: %s", name, snap["state"])
                if snap["state"] == "SUCCESS":
                    break
                if snap["state"] not in ("IN_PROGRESS", "STARTED"):
                    raise RuntimeError("Snapshot failed: " + str(snap))
                ingest.guard_capacity(config(), 0, 0)
                time.sleep(30)
            # Test an actual restore to a different index, not just repository connectivity.
            source = index(g, "topics")
            target = "restore-probe-" + g
            exists = b.http.head(b.cfg["os_url"] + "/" + target).status_code == 200
            if not exists:
                b.os(
                    "POST",
                    "/_snapshot/openalex-recovery/" + name + "/_restore",
                    {
                        "indices": source,
                        "include_global_state": False,
                        "rename_pattern": source,
                        "rename_replacement": target,
                    },
                )
            deadline = time.monotonic() + 1800
            while time.monotonic() < deadline:
                h = b.os("GET", "/_cluster/health/" + target)
                if h["status"] == "green":
                    break
                time.sleep(5)
            else:
                raise RuntimeError("Restored test index did not become healthy")
            n = b.os("GET", "/" + source + "/_count")["count"]
            restored = b.os("GET", "/" + target + "/_count")["count"]
            if n != restored:
                raise RuntimeError("Snapshot restore count mismatch")
            b.os("DELETE", "/" + target)
            return recovery.evidence(
                g,
                report_name,
                passed=True,
                repository="openalex-recovery",
                snapshot=name,
                restore_entity="topics",
                restored_records=restored,
            )
        finally:
            b.close()


@contextmanager
def index_check_copy(scratch, source, logfile):
    """Delete successful scratch copies; preserve failed evidence without masking the error."""
    tmp = Path(tempfile.mkdtemp(prefix="shard-", dir=scratch))
    try:
        yield tmp
    except BaseException as error:
        report = {
            "source": str(source),
            "copy": str(tmp / "index"),
            "time": time.time(),
            "error": repr(error),
            "files": {},
        }
        LOG.error("Index check failed; preserving evidence at %s", tmp)
        try:
            if logfile.exists():
                shutil.copyfile(logfile, tmp / "check.log")
            # Publish location first, even if reading hashes fails or the process is stopped.
            atomic_json(tmp / "failure.json", report)
            report["diagnostic_path"] = diagnostics.incident("index-check", {"source": str(source), "copy": str(tmp), "error": repr(error)}, stacks=True)
            atomic_json(tmp / "failure.json", report)
            for file in sorted((tmp / "index").glob("*")):
                if not file.is_file() or file.name == "write.lock":
                    continue
                record = {}
                for label, path in (("copy", file), ("source", source / file.name)):
                    try:
                        h = hashlib.sha256()
                        with path.open("rb") as stream:
                            for block in iter(
                                lambda: stream.read(8 * 1024 * 1024), b""
                            ):
                                h.update(block)
                        record[label + "_sha256"] = h.hexdigest()
                    except OSError as ex:
                        record[label + "_error"] = str(ex)
                report["files"][file.name] = record
            atomic_json(tmp / "failure.json", report)
        except Exception:
            LOG.exception(
                "Could not finish evidence metadata; retained files at %s", tmp
            )
        raise
    else:
        shutil.rmtree(tmp)


def assert_index_closed(b, name):
    rows = b.os("GET", "/_cat/indices/" + name + "?format=json&h=index,status")
    if (
        len(rows) != 1
        or rows[0].get("index") != name
        or rows[0].get("status") != "close"
    ):
        raise RuntimeError("Source index is not closed: " + name)


def prepare_index_checker():
    java = ROOT / "var/engines/opensearch-3.3.2/jdk/bin"
    cp = str(ROOT / "var/engines/opensearch-3.3.2/lib/*")
    digest = hashlib.sha256(b"".join((ROOT/name).read_bytes() for name in
        ("scripts/ReadOnlyIndexCheck.java", "scripts/CaptureAgent.java", "config/engines.lock.json"))).hexdigest()[:20]
    classes = ROOT / "var/check-runtimes" / digest
    exports = ["--add-exports", "java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
               "--add-exports", "java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED"]
    agent = classes / "capture-agent.jar"
    with ingest.lock(ROOT / "var/check-runtime.lock"):
        if not agent.exists():
            classes.mkdir(parents=True, exist_ok=True)
            subprocess.run([str(java / "javac"), *exports, "-cp", cp, "-d", str(classes),
                str(ROOT / "scripts/ReadOnlyIndexCheck.java"), str(ROOT / "scripts/CaptureAgent.java")], check=True, timeout=120)
            manifest = classes / "capture-manifest.mf"
            manifest.write_text("Premain-Class: CaptureAgent\n\n")
            temp_agent = classes / "capture-agent.tmp"
            subprocess.run([str(java / "jar"), "cfm", str(temp_agent), str(manifest), "-C", str(classes), "."], check=True, timeout=120)
            os.replace(temp_agent, agent)
    return java, exports, agent, classes, cp


def require_capture(logfile):
    import re
    text = logfile.read_text()
    capture = re.search(r"CAPTURE_SUMMARY installed=true calls=(\d+) failures=0 reserved=0", text)
    if not capture or "CAPTURE_AGENT_TRANSFORMED" not in text or "INTEGRITY_OK" not in text:
        raise RuntimeError("Integrity capture instrumentation did not complete")
    if int(capture.group(1)) == 0 and "numSegments=0" not in text:
        raise RuntimeError("No checks were instrumented for a nonempty index")


def committed_fingerprint(path):
    """Conservative identity: extra files invalidate reuse rather than being ignored."""
    result = {}
    for file in sorted(path.iterdir()):
        if not file.is_file() or file.name == "write.lock":
            continue
        before = file.stat()
        digest = hashlib.sha256()
        with file.open("rb") as stream:
            for block in iter(lambda: stream.read(8*1024**2), b""):
                digest.update(block)
        after = file.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
            raise RuntimeError("Index changed while fingerprinting")
        result[file.name] = {"bytes":after.st_size,"sha256":digest.hexdigest()}
    if not any(name.startswith("segments_") for name in result):
        raise RuntimeError("Index has no committed segments file")
    return result


class IndexIntegrityFailure(RuntimeError):
    pass


def check_indexes(g, final=False):
    """Close only inactive generation indexes; no -exorcise or file deletion."""
    if (ROOT / "var/active.json").exists() and json.loads(
        (ROOT / "var/active.json").read_text()
    )["generation"] == g:
        raise RuntimeError("cannot close active generation")
    report_name = "final-index-check" if final else "offline-index-check"
    with ingest.lock(generation_path(g) / "import.lock"):
        cursor_path = generation_path(g) / (report_name + "-cursor.json")
        cursor = json.loads(cursor_path.read_text()) if cursor_path.exists() else {}
        if cursor.get("signature") != recovery.signature(g):
            cursor = {"signature":recovery.signature(g), "shards":{}}
        java, exports, agent, classes, cp = prepare_index_checker()
        b = Backend(writer=True)
        checked = []
        try:
            if final:
                flushed = b.os("POST", "/" + index_expression(g) + "/_flush?wait_if_ongoing=true", {})
                if flushed["_shards"]["failed"]:
                    raise RuntimeError("Final index flush failed")
            indexes = b.os(
                "GET",
                "/_cat/indices/" + index_expression(g) + "?format=json&h=index,uuid",
            )
            for item in indexes:
                name = item["index"]
                closed = b.os("POST", "/" + name + "/_close", {})
                try:
                    if not closed.get("acknowledged") or not closed.get(
                        "shards_acknowledged"
                    ):
                        raise RuntimeError("Index close was not acknowledged: " + name)
                    assert_index_closed(b, name)
                    locations = sorted(
                        (
                            ROOT / "var/data/opensearch/nodes/0/indices" / item["uuid"]
                        ).glob("*/index")
                    )
                    if not locations:
                        raise RuntimeError(
                            "Index has no physical shard directories: " + name
                        )
                    for path in locations:
                        fingerprint = committed_fingerprint(path)
                        key = str(path)
                        if cursor["shards"].get(key, {}).get("files") == fingerprint:
                            checked.append(key)
                            LOG.info("Reused verified unchanged shard %s", path)
                            continue
                        logfile = (
                            ROOT
                            / "reports"
                            / ("check-" + name + "-" + path.parent.name + ".log")
                        )
                        ingest.guard_capacity(config(), 0, 0)
                        scratch = ROOT / "var/index-check-scratch"
                        scratch.mkdir(exist_ok=True)
                        LOG.info("Checking committed copy of %s", path)
                        with (
                            index_check_copy(scratch, path, logfile) as tmp,
                            logfile.open("w") as log,
                        ):
                            result = subprocess.run(
                                [
                                    str(java / "java"),
                                    "-Xmx8g",
                                    "-XX:ActiveProcessorCount=4",
                                    *exports,
                                    "-javaagent:" + str(agent),
                                    "-Doa.capture.dir=" + str(tmp / "checksum-evidence"),
                                    "-cp",
                                    str(classes) + ":" + cp,
                                    "ReadOnlyIndexCheck",
                                    str(path),
                                    str(tmp / "index"),
                                ],
                                stdout=log,
                                stderr=subprocess.STDOUT,
                                check=False,
                                timeout=config().get("index_check_timeout", 1800),
                            )
                            log.flush()
                            if result.returncode:
                                text = logfile.read_text()
                                if any(term in text for term in ("CorruptIndexException", "checksum failed", "Index failed integrity check", "EOFException")):
                                    raise IndexIntegrityFailure("Physical index integrity failure: " + str(logfile))
                                raise RuntimeError("Index checker runtime failed: " + str(logfile))
                            require_capture(logfile)
                            assert_index_closed(b, name)
                        if committed_fingerprint(path) != fingerprint:
                            raise RuntimeError("Index changed during integrity check")
                        cursor["shards"][key] = {"files":fingerprint,"checked":time.time(),"log":str(logfile)}
                        atomic_json(cursor_path,cursor)
                        checked.append(str(path))
                        LOG.info("Integrity checked %s", path)
                finally:
                    b.os("POST", "/" + name + "/_open", {})
            return recovery.evidence(
                g,
                report_name,
                passed=True,
                shards=checked,
                method="closed-commit-copy",
            )
        except Exception as ex:
            recovery.evidence(
                g, report_name, passed=False, error=str(ex), checked=checked
            )
            raise
        finally:
            b.close()


def canary_config():
    base = ROOT / "var/storage-canary"
    conf = base / "config"
    conf.mkdir(parents=True, exist_ok=True)
    (base / "data").mkdir(exist_ok=True)
    (base / "logs").mkdir(exist_ok=True)
    origin = ROOT / "var/engines/opensearch-3.3.2/config"
    for f in ("jvm.options", "log4j2.properties"):
        shutil.copy2(origin / f, conf / f)
    (conf / "opensearch.yml").write_text(f"""cluster.name: openalex-storage-canary
node.name: openalex-storage-canary
network.host: 127.0.0.1
http.port: 19201
transport.port: 19301
discovery.type: single-node
path.data: {base}/data
path.logs: {base}/logs
plugins.security.disabled: true
node.processors: 4
node.store.allow_mmap: false
""")
    return conf


def verify(g, minimum_records=20000000, minimum_seconds=86400):
    from . import manage
    with ingest.lock(ROOT / "var/storage-canary.controller.lock"):
        owner = ROOT / "var/storage-canary/owner.json"
        if manage.identity(manage.state_path("canary")) and (not owner.exists() or json.loads(owner.read_text()).get("generation") != g):
            raise RuntimeError("Canary belongs to another run")
        try:
            return _verify(g, minimum_records, minimum_seconds)
        finally:
            manage.stop("canary")


def _verify(g, minimum_records=20000000, minimum_seconds=86400):
    from . import manage

    if minimum_records < 20000000 or minimum_seconds < 86400:
        raise ValueError(
            "production storage gate requires >=20 million records and >=24 hours"
        )
    recovery.require_evidence(g, "metadata-complete")
    # Source metadata is frozen for the entire canary audit.
    with ingest.lock(generation_path(g) / "storage-check.lock"):
        manage.start("canary")
        b = Backend(writer=True)
        b.cfg = dict(b.cfg, os_url="http://127.0.0.1:19201")
        cg = "storage_" + g
        p = generation_path(g) / "storage-progress.json"
        state = json.loads(p.read_text()) if p.exists() else {}
        if state.get("signature") != recovery.signature(g):
            if state:
                raise RuntimeError(
                    "Storage evidence configuration changed; archive prior canary and create a new verification run"
                )
            state = dict(
                signature=recovery.signature(g),
                started=time.time(),
                last_id="",
                unique=0,
                updates=0,
                phase="initial",
                cycles=0,
            )
        name = index(cg, "works")
        if b.http.head(b.cfg["os_url"] + "/" + name).status_code == 404:
            m = mapping(4)
            m["settings"].update(
                {
                    "index.store.type": "niofs",
                    "index.translog.durability": "request",
                    "index.merge.scheduler.max_thread_count": 1,
                }
            )
            b.os("PUT", "/" + name, m)
        else:
            actual = b.os("GET", "/" + name + "/_settings")[name]["settings"]["index"]
            if actual.get("store", {}).get("type") != "niofs":
                raise RuntimeError("Canary storage mode mismatch")
        atomic_json(ROOT / "var/storage-canary/owner.json", {"generation": g})
        if state["unique"] >= minimum_records and state["phase"] == "initial":
            state["initial_last"] = state["last_id"]
            state["phase"] = "updates"
            state["last_id"] = ""
        state.setdefault("active_seconds", 0)
        try:
            while state["unique"] < minimum_records or state["active_seconds"] < minimum_seconds:
                batch_started = time.monotonic()
                ingest.guard_capacity(config(), 0, 0)
                upper = (
                    " AND id<=" + sql_string(state["initial_last"])
                    if state["phase"] == "updates"
                    else ""
                )
                rows = b.ch(
                    f"SELECT id,raw,version FROM {database(g)}.entities FINAL WHERE entity='works' AND id>{sql_string(state['last_id'])} AND cityHash64(id)%4=0{upper} ORDER BY id LIMIT 4000 FORMAT JSONEachRow"
                )
                if not rows:
                    if state["unique"] < minimum_records:
                        raise RuntimeError(
                            "Not enough distinct real works for required storage test"
                        )
                    state["last_id"] = ""
                    state["phase"] = "updates"
                    state["cycles"] += 1
                    continue
                records = [
                    (
                        r["id"],
                        int(r["version"]),
                        project("works", orjson.loads(r["raw"])),
                    )
                    for r in rows
                ]
                with ThreadPoolExecutor(max_workers=4) as pool:
                    jobs = [
                        pool.submit(b.bulk, cg, "works", records[i : i + 1000])
                        for i in range(0, len(records), 1000)
                    ]
                    for job in jobs:
                        job.result()
                actual = b.mget(cg, "works", [r["id"] for r in rows])
                if any(
                    not d.get("found") or int(d["_version"]) != v
                    for d, (_, v, _) in zip(actual, records)
                ):
                    raise RuntimeError("Canary acknowledged-write verification failed")
                if state["phase"] == "initial":
                    state["unique"] += len(rows)
                else:
                    state["updates"] += len(rows)
                state["last_id"] = rows[-1]["id"]
                state["updated"] = time.time()
                state["active_seconds"] += time.monotonic() - batch_started
                atomic_json(p, state)
                if state["unique"] >= minimum_records and state["phase"] == "initial":
                    state["initial_last"] = state["last_id"]
                    state["phase"] = "updates"
                    state["last_id"] = ""
                    atomic_json(p, state)
            b.os("POST", "/" + name + "/_flush?wait_if_ongoing=true")
            # Process-group crash is confined to the canary, never the serving instance.
            proc = manage.identity(manage.state_path("canary"), child=True)
            if not proc:
                raise RuntimeError("Canary process disappeared")
            os.killpg(proc.pid, __import__("signal").SIGKILL)
            time.sleep(3)
            manage.stop("canary")
            manage.start("canary")
            deadline = time.monotonic() + 600
            while b.os("GET", "/_cluster/health/" + name)["status"] != "green":
                if time.monotonic() > deadline:
                    raise RuntimeError("Canary crash recovery failed")
                time.sleep(5)
            # Audit every acknowledged initial ID against the same source range predicate.
            after = ""
            n = 0
            while n < state["unique"]:
                rows = b.ch(
                    f"SELECT id,version FROM {database(g)}.entities FINAL WHERE entity='works' AND id>{sql_string(after)} AND cityHash64(id)%4=0 ORDER BY id LIMIT 1000 FORMAT JSONEachRow"
                )
                if not rows:
                    raise RuntimeError("Canary source records missing")
                docs = b.mget(cg, "works", [r["id"] for r in rows])
                if any(
                    not d.get("found") or int(d["_version"]) != int(r["version"])
                    for d, r in zip(docs, rows)
                ):
                    raise RuntimeError("Canary lost acknowledged records after crash")
                n += len(rows)
                after = rows[-1]["id"]
            # Full physical check after a clean stop.
            info = b.os("GET", "/_cat/indices/" + name + "?format=json&h=uuid")[0]
            manage.stop("canary")
            java, exports, agent, classes, cp = prepare_index_checker()
            for d in sorted(
                (ROOT / "var/storage-canary/data/nodes/0/indices" / info["uuid"]).glob(
                    "*/index"
                )
            ):
                with (
                    ROOT / "reports" / ("canary-check-" + d.parent.name + ".log")
                ).open("w") as out:
                    subprocess.run(
                        [
                            str(java / "java"),
                            "-Xmx8g",
                            "-XX:ActiveProcessorCount=4",
                            *exports,
                            "-javaagent:" + str(agent),
                            "-Doa.capture.dir=" + str(ROOT / "var/storage-canary/checksum-evidence"),
                            "-cp",
                            str(classes) + ":" + cp,
                            "ReadOnlyIndexCheck",
                            str(d),
                        ],
                        stdout=out,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=config().get("index_check_timeout", 1800),
                    )
                    out.flush()
                    require_capture(Path(out.name))
            assert_storage_logs_clean(ROOT / "var/storage-canary/logs")
            return recovery.evidence(
                g,
                "storage-verification",
                passed=True,
                mode="niofs",
                unique=state["unique"],
                seconds=state["active_seconds"],
                wall_seconds=time.time() - state["started"],
                crash_recovered=True,
                physical_check=True,
            )
        except Exception as ex:
            diagnostic_path = diagnostics.incident("storage-verification", {"generation":g,"error":repr(ex)}, stacks=True)
            recovery.evidence(g, "storage-verification", passed=False, error=str(ex), diagnostic_path=diagnostic_path)
            raise
        finally:
            b.close()
            manage.stop("canary")


def assert_storage_logs_clean(directory):
    # A day-long soak can produce large logs. Stream plain and rotated gzip logs.
    terms = ("read past eof", "corruptindexexception", "checksum failed", "failed engine")
    for path in sorted(directory.glob("*.log*")):
        if not path.is_file():
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                if any(term in line.lower() for term in terms):
                    raise RuntimeError(f"Canary engine log contains storage failures: {path.name}:{line_number}")


def promote_profile(g):
    from . import manage

    recovery.require_evidence(g, "storage-verification")
    p = ROOT / "var/storage-profile.json"
    if p.exists() and json.loads(p.read_text()).get("allow_mmap") is False:
        return
    atomic_json(p, {"allow_mmap": False, "verified_for": g})
    manage.stop("opensearch")
    manage.start("opensearch")
    b = Backend(writer=True)
    try:
        nodes = b.os("GET", "/_nodes/settings")
        if not all(
            str(v["settings"]["node"]["store"]["allow_mmap"]).lower() == "false"
            for v in nodes["nodes"].values()
        ):
            raise RuntimeError("Production mmap mode did not change")
        b.os(
            "PUT",
            "/" + index_expression(g) + "/_settings",
            {
                "index.merge.scheduler.max_thread_count": 1,
                "index.translog.durability": "request",
            },
        )
    finally:
        b.close()
