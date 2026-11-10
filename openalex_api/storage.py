
from .config import CODE_ROOT
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
from .config import ROOT, config, generation_path, atomic_json, search_is_active
from .backend import Backend, database, index, index_expression, sql_string
from .model import mapping, project
from . import ingest, recovery, diagnostics

LOG = logging.getLogger(__name__)


@contextmanager
def probe_source_open(b, g, name):
    """Restore probes may briefly open a retired source; always restore its closed state."""
    from .resume import ensure_retired_index_closed

    rows = b.os("GET", "/_cat/indices/" + name + "?format=json&h=index,status")
    closed = rows == [{"index": name, "status": "close"}]
    journal = generation_path(g) / "restore-probe-open.json"
    if closed:
        atomic_json(journal, dict(index=name, state="opening"))
    try:
        if closed:
            b.os("POST", "/" + name + "/_open", {})
        yield
    finally:
        if closed:
            ensure_retired_index_closed(b, name)
            atomic_json(journal, dict(index=name, state="closed"))


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
        probe_journal = p / "restore-probe-open.json"
        if probe_journal.exists():
            pending = json.loads(probe_journal.read_text())
            if pending["state"] == "opening":
                client = Backend(writer=True)
                try:
                    from .resume import ensure_retired_index_closed

                    ensure_retired_index_closed(client, pending["index"])
                    atomic_json(probe_journal, pending | {"state": "closed"})
                finally:
                    client.close()
        if config().get("execution_profile") == "throughput96":
            from .resources import checkpoint, estimate

            checkpoint(g, large=True, **estimate(g, "archive"))
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
            if source not in snap.get("indices", []):
                retired = p / "retired-search.json"
                original = (
                    json.loads(retired.read_text()).get("names", {}).get("topics")
                    if retired.exists()
                    else None
                )
                if original not in snap.get("indices", []):
                    raise RuntimeError("snapshot topics identity cannot be resolved")
                source = original
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
            with probe_source_open(b, g, source):
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
                snapshot_uuid=snap.get("uuid"),
                restore_index=source,
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
            report["diagnostic_path"] = diagnostics.incident(
                "index-check",
                {"source": str(source), "copy": str(tmp), "error": repr(error)},
                stacks=True,
            )
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
    digest = hashlib.sha256(
        b"".join(
            (ROOT / name).read_bytes()
            for name in (
                "scripts/ReadOnlyIndexCheck.java",
                "scripts/CaptureAgent.java",
                "config/engines.lock.json",
            )
        )
    ).hexdigest()[:20]
    classes = ROOT / "var/check-runtimes" / digest
    exports = [
        "--add-exports",
        "java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
        "--add-exports",
        "java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED",
    ]
    agent = classes / "capture-agent.jar"
    with ingest.lock(ROOT / "var/check-runtime.lock"):
        if not agent.exists():
            classes.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [
                    str(java / "javac"),
                    *exports,
                    "-cp",
                    cp,
                    "-d",
                    str(classes),
                    str(CODE_ROOT / "scripts/ReadOnlyIndexCheck.java"),
                    str(CODE_ROOT / "scripts/CaptureAgent.java"),
                ],
                check=True,
                timeout=120,
            )
            manifest = classes / "capture-manifest.mf"
            manifest.write_text("Premain-Class: CaptureAgent\n\n")
            temp_agent = classes / "capture-agent.tmp"
            subprocess.run(
                [
                    str(java / "jar"),
                    "cfm",
                    str(temp_agent),
                    str(manifest),
                    "-C",
                    str(classes),
                    ".",
                ],
                check=True,
                timeout=120,
            )
            os.replace(temp_agent, agent)
    return java, exports, agent, classes, cp


def require_capture(logfile):
    import re

    text = logfile.read_text()
    capture = re.search(
        r"CAPTURE_SUMMARY installed=true calls=(\d+) failures=0 reserved=0", text
    )
    if (
        not capture
        or "CAPTURE_AGENT_TRANSFORMED" not in text
        or "INTEGRITY_OK" not in text
    ):
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
            for block in iter(lambda: stream.read(8 * 1024**2), b""):
                digest.update(block)
        after = file.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError("Index changed while fingerprinting")
        result[file.name] = {"bytes": after.st_size, "sha256": digest.hexdigest()}
    if not any(name.startswith("segments_") for name in result):
        raise RuntimeError("Index has no committed segments file")
    return result


class IndexIntegrityFailure(RuntimeError):
    pass


def check_indexes(g, final=False):
    """Close only inactive generation indexes; no -exorcise or file deletion."""
    if search_is_active(g):
        raise RuntimeError("cannot close active generation")
    report_name = "final-index-check" if final else "offline-index-check"
    with ingest.lock(generation_path(g) / "import.lock"):
        cursor_path = generation_path(g) / (report_name + "-cursor.json")
        cursor = json.loads(cursor_path.read_text()) if cursor_path.exists() else {}
        if cursor.get("signature") != recovery.signature(g):
            cursor = {"signature": recovery.signature(g), "shards": {}}
        java, exports, agent, classes, cp = prepare_index_checker()
        b = Backend(writer=True)
        checked = []
        try:
            if final:
                flushed = b.os(
                    "POST",
                    "/" + index_expression(g) + "/_flush?wait_if_ongoing=true",
                    {},
                )
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
                        if config().get("execution_profile") == "throughput96":
                            from .resources import checkpoint

                            copy_bytes = sum(
                                f.stat().st_size for f in path.rglob("*") if f.is_file()
                            )
                            checkpoint(g, large=True, temporary=copy_bytes)
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
                                    *runtime_options(),
                                    *exports,
                                    "-javaagent:" + str(agent),
                                    "-Doa.capture.dir="
                                    + str(tmp / "checksum-evidence"),
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
                                if any(
                                    term in text
                                    for term in (
                                        "CorruptIndexException",
                                        "checksum failed",
                                        "Index failed integrity check",
                                        "EOFException",
                                    )
                                ):
                                    raise IndexIntegrityFailure(
                                        "Physical index integrity failure: "
                                        + str(logfile)
                                    )
                                raise RuntimeError(
                                    "Index checker runtime failed: " + str(logfile)
                                )
                            require_capture(logfile)
                            assert_index_closed(b, name)
                        if committed_fingerprint(path) != fingerprint:
                            raise RuntimeError("Index changed during integrity check")
                        cursor["shards"][key] = {
                            "files": fingerprint,
                            "checked": time.time(),
                            "log": str(logfile),
                        }
                        atomic_json(cursor_path, cursor)
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


def runtime_options():
    """Pinned-JDK modes; the chosen mode is part of storage evidence."""
    path = ROOT / "var/storage-runtime.json"
    mode = json.loads(path.read_text()).get("mode", "default") if path.exists() else "default"
    if mode not in ("default", "c1"):
        raise RuntimeError("Unknown storage JVM runtime mode")
    return ["-XX:TieredStopAtLevel=1"] if mode == "c1" else []


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
        if manage.identity(manage.state_path("canary")) and (
            not owner.exists() or json.loads(owner.read_text()).get("generation") != g
        ):
            raise RuntimeError("Canary belongs to another run")
        try:
            return _verify(g, minimum_records, minimum_seconds)
        except __import__("openalex_api.resources", fromlist=["Paused"]).Paused:
            raise
        except OSError as error:
            if error.errno in (28, 122):
                from .resources import Paused
                diagnostics.incident("storage-checkpoint-space", {"generation": g, "error": repr(error)})
                raise Paused("disk-pause") from error
            raise
        except Exception as error:
            p = generation_path(g) / "storage-progress.json"
            try:
                if p.exists():
                    state = json.loads(p.read_text())
                    state.update(failure=repr(error), backend_context=getattr(error, "backend_context", {}), failed_phase=state.get("validation_phase", "writing"), updated=time.time())
                    atomic_json(p, state)
                recovery.evidence(g, "storage-verification", passed=False, error=repr(error), backend_context=getattr(error, "backend_context", {}))
            except Exception:
                LOG.exception("Could not persist original storage failure")
            raise
        finally:
            unwinding = __import__("sys").exc_info()[0] is not None
            try:
                manage.stop("canary")
            except Exception:
                if not unwinding:
                    raise
                LOG.exception("Canary stop remains pending")


def guard_canary_bulk(b, logs):
    """Surface fatal engine logs before treating failed bulk items as transient."""
    original = b.os

    def checked_request(method, path, *args, **kwargs):
        result = original(method, path, *args, **kwargs)
        if path == "/_bulk" and result.get("errors"):
            assert_storage_logs_clean(logs)
        return result

    b.os = checked_request


def _verify(g, minimum_records=20000000, minimum_seconds=86400):
    from . import manage

    if minimum_records < 20000000 or minimum_seconds < 86400:
        raise ValueError(
            "production storage gate requires >=20 million records and >=24 hours"
        )
    recovery.require_evidence(g, "metadata-complete")
    # Failed engine evidence must be archived with its canary before a new run.
    assert_storage_logs_clean(ROOT / "var/storage-canary/logs")
    # Source metadata is frozen for the entire canary audit.
    with ingest.lock(generation_path(g) / "storage-check.lock"):
        from .storage_finish import finish, require_records
        p = generation_path(g) / "storage-progress.json"
        saved = json.loads(p.read_text()) if p.exists() else {}
        if saved.get("failure"):
            raise RuntimeError("Storage attempt failed; preserve it for investigation: " + saved["failure"])
        from .proofs import dependencies
        if saved and saved.get("dependencies") != dependencies(g, "storage-verification"):
            raise RuntimeError("Storage evidence configuration changed; archive prior canary and create a new verification run")
        if saved.get("validation_phase", "writing") not in ("physical", "passed"):
            manage.start("canary")
        b = Backend(writer=True)
        b.cfg = dict(b.cfg, os_url="http://127.0.0.1:19201")
        guard_canary_bulk(b, ROOT / "var/storage-canary/logs")
        cg = "storage_" + g
        if saved.get("validation_phase", "writing") != "writing":
            try:
                return finish(g, saved, b, index(cg, "works"))
            finally:
                b.close()
        p = generation_path(g) / "storage-progress.json"
        state = json.loads(p.read_text()) if p.exists() else {}
        from .proofs import dependencies

        storage_dependencies = dependencies(g, "storage-verification")
        if (state.get("signature") != recovery.signature(g)
                or state.get("dependencies") != storage_dependencies):
            if state:
                raise RuntimeError(
                    "Storage evidence configuration changed; archive prior canary and create a new verification run"
                )
            state = dict(
                signature=recovery.signature(g),
                dependencies=storage_dependencies,
                started=time.time(),
                last_id="",
                unique=0,
                updates=0,
                phase="initial",
                cycles=0,
                validation_phase="writing",
                attempt=__import__("uuid").uuid4().hex,
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
            while (
                state["unique"] < minimum_records
                or state["active_seconds"] < minimum_seconds
            ):
                batch_started = time.monotonic()
                ingest.guard_capacity(config(), 0, 0)
                if config().get("execution_profile") == "throughput96":
                    from .resources import checkpoint

                    checkpoint(g)
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
                require_records(rows, actual)
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
            return finish(g, state, b, name)
        except (__import__("openalex_api.resources", fromlist=["Paused"]).Paused, OSError):
            raise
        except Exception as ex:
            ex.backend_context = dict(getattr(b, "last_mget_context", {}), **getattr(ex, "backend_context", {}))
            diagnostic_path = diagnostics.incident(
                "storage-verification",
                {"generation": g, "error": repr(ex),
                 "backend_context": getattr(ex, "backend_context", {}),
                 "batch": {"attempt": state.get("attempt"), "last_confirmed": state.get("last_id"),
                           "confirmed": state.get("unique")}},
                stacks=True,
            )
            recovery.evidence(
                g,
                "storage-verification",
                passed=False,
                error=str(ex),
                diagnostic_path=diagnostic_path,
            )
            raise
        finally:
            b.close()
            unwinding = __import__("sys").exc_info()[0] is not None
            try:
                manage.stop("canary")
            except Exception:
                if not unwinding:
                    raise
                LOG.exception("Canary stop pending after storage failure")


def assert_storage_logs_clean(directory):
    # A day-long soak can produce large logs. Stream plain and rotated gzip logs.
    terms = (
        "read past eof",
        "corruptindexexception",
        "checksum failed",
        "failed engine",
        "arrayindexoutofboundsexception",
        "array_index_out_of_bounds_exception",
        "mergeexception",
        "unrecoverable error",
    )
    for path in sorted(directory.glob("*.log*")):
        if not path.is_file():
            continue
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", errors="replace") as stream:
            for line_number, line in enumerate(stream, 1):
                if any(term in line.lower() for term in terms):
                    raise RuntimeError(
                        f"Canary engine log contains storage failures: {path.name}:{line_number}"
                    )


class StorageLogMonitor:
    """Scan existing logs once, then only appended bytes; detect rotation/truncation."""
    def __init__(self, directory):
        self.directory = directory
        self.positions = {}

    def check(self):
        terms = ("read past eof", "corruptindexexception", "checksum failed", "failed engine",
                 "arrayindexoutofboundsexception", "array_index_out_of_bounds_exception",
                 "mergeexception", "unrecoverable error", "merge_capture_write_failed")
        for path in sorted(self.directory.glob("*.log*")):
            if not path.is_file():
                continue
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino)
            previous, offset, tail = self.positions.get(str(path), (None, 0, b""))
            if identity != previous or stat.st_size < offset:
                offset, tail = 0, b""
            if path.suffix == ".gz":
                version = (identity,stat.st_size,stat.st_mtime_ns)
                if previous == version:
                    continue
                with gzip.open(path,"rt",errors="replace") as stream:
                    data = stream.read()
                end, identity, tail = stat.st_size, version, b""
            else:
                with path.open("rb") as stream:
                    if tail:
                        stream.seek(offset-len(tail))
                        if stream.read(len(tail)) != tail:
                            offset = 0
                    stream.seek(offset)
                    raw = stream.read()
                    data = raw.decode(errors="replace")
                    cut = raw.rfind(b"\n")
                    raw = raw[:cut+1] if cut>=0 else b""
                    end = offset+len(raw)
                    stream.seek(max(0,end-64))
                    tail = stream.read(min(64,end))
            if any(term in data.lower() for term in terms):
                raise RuntimeError("Canary engine log contains storage failures: " + path.name)
            self.positions[str(path)] = (identity,end,tail)


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
