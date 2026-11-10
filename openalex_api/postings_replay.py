"""Matched default/C1 replay for the unresolved Lucene postings failure.

This is diagnostic evidence only.  It deliberately never writes any formal
storage-verification receipt and never starts the production build workflow.
"""

from __future__ import annotations

from .config import CODE_ROOT

import gzip
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import orjson
import psutil

from . import diagnostics, ingest, resources
from .backend import Backend, database, sql_string
from .config import ROOT, atomic_json, generation_path
from .continuous_storage import compare_reverse, source_shards
from .storage_finish import require_records
from .storage_repair import check_one, commit_manifest

PROFILE = "postings-v1"
BASE = ROOT / "reports/postings-replay-20260916"
SOURCE = ROOT / "reports/merge-debug-20260914/live-v2/live-default"
EXTENSION = SOURCE.parent / "extension/progress.json"
AGENT = ROOT / "reports/postings-debug-20260916/postings-capture.jar"
AGENT_RECEIPT = ROOT / "reports/postings-debug-20260916/runtime-options.json"
SOURCE_INDEX_UUID = "rsrBsssfQt2UQJo_uZ0KHw"
SOURCE_GENERATION = "full_20260908"
SOURCE_INDEX_NAME = "oa-storage-full-20260908-works"
INHERITED_COUNT = 5_304_000
INHERITED_UPPER = "W197464636"
PRESSURE_COUNT = 2_028_000
BATCH = 4_000
ARMS = {
    "default": {"http": 19211, "transport": 19311, "flags": []},
    "c1": {"http": 19212, "transport": 19312, "flags": ["-XX:TieredStopAtLevel=1"]},
}
TRIALS = (
    {"number": 1, "kind": "exact"},
    {"number": 2, "kind": "exact"},
    {"number": 3, "kind": "pressure"},
)
REQUIRED_MARKERS = (
    "POSTINGS_CAPTURE_INSTALLED",
    "POSTINGS_CAPTURE_TRANSFORMED org/apache/lucene/codecs/FieldsConsumer",
    "POSTINGS_CAPTURE_TRANSFORMED org/apache/lucene/codecs/DocValuesConsumer",
    "POSTINGS_CAPTURE_TRANSFORMED org/apache/lucene/codecs/lucene103/Lucene103PostingsReader",
    "CAPTURE_AGENT_TRANSFORMED checksumEntireFile",
)

stop_requested = False
LOG_TERMS = (
    "read past eof",
    "corruptindexexception",
    "checksum failed",
    "failed engine",
    "arrayindexoutofboundsexception",
    "array_index_out_of_bounds_exception",
    "mergeexception",
    "unrecoverable error",
)


def scan_logs(paths: list[Path], cursor: dict) -> int:
    """Scan only newly appended bytes while handling rotation and gzip once."""
    scanned = 0
    files = []
    for path in paths:
        if path.is_dir():
            files.extend(p for p in path.iterdir() if p.is_file() and (".log" in p.name or p.suffix == ".json"))
        elif path.exists():
            files.append(path)
    for path in sorted(set(files)):
        stat = path.stat()
        key = str(path)
        old = cursor.get(key, {})
        compressed = path.suffix == ".gz"
        unchanged_compressed = compressed and old.get("size") == stat.st_size and old.get("mtime_ns") == stat.st_mtime_ns
        if unchanged_compressed:
            continue
        offset = 0
        if not compressed and old.get("inode") == stat.st_ino and old.get("offset", 0) <= stat.st_size:
            offset = old.get("offset", 0)
        opener = gzip.open if compressed else open
        with opener(path, "rt", errors="replace") as stream:
            if offset:
                stream.seek(offset)
            for line_number, line in enumerate(stream, 1):
                scanned += len(line)
                if any(term in line.lower() for term in LOG_TERMS):
                    raise RuntimeError(f"replay engine log contains storage failure: {path.name}:{line_number}")
            final_offset = stream.tell()
        cursor[key] = {
            "inode": stat.st_ino,
            "offset": final_offset,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }
    return scanned


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            value.update(block)
    return value.hexdigest()


def replay_root(g: str) -> Path:
    return BASE / g


def progress_path(g: str) -> Path:
    return replay_root(g) / "progress.json"


def arm_root(g: str, trial: int, arm: str) -> Path:
    return replay_root(g) / f"trial-{trial}" / arm


def process_path(g: str, trial: int, arm: str) -> Path:
    return arm_root(g, trial, arm) / "process.json"


def read_process(path: Path):
    try:
        saved = json.loads(path.read_text())
        proc = psutil.Process(saved["pid"])
        if abs(proc.create_time() - saved["create_time"]) > 0.01:
            return None
        if proc.status() == psutil.STATUS_ZOMBIE:
            return None
        return proc
    except (OSError, ValueError, KeyError, psutil.Error):
        return None


def validate_agent() -> dict:
    receipt = json.loads(AGENT_RECEIPT.read_text())
    if not receipt.get("diagnostic_only") or receipt.get("production_gate_passed"):
        raise RuntimeError("postings agent receipt is not diagnostic-only")
    if not receipt.get("replace_previous_merge_agent"):
        raise RuntimeError("postings agent must replace the old merge agent")
    if digest(AGENT) != receipt["jar_sha256"]:
        raise RuntimeError("tested postings capture agent changed")
    engine_lock = ROOT / "config/engines.lock.json"
    jdk_release = ROOT / "var/engines/opensearch-3.3.2/jdk/release"
    if digest(engine_lock) != receipt["engine_lock_sha256"]:
        raise RuntimeError("engine lock differs from the tested postings runtime")
    if digest(jdk_release) != receipt["jdk_release_sha256"]:
        raise RuntimeError("bundled JDK differs from the tested postings runtime")
    for name, expected in receipt.get("sources", {}).items():
        if digest(CODE_ROOT / "scripts" / name) != expected:
            raise RuntimeError(f"tested postings agent source changed: {name}")
    qualification = Path(receipt.get("qualification_v2", ""))
    if not qualification.is_absolute():
        qualification = AGENT_RECEIPT.parent / qualification
    qualified = json.loads(qualification.read_text()) if qualification.is_file() else {}
    if not qualified.get("passed") or qualified.get("jar_sha256") != receipt["jar_sha256"]:
        raise RuntimeError("postings agent v2 qualification is missing or mismatched")
    if not receipt.get("write_probe_required"):
        raise RuntimeError("postings agent receipt lacks the runtime write probe gate")
    return receipt


def require_write_probes(logs: Path, pid: int) -> None:
    for directory, label in (("checksum-capture", "checksum"), ("merge-context", "merge")):
        path = logs / directory / "agent-write-probe.txt"
        expected = f"pid={pid} label={label}"
        if not path.is_file() or expected not in path.read_text():
            raise RuntimeError(f"capture agent write probe missing for {label}")


def source_evidence() -> dict:
    extension = json.loads(EXTENSION.read_text())
    if not (
        extension.get("state") == "complete"
        and extension.get("total") == PRESSURE_COUNT
        and extension.get("last_id") == INHERITED_UPPER
        and extension.get("physical_passed")
    ):
        raise RuntimeError("pristine source extension evidence is incomplete")
    expected = {
        f"{arm}-{shard}" for arm in ARMS for shard in range(4)
    }
    if set(extension.get("physical", {})) != expected:
        raise RuntimeError("pristine source lacks paired physical manifests")
    for arm in ARMS:
        recovery_names = {
            item[0]
            for item in extension.get("arms", {}).get(arm, {}).get("startup_observation", {}).get("recovery", [])
            if item
        }
        if SOURCE_INDEX_NAME not in recovery_names:
            raise RuntimeError("frozen source index name is absent from extension evidence")
    for shard in source_shards(SOURCE, SOURCE_INDEX_UUID):
        key = "default-" + shard.parent.name
        if commit_manifest(shard) != extension["physical"][key]:
            raise RuntimeError("pristine source changed after physical validation")
    return extension


def phase_for_trial(number: int, kind: str) -> str:
    return f"trial-{number}-{kind}"


def new_state(g: str) -> dict:
    return {
        "version": 1,
        "profile": PROFILE,
        "generation": g,
        "state": "initializing",
        "phase": "preparing",
        "trial": 1,
        "kind": "exact",
        "common_after": "",
        "common_checked": 0,
        "trials": {},
        "classification": None,
        "production_gate_passed": False,
        "formal_proofs_unchanged": True,
        "started": time.time(),
        "updated": time.time(),
    }


def public_state(state: dict, path: Path) -> dict:
    trial = state.get("trials", {}).get(str(state.get("trial")), {})
    arms = trial.get("arms", {})
    return {
        "profile": PROFILE,
        "state": state.get("state"),
        "phase": state.get("phase"),
        "trial": state.get("trial"),
        "kind": state.get("kind"),
        "common_checked": state.get("common_checked", 0),
        "common_after": state.get("common_after", ""),
        "expected": state.get("expected"),
        "throughput_records_per_second": state.get("throughput_records_per_second"),
        "throughput_bytes_per_second": state.get("throughput_bytes_per_second"),
        "eta_seconds": state.get("eta_seconds"),
        "copied_bytes": state.get("copied_bytes"),
        "copy_total_bytes": state.get("copy_total_bytes"),
        "elapsed_seconds": time.time() - state.get("started", time.time()),
        "arms": {
            name: {
                key: row.get(key)
                for key in (
                    "state",
                    "pid",
                    "confirmed",
                    "merge_task",
                    "merge_state",
                    "capture_count",
                    "error",
                )
                if key in row
            }
            for name, row in arms.items()
        },
        "classification": state.get("classification"),
        "pause_reason": state.get("pause_reason"),
        "error": state.get("error"),
        "updated": state.get("updated"),
        "progress": str(path),
        "production_gate_passed": False,
        "formal_proofs_unchanged": True,
    }


def save_state(g: str, document: dict, **values) -> None:
    document.update(values)
    document["updated"] = time.time()
    path = progress_path(g)
    atomic_json(path, document)
    atomic_json(generation_path(g) / "postings-replay.json", public_state(document, path))


def commit_common(state: dict, rows: list[dict], acknowledgements: dict) -> None:
    """Advance only after the same ordered ID/version batch passed on both arms."""
    if set(acknowledgements) != set(ARMS) or not all(acknowledgements.values()):
        raise RuntimeError("both replay arms must acknowledge the batch")
    if not rows:
        return
    if rows[0]["id"] <= state.get("common_after", ""):
        raise RuntimeError("matched replay rows are not strictly after the cursor")
    ids = [row["id"] for row in rows]
    if ids != sorted(set(ids)):
        raise RuntimeError("matched replay batch is not unique and ordered")
    state["common_after"] = ids[-1]
    state["common_checked"] = state.get("common_checked", 0) + len(rows)


def capture_options(logs: Path) -> list[str]:
    return [
        "--add-modules=jdk.incubator.vector",
        "--add-exports=java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED",
        "-javaagent:" + str(AGENT),
        "-Doa.capture.dir=" + str(logs / "checksum-capture"),
        "-Doa.merge.capture=" + str(logs / "merge-context"),
    ]


def configure_arm(base: Path, label: str, logs: Path | None = None) -> None:
    conf = base / "config"
    logs = logs or (base / "logs")
    conf.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "checksum-capture").mkdir(parents=True, exist_ok=True)
    (logs / "merge-context").mkdir(parents=True, exist_ok=True)
    for name in ("jvm.options", "log4j2.properties"):
        shutil.copy2(SOURCE / "config" / name, conf / name)
    ports = ARMS[label]
    (conf / "opensearch.yml").write_text(
        "cluster.name: oa-postings-replay\n"
        f"node.name: oa-postings-{label}\n"
        "network.host: 127.0.0.1\n"
        f"http.port: {ports['http']}\n"
        f"transport.port: {ports['transport']}\n"
        "discovery.type: single-node\n"
        f"path.data: {base / 'data'}\n"
        f"path.logs: {logs}\n"
        "plugins.security.disabled: true\n"
        "node.processors: 4\n"
        "node.store.allow_mmap: false\n"
        "cluster.routing.allocation.disk.watermark.low: 700gb\n"
        "cluster.routing.allocation.disk.watermark.high: 500gb\n"
        "cluster.routing.allocation.disk.watermark.flood_stage: 300gb\n"
    )
    engine = ROOT / "var/engines/opensearch-3.3.2"
    launcher = base / "start-opensearch"
    launcher.write_text(
        "#!/usr/bin/env bash\n"
        "set -e -o pipefail\n"
        f"export OPENSEARCH_HOME={engine}\n"
        f"source {engine / 'bin/opensearch-env'}\n"
        'if [ -z "$OPENSEARCH_TMPDIR" ]; then OPENSEARCH_TMPDIR=$("$JAVA" "$XSHARE" -cp "$OPENSEARCH_CLASSPATH" org.opensearch.tools.launchers.TempDirectory); fi\n'
        'FINAL_JAVA_OPTS=$(export OPENSEARCH_TMPDIR; "$JAVA" "$XSHARE" -cp "$OPENSEARCH_CLASSPATH" org.opensearch.tools.launchers.JvmOptionsParser "$OPENSEARCH_PATH_CONF")\n'
        'exec "$JAVA" "$XSHARE" $FINAL_JAVA_OPTS $OA_FINAL_JAVA_OPTS '
        '-Dopensearch.path.home="$OPENSEARCH_HOME" '
        '-Dopensearch.path.conf="$OPENSEARCH_PATH_CONF" '
        '-Dopensearch.distribution.type=tar -Dopensearch.bundled_jdk=true '
        '-cp "$OPENSEARCH_CLASSPATH" org.opensearch.bootstrap.OpenSearch <<<"$KEYSTORE_PASSWORD"\n'
    )
    launcher.chmod(0o700)


class Replay:
    def __init__(self, g: str):
        self.g = g
        self.path = progress_path(g)
        self.state = json.loads(self.path.read_text()) if self.path.exists() else new_state(g)
        self.engine = ROOT / "var/engines/opensearch-3.3.2"
        self.backends: dict[str, Backend] = {}
        self.processes: dict[str, psutil.Process | subprocess.Popen] = {}

    def save(self, **values) -> None:
        save_state(self.g, self.state, **values)

    def trial(self) -> dict:
        key = str(self.state["trial"])
        return self.state.setdefault("trials", {}).setdefault(
            key,
            {
                "number": self.state["trial"],
                "kind": self.state["kind"],
                "state": "preparing",
                "copied": {arm: {} for arm in ARMS},
                "arms": {arm: {"state": "stopped", "confirmed": 0} for arm in ARMS},
                "physical": {},
            },
        )

    def guard(self, large: bool = False, additional: int = 0) -> None:
        if stop_requested:
            raise resources.Paused("signal")
        resources.checkpoint(self.g, large=large, additional=additional)
        for arm, proc in self.processes.items():
            alive = proc.poll() is None if isinstance(proc, subprocess.Popen) else proc.is_running()
            if not alive:
                raise RuntimeError(f"{arm} replay engine exited unexpectedly")
            trial = self.trial()
            row = trial["arms"][arm]
            console = Path(row["console"]) if row.get("console") else arm_root(self.g, self.state["trial"], arm)
            logs = Path(row["logs"]) if row.get("logs") else arm_root(self.g, self.state["trial"], arm) / "logs"
            row["last_log_scan_bytes"] = scan_logs(
                [logs, console],
                row.setdefault("log_cursor", {}),
            )

    def migrate_known_startup_failure(self) -> bool:
        if self.state.get("state") != "failed" or self.state.get("common_checked") != 0:
            return False
        if self.state.get("error") != "RuntimeError('c1 replay engine exited unexpectedly')":
            return False
        trial = self.trial()
        consoles = [Path(row.get("console", "")) for row in trial.get("arms", {}).values()]
        if not consoles or not all(
            path.is_file() and "NoClassDefFoundError: org/apache/lucene/store/IndexInput" in path.read_text(errors="replace")
            for path in consoles
        ):
            return False
        receipt = replay_root(self.g) / "startup-launcher-migration-v1.json"
        if receipt.exists():
            raise RuntimeError("known startup launcher failure already migrated once")
        archived = replay_root(self.g) / "startup-launcher-failure-v1.json"
        shutil.copy2(self.path, archived)
        atomic_json(
            receipt,
            {
                "reason": "javaagent-was-applied-to-jvm-ergonomics-helper-without-lucene-classpath",
                "archived_state": str(archived),
                "common_checked": 0,
                "copies_reused": True,
                "time": time.time(),
            },
        )
        trial.setdefault("startup_failures", []).append(
            {
                "error": self.state.get("error"),
                "diagnostic_path": self.state.get("diagnostic_path"),
                "classification": self.state.get("classification"),
                "consoles": [str(path) for path in consoles],
                "archived_state": str(archived),
            }
        )
        trial["arms"] = {arm: {"state": "stopped", "confirmed": 0} for arm in ARMS}
        self.state.update(
            state="running",
            phase="starting",
            error=None,
            classification=None,
            diagnostic_path=None,
            startup_launcher_version=2,
        )
        self.save()
        return True

    def prepare(self) -> None:
        replay_root(self.g).mkdir(parents=True, exist_ok=True)
        validate_agent()
        extension = source_evidence()
        if self.path.exists():
            if self.state.get("profile") != PROFILE:
                raise RuntimeError("diagnostic profile changed")
            if self.state.get("state") == "failed":
                if self.migrate_known_startup_failure():
                    return
                raise RuntimeError("failed replay is immutable; inspect captured evidence")
            return
        source_bytes = sum(p.stat().st_size for p in (SOURCE / "data").rglob("*") if p.is_file())
        self.save(
            state="running",
            phase="copying",
            source_bytes=source_bytes,
            source_extension_sha256=digest(EXTENSION),
            source_index_uuid=SOURCE_INDEX_UUID,
            agent_sha256=digest(AGENT),
            inherited_count=INHERITED_COUNT,
            inherited_upper=INHERITED_UPPER,
            extension_evidence={
                "total": extension["total"],
                "last_id": extension["last_id"],
                "physical_passed": extension["physical_passed"],
            },
        )

    def copy_pair(self) -> None:
        trial = self.trial()
        if trial.get("copy_complete"):
            return
        source_size = self.state["source_bytes"]
        self.state.setdefault("phase_started", self.state.get("started", time.time()))
        self.state["copy_total_bytes"] = source_size * len(ARMS)
        copied_bytes = sum(
            row["stamp"][0]
            for arm_rows in trial["copied"].values()
            for row in arm_rows.values()
        )
        elapsed = max(0.001, time.time() - self.state["phase_started"])
        rate = copied_bytes / elapsed if copied_bytes else None
        self.save(
            phase="copying",
            state="running",
            pause_reason=None,
            copied_bytes=copied_bytes,
            throughput_bytes_per_second=rate,
            eta_seconds=max(0.0, (self.state["copy_total_bytes"] - copied_bytes) / rate)
            if rate
            else None,
        )
        self.guard(large=True, additional=source_size * 2)
        for arm in ARMS:
            base = arm_root(self.g, self.state["trial"], arm)
            copied = trial["copied"][arm]
            for source in sorted((SOURCE / "data").rglob("*")):
                if not source.is_file():
                    continue
                self.guard()
                rel = str(source.relative_to(SOURCE / "data"))
                target = base / "data" / rel
                stamp = [source.stat().st_size, source.stat().st_mtime_ns]
                old = copied.get(rel)
                if old and old.get("stamp") == stamp and target.exists() and digest(target) == old.get("sha256"):
                    continue
                before = digest(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(target.name + ".copying")
                shutil.copy2(source, temporary)
                if digest(temporary) != before or digest(source) != before:
                    raise RuntimeError("source/copy SHA-256 mismatch")
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                temporary.replace(target)
                copied[rel] = {"stamp": stamp, "sha256": before}
                copied_bytes = sum(
                    row["stamp"][0]
                    for arm_rows in trial["copied"].values()
                    for row in arm_rows.values()
                )
                elapsed = max(0.001, time.time() - self.state["phase_started"])
                rate = copied_bytes / elapsed
                self.state.update(
                    copied_bytes=copied_bytes,
                    throughput_bytes_per_second=rate,
                    eta_seconds=max(0.0, (self.state["copy_total_bytes"] - copied_bytes) / rate),
                )
                self.save()
            configure_arm(base, arm)
            for shard in source_shards(base, SOURCE_INDEX_UUID):
                key = "default-" + shard.parent.name
                expected = json.loads(EXTENSION.read_text())["physical"][key]
                if commit_manifest(shard) != expected:
                    raise RuntimeError(f"{arm} copied commit differs from source evidence")
        trial["copy_complete"] = time.time()
        trial["state"] = "copied"
        self.save(phase="starting")

    def start_pair(self) -> None:
        trial = self.trial()
        self.save(state="running", phase="starting", pause_reason=None)
        for arm in ARMS:
            base = arm_root(self.g, self.state["trial"], arm)
            existing = read_process(process_path(self.g, self.state["trial"], arm))
            if existing:
                self.processes[arm] = existing
                continue
            session = uuid.uuid4().hex
            logs = base / "logs" / session
            configure_arm(base, arm, logs)
            console = base / f"console-{session}.log"
            options = [
                "-Xms8g",
                "-Xmx8g",
                "-XX:ActiveProcessorCount=4",
                *ARMS[arm]["flags"],
            ]
            final_options = capture_options(logs)
            with console.open("w") as output:
                proc = subprocess.Popen(
                    [str(base / "start-opensearch")],
                    cwd=ROOT,
                    env=os.environ
                    | {
                        "OPENSEARCH_PATH_CONF": str(base / "config"),
                        "OPENSEARCH_JAVA_HOME": str(self.engine / "jdk"),
                        "OPENSEARCH_JAVA_OPTS": " ".join(options),
                        "OA_FINAL_JAVA_OPTS": " ".join(final_options),
                    },
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            identity = {
                "pid": proc.pid,
                "create_time": psutil.Process(proc.pid).create_time(),
                "trial": self.state["trial"],
                "arm": arm,
                "external_diagnostic": True,
            }
            atomic_json(process_path(self.g, self.state["trial"], arm), identity)
            self.processes[arm] = proc
            trial["arms"][arm].update(
                state="starting",
                pid=proc.pid,
                session=session,
                console=str(console),
                logs=str(logs),
                log_cursor={},
            )
            self.save()
        self.await_pair()

    def await_pair(self) -> None:
        trial = self.trial()
        for arm in ARMS:
            url = f"http://127.0.0.1:{ARMS[arm]['http']}"
            began = time.monotonic()
            deadline = began + 900
            with httpx.Client(timeout=10, trust_env=False) as client:
                while time.monotonic() < deadline:
                    self.guard()
                    try:
                        reply = client.get(url + "/_cluster/health/" + SOURCE_INDEX_NAME)
                        if reply.is_success and reply.json().get("status") == "green":
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(2)
                else:
                    raise RuntimeError(f"{arm} replay engine startup timed out")
                info = client.get(url + "/_cat/indices/" + SOURCE_INDEX_NAME + "?format=json&h=uuid,store.size").json()
                settings = client.get(url + "/" + SOURCE_INDEX_NAME + "/_settings").json()
            if len(info) != 1 or info[0].get("uuid") != SOURCE_INDEX_UUID:
                raise RuntimeError(f"{arm} replay index identity changed")
            settings_text = json.dumps(settings).lower()
            if "niofs" not in settings_text or '"durability": "request"' not in settings_text:
                raise RuntimeError(f"{arm} replay storage settings are not niofs/request")
            console = Path(trial["arms"][arm]["console"])
            text = console.read_text(errors="replace")
            missing = [marker for marker in REQUIRED_MARKERS if marker not in text]
            if missing:
                raise RuntimeError(f"{arm} capture agent markers missing: {missing}")
            require_write_probes(Path(trial["arms"][arm]["logs"]), trial["arms"][arm]["pid"])
            backend = Backend(writer=True)
            backend.cfg = dict(backend.cfg, os_url=url)
            self.backends[arm] = backend
            trial["arms"][arm].update(state="ready", startup_seconds=time.monotonic() - began)
            self.save()

    def row_batches(self, after: str, upper: str | None = None, raw: bool = False):
        """Drain each response before yielding to a slow reconciliation consumer."""
        fields = "id,version,raw" if raw else "id,version"
        bound = f" AND id<={sql_string(upper)}" if upper else ""
        client = next(iter(self.backends.values()), None) or Backend(writer=True)
        owned = not self.backends
        try:
            while True:
                query = (
                    f"SELECT {fields} FROM {database(self.g)}.entities FINAL "
                    f"WHERE entity='works' AND cityHash64(id)%4=0 AND id>{sql_string(after)}{bound} "
                    f"ORDER BY id LIMIT {BATCH} FORMAT JSONEachRow"
                )
                stream = client.ch_stream(query)
                try:
                    batch = list(stream)
                finally:
                    stream.close()
                if not batch:
                    return
                after = batch[-1]["id"]
                yield batch
                if len(batch) < BATCH:
                    return
        finally:
            if owned:
                client.close()

    def mget_index(self, arm: str, keys: list[str]) -> list[dict]:
        result = self.backends[arm].os(
            "POST", "/" + SOURCE_INDEX_NAME + "/_mget?_source=false", {"ids": keys}
        )
        docs = result.get("docs", [])
        if not isinstance(docs, list) or len(docs) != len(keys):
            raise RuntimeError(f"{arm} matched mget response count mismatch")
        for key, doc in zip(keys, docs):
            if not isinstance(doc, dict) or doc.get("_id") != key or "error" in doc:
                raise RuntimeError(f"{arm} matched mget item failure for {key}")
        return docs

    def bulk_index(self, arm: str, records: list[tuple]) -> None:
        payload = []
        for key, version, projection in records:
            payload.extend(
                [
                    orjson.dumps(
                        {
                            "index": {
                                "_index": SOURCE_INDEX_NAME,
                                "_id": key,
                                "version": version,
                                "version_type": "external_gte",
                            }
                        }
                    ),
                    orjson.dumps(projection),
                ]
            )
        encoded = b"\n".join(payload) + b"\n"
        for attempt in range(6):
            result = self.backends[arm].os("POST", "/_bulk", content=encoded)
            items = result.get("items", [])
            if len(items) != len(records):
                raise RuntimeError(f"{arm} bulk response count mismatch")
            retry = False
            for item, record in zip(items, records):
                value = item.get("index", {})
                if value.get("_id") != record[0] or value.get("_index") != SOURCE_INDEX_NAME:
                    raise RuntimeError(f"{arm} bulk response identity mismatch")
                status = value.get("status", 0)
                if status in (200, 201):
                    continue
                if status in (429, 502, 503):
                    retry = True
                    continue
                raise RuntimeError(f"{arm} bulk item failed status={status}")
            if not retry:
                return
            if attempt == 5:
                raise RuntimeError(f"{arm} bulk transient retry exhausted")
            time.sleep(min(2**attempt, 16))

    def finish_acknowledgement(self, rows: list[dict], results: dict) -> None:
        self.guard()
        commit_common(self.state, rows, results)
        trial = self.trial()
        for arm in ARMS:
            trial["arms"][arm]["confirmed"] = self.state["common_checked"]
        elapsed = max(0.001, time.time() - self.state.get("rate_started", time.time()))
        rate = (
            self.state["common_checked"] - self.state.get("rate_base_checked", 0)
        ) / elapsed
        expected = self.state.get("expected")
        self.state.update(
            throughput_records_per_second=rate,
            eta_seconds=max(0.0, (expected - self.state["common_checked"]) / rate)
            if expected and rate
            else None,
        )
        self.save(last_batch_seconds=self.state.pop("_batch_seconds", None))

    def acknowledge(self, rows: list[dict]) -> None:
        keys = [row["id"] for row in rows]
        results = {}
        chunks = [(offset, keys[offset : offset + 1000]) for offset in range(0, len(keys), 1000)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                (arm, offset): pool.submit(self.mget_index, arm, chunk)
                for arm in ARMS
                for offset, chunk in chunks
            }
            for arm in ARMS:
                docs = []
                for offset, chunk in chunks:
                    part = futures[(arm, offset)].result()
                    if len(part) != len(chunk):
                        raise RuntimeError(f"{arm} matched mget chunk length mismatch")
                    docs.extend(part)
                require_records(rows, docs)
                results[arm] = True
        self.finish_acknowledgement(rows, results)

    def open_pits(self) -> dict[str, str]:
        pits = {}
        for arm in ARMS:
            value = self.backends[arm].os(
                "POST", "/" + SOURCE_INDEX_NAME + "/_search/point_in_time?keep_alive=10m"
            )
            if not value.get("pit_id"):
                raise RuntimeError(f"{arm} PIT creation returned no identity")
            pits[arm] = value["pit_id"]
        return pits

    def close_pits(self, pits: dict[str, str]) -> None:
        for arm, pit in pits.items():
            try:
                self.backends[arm].os("DELETE", "/_search/point_in_time", {"pit_id": [pit]})
            except Exception:
                pass

    def search_page(self, arm: str, pit: str, after: str, size: int = BATCH) -> list[dict]:
        body = {
            "size": size,
            "pit": {"id": pit, "keep_alive": "10m"},
            "sort": [{"id": "asc"}],
            "_source": False,
            "version": True,
            "track_total_hits": False,
        }
        if after:
            body["search_after"] = [after]
        try:
            value = self.backends[arm].os("POST", "/_search", body)
        except Exception as error:
            error.backend_context = dict(
                getattr(error, "backend_context", {}), arm=arm,
                after=after, size=size, trial=self.state.get("trial"),
            )
            raise
        if value.get("timed_out") or value.get("_shards", {}).get("failed"):
            raise RuntimeError(f"{arm} PIT page was partial")
        hits = value.get("hits", {}).get("hits")
        if not isinstance(hits, list):
            raise RuntimeError(f"{arm} PIT page has no hit list")
        return hits

    def acknowledge_search(self, rows: list[dict], pits: dict[str, str]) -> None:
        after = self.state["common_after"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                arm: pool.submit(self.search_page, arm, pits[arm], after)
                for arm in ARMS
            }
            results = {}
            for arm, future in futures.items():
                last = compare_reverse(future.result(), rows, after)
                if last != rows[-1]["id"]:
                    raise RuntimeError(f"{arm} PIT page boundary mismatch")
                results[arm] = True
        self.finish_acknowledgement(rows, results)

    def exact(self) -> None:
        self.state["expected"] = INHERITED_COUNT
        phase = phase_for_trial(self.state["trial"], "exact")
        if self.state.get("phase") != phase:
            self.state["phase_started"] = time.time()
        self.state.update(
            rate_started=time.time(),
            rate_base_checked=self.state["common_checked"],
            throughput_records_per_second=None,
            eta_seconds=None,
        )
        self.save(phase=phase, state="running")
        pits = self.open_pits()
        try:
            for rows in self.row_batches(self.state["common_after"], INHERITED_UPPER):
                self.guard()
                began = time.monotonic()
                self.acknowledge_search(rows, pits)
                self.state["_batch_seconds"] = time.monotonic() - began
                if self.state["common_checked"] >= INHERITED_COUNT:
                    break
            with ThreadPoolExecutor(max_workers=2) as pool:
                extra = {
                    arm: pool.submit(self.search_page, arm, pits[arm], self.state["common_after"], 1)
                    for arm in ARMS
                }
                if any(future.result() for future in extra.values()):
                    raise RuntimeError("PIT traversal found documents beyond the source boundary")
        finally:
            self.close_pits(pits)
        if self.state["common_checked"] != INHERITED_COUNT or self.state["common_after"] != INHERITED_UPPER:
            raise RuntimeError("exact replay ended before the inherited boundary")
        self.trial()["state"] = "exact-complete"
        self.save(phase="stopping")

    def pressure_write(self) -> str:
        from .continuous_storage import project_record

        trial = self.trial()
        pressure = trial.setdefault("pressure", {"after": INHERITED_UPPER, "confirmed": 0})
        if self.state.get("phase") != "pressure-writing":
            self.state["phase_started"] = time.time()
        self.state.update(rate_started=time.time(), rate_base_checked=pressure["confirmed"])
        self.save(phase="pressure-writing", expected=PRESSURE_COUNT, common_after=pressure["after"], common_checked=pressure["confirmed"])
        for rows in self.row_batches(pressure["after"], raw=True):
            self.guard()
            chunks = [rows[n : n + 1000] for n in range(0, len(rows), 1000)]
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = []
                for arm in ARMS:
                    for chunk in chunks:
                        records = [(r["id"], int(r["version"]), project_record(orjson.loads(r["raw"]))) for r in chunk]
                        futures.append(pool.submit(self.bulk_index, arm, records))
                for future in futures:
                    future.result()
            self.acknowledge(rows)
            pressure.update(after=self.state["common_after"], confirmed=self.state["common_checked"])
            self.save()
            if pressure["confirmed"] >= PRESSURE_COUNT:
                break
        if pressure["confirmed"] != PRESSURE_COUNT:
            raise RuntimeError("pressure source ended before expected count")
        return pressure["after"]

    def pressure_scan(self, upper: str) -> None:
        self.state.update(
            common_after="",
            common_checked=0,
            expected=INHERITED_COUNT + PRESSURE_COUNT,
            phase_started=time.time(),
            throughput_records_per_second=None,
            eta_seconds=None,
        )
        self.state.update(rate_started=time.time(), rate_base_checked=0)
        self.save(phase="pressure-scanning")
        for arm in ARMS:
            self.backends[arm].os("POST", "/" + SOURCE_INDEX_NAME + "/_refresh")
        pits = self.open_pits()
        try:
            for rows in self.row_batches(self.state["common_after"], upper):
                self.guard()
                self.acknowledge_search(rows, pits)
                if self.state["common_checked"] >= self.state["expected"]:
                    break
            with ThreadPoolExecutor(max_workers=2) as pool:
                extra = {
                    arm: pool.submit(self.search_page, arm, pits[arm], self.state["common_after"], 1)
                    for arm in ARMS
                }
                if any(future.result() for future in extra.values()):
                    raise RuntimeError("pressure PIT traversal found extra documents")
        finally:
            self.close_pits(pits)
        if self.state["common_checked"] != self.state["expected"] or self.state["common_after"] != upper:
            raise RuntimeError("pressure scan ended before the matched boundary")

    def pressure_merge(self) -> None:
        trial = self.trial()
        self.save(phase="pressure-merging")
        for arm in ARMS:
            result = self.backends[arm].os(
                "POST", "/" + SOURCE_INDEX_NAME + "/_forcemerge?max_num_segments=1&wait_for_completion=false"
            )
            task = result.get("task")
            if not task:
                raise RuntimeError(f"{arm} force merge did not return a task")
            trial["arms"][arm].update(merge_task=task, merge_state="running")
            self.save()
        deadline = time.monotonic() + 12 * 3600
        remaining = set(ARMS)
        while remaining and time.monotonic() < deadline:
            self.guard()
            for arm in list(remaining):
                task = self.backends[arm].os("GET", "/_tasks/" + trial["arms"][arm]["merge_task"])
                if task.get("completed"):
                    if "error" in task:
                        raise RuntimeError(f"{arm} force merge task failed")
                    trial["arms"][arm]["merge_state"] = "complete"
                    remaining.remove(arm)
                    self.save()
            if remaining:
                time.sleep(15)
        if remaining:
            raise RuntimeError("paired force merge observation exceeded 12 hours")
        trial["state"] = "pressure-complete"

    def collect_runtime(self, reason: str) -> None:
        trial = self.trial()
        self.save(phase="capturing", failure_reason=reason)
        for arm in ARMS:
            base = arm_root(self.g, self.state["trial"], arm)
            evidence = base / "runtime-evidence"
            evidence.mkdir(parents=True, exist_ok=True)
            backend = self.backends.get(arm)
            if backend:
                for name, endpoint in (
                    ("nodes", "/_nodes?filter_path=nodes.*.jvm,nodes.*.settings,nodes.*.process"),
                    ("shards", "/_cat/shards?format=json&bytes=b"),
                    ("segments", "/_cat/segments?format=json&bytes=b"),
                    ("merges", "/_nodes/stats/indices/merges"),
                    ("hot-threads", "/_nodes/hot_threads"),
                ):
                    try:
                        response = backend.http.get(backend.cfg["os_url"] + endpoint, timeout=30)
                        (evidence / f"{name}.txt").write_text(response.text)
                    except Exception as error:
                        (evidence / f"{name}.error.txt").write_text(repr(error))
            proc = read_process(process_path(self.g, self.state["trial"], arm))
            if proc:
                java = self.engine / "jdk/bin/jcmd"
                for command_name, command in (("vm-info", "VM.info"), ("threads", "Thread.print -l"), ("compiler", "Compiler.queue")):
                    try:
                        output = subprocess.run([str(java), str(proc.pid), *command.split()], capture_output=True, text=True, timeout=60)
                        (evidence / f"jcmd-{command_name}.txt").write_text(output.stdout + output.stderr)
                    except Exception as error:
                        (evidence / f"jcmd-{command_name}.error.txt").write_text(repr(error))
            captures = list((base / "logs").glob("*/checksum-capture/failure-*.json"))
            trial["arms"][arm]["capture_count"] = len(captures)
            self.save()
        self.state["classification"] = classify_trial(self.g, self.state["trial"])
        self.save()

    def stop_pair(self, force: bool = False) -> None:
        trial = self.trial()
        for backend in self.backends.values():
            backend.close()
        self.backends.clear()
        for arm in ARMS:
            row = trial["arms"][arm]
            proc = read_process(process_path(self.g, self.state["trial"], arm))
            if not proc:
                row.setdefault("state", "stopped")
                continue
            row.update(state="stopping", stop_requested=time.time())
            self.save()
            try:
                os.killpg(proc.pid, signal.SIGKILL if force else signal.SIGTERM)
                proc.wait(timeout=30 if force else 600)
            except psutil.TimeoutExpired:
                row["stop_pending"] = True
                self.save()
                if not force:
                    raise RuntimeError(f"{arm} graceful stop remains pending")
            except psutil.NoSuchProcess:
                pass
            row.update(state="stopped", stopped=time.time(), stop_pending=False)
            self.save()
        self.processes.clear()

    def physical(self) -> None:
        trial = self.trial()
        self.save(phase="physical-check")
        for arm in ARMS:
            for shard in source_shards(arm_root(self.g, self.state["trial"], arm), SOURCE_INDEX_UUID):
                key = arm + "-" + shard.parent.name
                if key in trial["physical"]:
                    continue
                logfile = arm_root(self.g, self.state["trial"], arm) / "physical" / f"shard-{shard.parent.name}.log"
                logfile.parent.mkdir(parents=True, exist_ok=True)
                trial["physical"][key] = check_one(self.g, shard, logfile, ARMS[arm]["flags"])
                self.save()
        trial["physical_complete"] = time.time()
        trial["state"] = "complete"
        self.save()

    def next_trial(self) -> bool:
        number = self.state["trial"]
        if number >= len(TRIALS):
            return False
        next_row = TRIALS[number]
        self.state.update(
            trial=next_row["number"],
            kind=next_row["kind"],
            common_after="",
            common_checked=0,
            expected=None,
            phase_started=time.time(),
            throughput_records_per_second=None,
            throughput_bytes_per_second=None,
            eta_seconds=None,
            copied_bytes=0,
            copy_total_bytes=None,
        )
        self.save(phase="copying")
        return True

    def run(self) -> dict:
        self.prepare()
        while True:
            try:
                self.copy_pair()
                self.start_pair()
                if self.state["kind"] == "exact":
                    self.exact()
                else:
                    upper = self.pressure_write()
                    self.pressure_scan(upper)
                    self.pressure_merge()
                self.stop_pair()
                self.physical()
            except BaseException as error:
                if isinstance(error, OSError) and error.errno in (28, 122):
                    error = resources.Paused("disk-pause")
                if isinstance(error, resources.Paused):
                    self.save(state="pausing", pause_reason=str(error))
                    try:
                        self.stop_pair()
                    finally:
                        self.save(state="paused")
                    raise
                try:
                    self.collect_runtime(repr(error))
                except Exception as capture_error:
                    self.state["capture_error"] = repr(capture_error)
                    self.save()
                try:
                    self.stop_pair()
                finally:
                    report = diagnostics.incident(
                        "postings-replay",
                        {
                            "generation": self.g,
                            "trial": self.state["trial"],
                            "error": repr(error),
                            "backend_context": getattr(error, "backend_context", {}),
                            "classification": self.state.get("classification"),
                        },
                        stacks=True,
                    )
                    self.save(state="failed", phase="failed", error=repr(error),
                              backend_context=getattr(error, "backend_context", {}), diagnostic_path=report)
                raise
            if not self.next_trial():
                self.save(
                    state="complete",
                    phase="complete",
                    classification={"kind": "not-reproduced", "trials": len(TRIALS)},
                    production_gate_passed=False,
                    formal_proofs_unchanged=True,
                )
                return public_state(self.state, self.path)


def _capture_classification(meta: dict, captured: bytes | None, disk: bytes | None) -> str:
    if not meta.get("captured") or not meta.get("complete") or captured is None:
        return "capture-incomplete"
    actual_match = re.search(r"actual=([0-9a-fA-F]+)", meta.get("error", ""))
    actual = actual_match.group(1).lower() if actual_match else None
    software = str(meta.get("software_crc32", "")).lower()
    jvm = str(meta.get("jvm_crc32_recomputed", "")).lower()
    stored = str(meta.get("stored_crc32", "")).lower()
    if disk is not None and captured != disk and software == jvm == actual:
        return "transient-read-content"
    if disk is not None and captured == disk and software == jvm == stored and actual and actual != stored:
        return "checksum-execution-inconsistency"
    if disk is not None and captured == disk and software == jvm and stored and software != stored:
        return "runtime-disk-content-corruption"
    return "unclassified"


def _read_vint(stream) -> int:
    value = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise EOFError("truncated vint")
        current = byte[0]
        value |= (current & 0x7F) << shift
        if not current & 0x80:
            return value
        shift += 7
        if shift > 35:
            raise ValueError("invalid vint")


def _read_string(stream) -> str:
    return stream.read(_read_vint(stream)).decode("utf-8")


def compound_slice(cfs: Path, member: str) -> bytes | None:
    """Read one Lucene compound member using its adjacent .cfe table."""
    cfe = cfs.with_suffix(".cfe")
    if not cfs.exists() or not cfe.exists():
        return None
    segment = cfs.stem
    suffix = member[len(segment) :] if member.startswith(segment) else member
    try:
        with cfe.open("rb") as stream:
            stream.read(4)
            _read_string(stream)
            stream.read(4)
            stream.read(16)
            _read_string(stream)
            count = _read_vint(stream)
            for _ in range(count):
                name = _read_string(stream)
                offset = int.from_bytes(stream.read(8), "little", signed=True)
                length = int.from_bytes(stream.read(8), "little", signed=True)
                if name == suffix or name == member:
                    with cfs.open("rb") as data:
                        data.seek(offset)
                        return data.read(length)
    except (OSError, UnicodeError, ValueError, EOFError):
        return None
    return None


def classify_trial(g: str, trial_number: int) -> dict:
    results = []
    for arm in ARMS:
        base = arm_root(g, trial_number, arm)
        paths = list((base / "checksum-capture").glob("failure-*.json"))
        paths.extend((base / "logs").glob("*/checksum-capture/failure-*.json"))
        for meta_path in sorted(paths):
            try:
                meta = json.loads(meta_path.read_text())
                binary = meta_path.with_suffix(".bin")
                captured = binary.read_bytes() if binary.exists() else None
                match = re.search(r'path="([^"]+\.cfs)".*?\[slice=([^\]]+)\]', meta.get("resource", ""))
                disk = compound_slice(Path(match.group(1)), match.group(2)) if match else None
                results.append(
                    {
                        "arm": arm,
                        "metadata": str(meta_path),
                        "kind": _capture_classification(meta, captured, disk),
                        "captured_sha256": hashlib.sha256(captured).hexdigest() if captured is not None else None,
                        "disk_sha256": hashlib.sha256(disk).hexdigest() if disk is not None else None,
                        "software_crc32": meta.get("software_crc32"),
                        "jvm_crc32_recomputed": meta.get("jvm_crc32_recomputed"),
                        "stored_crc32": meta.get("stored_crc32"),
                    }
                )
            except Exception as error:
                results.append({"arm": arm, "metadata": str(meta_path), "kind": "capture-incomplete", "error": repr(error)})
    if not results:
        return {"kind": "capture-incomplete", "reason": "no checksum capture emitted", "captures": []}
    kinds = {row["kind"] for row in results}
    arms = sorted({row["arm"] for row in results})
    return {
        "kind": next(iter(kinds)) if len(kinds) == 1 else "mixed",
        "capture_arms": arms,
        "arm_pattern": "paired" if arms == sorted(ARMS) else "single-arm",
        "hypothesis_only": arms != sorted(ARMS),
        "captures": results,
    }


def force_stop(g: str) -> list[dict]:
    select_run(g)
    results = []
    for trial in TRIALS:
        for arm in ARMS:
            path = process_path(g, trial["number"], arm)
            proc = read_process(path)
            if not proc:
                continue
            receipt = {
                "generation": g,
                "trial": trial["number"],
                "arm": arm,
                "pid": proc.pid,
                "create_time": proc.create_time(),
                "reason": "explicit-force",
                "time": time.time(),
            }
            atomic_json(replay_root(g) / "force-stop" / f"trial-{trial['number']}-{arm}.json", receipt)
            os.killpg(proc.pid, signal.SIGKILL)
            results.append(receipt)
    return results


def select_run(g: str, new_attempt: bool = False):
    global BASE
    pointer = generation_path(g) / "postings-replay.json"
    previous = json.loads(pointer.read_text()).get("progress") if pointer.exists() else None
    if previous:
        path = Path(previous).resolve()
        if not path.is_relative_to(ROOT / "reports") or path.name != "progress.json" or path.parent.name != g:
            raise RuntimeError("invalid saved diagnostic path")
        previous = path
    if new_attempt:
        BASE = ROOT / "reports" / ("postings-replay-" + time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
    elif previous:
        BASE = previous.parent.parent
    return previous


def run(g: str, profile: str = PROFILE, new_attempt: bool = False) -> dict:
    if g != SOURCE_GENERATION:
        raise ValueError("postings-v1 is pinned to the frozen full_20260908 source")
    if profile != PROFILE:
        raise ValueError("unsupported storage diagnostic profile")
    with ingest.lock(ROOT / "var/run" / f"diagnose-storage-{g}.controller.lock"):
        previous = select_run(g, new_attempt)
        if new_attempt:
            replay_root(g).mkdir(parents=True, exist_ok=True)
            atomic_json(replay_root(g) / "previous-attempt.json", dict(
                progress=str(previous) if previous else None,
                sha256=digest(previous) if previous and previous.exists() else None,
                reason="explicit fresh attempt; old evidence and copies retained", time=time.time()))
        return Replay(g).run()


def _signal(signum, frame):
    global stop_requested
    stop_requested = True


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("generation")
    parser.add_argument("--profile", default=PROFILE, choices=[PROFILE])
    parser.add_argument("--new-attempt", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    print(json.dumps(run(args.generation, args.profile, args.new_attempt), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
