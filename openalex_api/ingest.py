"""Streaming whole-shard import with durable checkpoints and replay-safe writes."""

import contextlib, fcntl, gzip, hashlib, json, logging, os, sqlite3, time, math
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import multiprocessing, signal
from pathlib import Path
import orjson, psutil
from .config import (
    ROOT,
    ENTITIES,
    config,
    generation_path,
    generation_name,
    atomic_json,
    read_generation,
)
from .model import project, canonical_id
from .backend import Backend, database, index, index_expression, sql_string

LOG = logging.getLogger(__name__)


def local_lock_path(path):
    # Single-host service: kernel-local locks survive process crashes correctly on this DTFS mount.
    namespace = hashlib.sha256(str(ROOT).encode()).hexdigest()[:16]
    directory = Path("/tmp") / f"openalex-api-{os.getuid()}-{namespace}"
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory / (
        hashlib.sha256(str(Path(path).resolve()).encode()).hexdigest() + ".lock"
    )


@contextlib.contextmanager
def lock(path):
    path = local_lock_path(path)
    with path.open("a+") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another operation holds " + str(path))
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def inventory(snapshot):
    from .snapshot import inventory as inspect_snapshot
    return inspect_snapshot(snapshot)


def select_pilot(items, target):
    chosen = set()
    for e in ENTITIES:
        es = sorted(
            [x for x in items if x["entity"] == e],
            key=lambda x: (x["expected"], x["relative"]),
        )
        if e not in ["works", "authors", "awards"]:
            chosen.update(x["relative"] for x in es)
            continue
        if not es:
            continue
        budget = (
            target if e == "works" else min(target, 100000 if e == "authors" else 20000)
        )
        # Deterministic whole-file size/date strata; no gzip head sampling.
        candidates = []
        bydate = sorted(es, key=lambda x: x["relative"])
        for seq in [es, bydate]:
            for q in [0, 0.25, 0.5, 0.75, 1]:
                candidates.append(seq[min(int(q * (len(seq) - 1)), len(seq) - 1)])
        n = 0
        for item in candidates + bydate:
            if item["relative"] in chosen:
                continue
            chosen.add(item["relative"])
            n += item["expected"]
            if n >= budget:
                break
    return chosen


def connect(g):
    db = sqlite3.connect(generation_path(g) / "tasks.sqlite", timeout=60)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.row_factory = sqlite3.Row
    return db


def create_generation(g, mode="pilot", works=1000000, shards=None, metadata_only=False):
    generation_name(g)
    cfg = config()
    p = generation_path(g)
    if p.exists():
        raise ValueError("generation exists; use import to resume")
    inv = inventory(cfg["snapshot"])
    if any(v["missing"] or v["size_mismatch"] for v in inv["entities"].values()):
        atomic_json(ROOT / "reports/inventory-errors.json", inv)
        raise RuntimeError(
            "Missing or wrong-size source shards; see inventory-errors.json"
        )
    p.mkdir(parents=True)
    selected = (
        select_pilot(inv["files"], works)
        if mode == "pilot"
        else {x["relative"] for x in inv["files"]}
    )
    meta = dict(
        generation=g,
        mode=mode,
        state="created",
        created=time.time(),
        manifest_sha256=inv["manifest_sha256"],
        snapshot=inv["snapshot"],
        expected=inv["entities"],
        shards=shards or {},
        mutable=True,
        validated=False,
        pipeline_version=3 if metadata_only else 1,
        snapshot_complete=inv.get("complete", True),
    )
    atomic_json(p / "generation.json", meta)
    atomic_json(p / "inventory.json", inv)
    with connect(g) as db:
        db.execute("""CREATE TABLE shards (path TEXT PRIMARY KEY,entity TEXT,relative TEXT,ordinal INTEGER,
        expected INTEGER,bytes INTEGER,selected INTEGER,state TEXT DEFAULT 'pending',line INTEGER DEFAULT 0,
        records INTEGER DEFAULT 0,sha256 TEXT,prefix_sha256 TEXT,error TEXT,seconds REAL DEFAULT 0)""")
        db.executemany(
            "INSERT INTO shards(path,entity,relative,ordinal,expected,bytes,selected) VALUES(:path,:entity,:relative,:ordinal,:expected,:bytes,:selected)",
            [x | dict(selected=int(x["relative"] in selected)) for x in inv["files"]],
        )
    if metadata_only:
        with connect(g) as db:
            for name, definition in [("metadata_line", "INTEGER DEFAULT 0"),
                                     ("metadata_state", "TEXT DEFAULT 'pending'"),
                                     ("metadata_prefix_sha256", "TEXT"),
                                     ("legacy_verified", "INTEGER DEFAULT 0")]:
                db.execute(f"ALTER TABLE shards ADD COLUMN {name} {definition}")
    b = Backend(writer=True)
    try:
        b.create(g, shards or {}, search=not metadata_only)
    finally:
        b.close()
    return meta


def flush(b, g, e, rows, metadata_only=False):
    started = time.monotonic()
    LOG.info(
        "batch start generation=%s entity=%s source=%s lines=%s-%s records=%d raw_bytes=%d max_record_bytes=%d",
        g,
        e,
        rows[0][2] if rows else None,
        rows[0][0] if rows else None,
        rows[-1][0] if rows else None,
        len(rows),
        sum(len(r[3]) for r in rows),
        max((len(r[3]) for r in rows), default=0),
    )
    entities = []
    identifiers = []
    edges = []
    audit = []
    search = []
    from . import semantics

    for line_no, ordinal, source, raw, r in rows:
        row = semantics.entity_row(e, line_no, ordinal, source, raw, r)
        entities.append(row)
        audit.append(semantics.audit_row(row))
        identifiers.extend(semantics.identifiers(e, row["id"], row["version"], r))
        if not metadata_only:
            search.append((row["id"], row["version"], project(e, r)))

    def edge_rows():
        if e == "works":
            for line_no, ordinal, source, raw, r in rows:
                yield from semantics.references(
                    canonical_id(r.get("id")), ordinal * 1000000000 + line_no, r
                )

    from .batches import bounded

    # Checkpoint only after every sink acknowledges. Replays are logically idempotent via FINAL / external version.
    b.insert(g, "entities", entities)
    b.insert(g, "identifiers", identifiers)
    edge_count = 0
    for edges in bounded(edge_rows()):
        b.insert(g, "references_out", edges)
        b.insert(g, "references_in", edges)
        edge_count += len(edges)
    b.insert(g, "audit", audit)
    if not metadata_only:
        b.bulk(g, e, search)
    LOG.info(
        "batch acknowledged generation=%s entity=%s records=%d identifiers=%d edges=%d seconds=%.3f",
        g,
        e,
        len(rows),
        len(identifiers),
        edge_count,
        time.monotonic() - started,
    )


def project_usage_bytes():
    # Server metadata is used by capacity(); this bound includes actual project files once at run start.
    total = 0
    seen = set()
    for root, dirs, files in os.walk(ROOT):
        if ".venv" in dirs:
            dirs.remove(".venv")
        for name in files:
            try:
                stat = os.stat(Path(root) / name, follow_symlinks=False)
                identity = (stat.st_dev, stat.st_ino)
                if identity not in seen:
                    seen.add(identity)
                    total += stat.st_size
            except FileNotFoundError:
                pass
    return total


def import_shard(g, shard, baseline, free_start, metadata_only=False):
    cfg = config()
    b = Backend(writer=True)
    start = time.monotonic()
    rows = []
    line_no = 0
    digest = hashlib.sha256()
    p = Path(shard["path"])
    from . import ledger

    db = ledger.WorkerLedger() if ledger._requests is not None else connect(g)
    batch_bytes = 0
    prefix = hashlib.sha256()
    if metadata_only:
        shard = dict(
            shard,
            line=shard["metadata_line"],
            prefix_sha256=shard["metadata_prefix_sha256"],
        )
    checkpoint = shard["line"] if shard.get("prefix_sha256") else 0
    before = p.stat()

    def commit_batch(records, last_line, prefix_digest):
        check_cancel()
        if cfg.get("execution_profile") == "throughput96":
            from .resources import checkpoint as admission_checkpoint

            admission_checkpoint(g)
        guard_capacity(cfg, baseline, free_start)
        flush(b, g, shard["entity"], records, metadata_only)
        t = time.monotonic()
        db.execute(
            "UPDATE shards SET line=?,records=?,prefix_sha256=? WHERE path=?",
            (last_line, last_line, prefix_digest, str(p)),
        )
        if metadata_only:
            db.execute(
                "UPDATE shards SET metadata_line=?,metadata_prefix_sha256=? WHERE path=?",
                (last_line, prefix_digest, str(p)),
            )
        db.commit()
        LOG.info(
            "checkpoint generation=%s source=%s line=%s seconds=%.3f",
            g,
            shard["relative"],
            last_line,
            time.monotonic() - t,
        )

    try:
        check_cancel()
        if p.stat().st_size != shard["bytes"]:
            raise ValueError("source size changed")
        db.execute(
            "UPDATE shards SET state='running',error=NULL WHERE path=?", (str(p),)
        )
        if metadata_only:
            db.execute(
                "UPDATE shards SET metadata_state='running' WHERE path=?", (str(p),)
            )
        db.commit()
        with p.open("rb") as source:
            # Gzip validates CRC and truncation at EOF; compressed digest is computed in a separate bounded read.
            with gzip.open(source, "rb") as f:
                for line_no, raw in enumerate(f, 1):
                    if rows and batch_bytes + len(raw) > cfg.get(
                        "import_batch_bytes", 16 * 1024**2
                    ):
                        commit_batch(rows, line_no - 1, prefix.hexdigest())
                        rows = []
                        batch_bytes = 0
                    prefix.update(raw)
                    if line_no <= checkpoint:
                        if (
                            line_no == checkpoint
                            and prefix.hexdigest() != shard["prefix_sha256"]
                        ):
                            raise ValueError(
                                "source prefix changed since checkpoint; rebuild a generation"
                            )
                        continue
                    from .semantics import parse

                    r = parse(raw)
                    rows.append((line_no, shard["ordinal"], shard["relative"], raw, r))
                    batch_bytes += len(raw)
                    if len(rows) >= cfg.get(
                        "import_batch_rows", cfg["batch_size"]
                    ) or batch_bytes >= cfg.get("import_batch_bytes", 16 * 1024 * 1024):
                        commit_batch(rows, line_no, prefix.hexdigest())
                        rows = []
                        batch_bytes = 0
            if rows:
                commit_batch(rows, line_no, prefix.hexdigest())
            if line_no != shard["expected"]:
                raise ValueError(
                    f"record count mismatch: {line_no} != {shard['expected']}"
                )
            source.seek(0)
            while chunk := source.read(8 * 1024 * 1024):
                digest.update(chunk)
        after = p.stat()
        if (before.st_mtime_ns, before.st_size, before.st_ino) != (
            after.st_mtime_ns,
            after.st_size,
            after.st_ino,
        ):
            raise ValueError("source file changed during import")
        db.execute(
            "UPDATE shards SET state='done',line=?,records=?,sha256=?,prefix_sha256=?,seconds=? WHERE path=?",
            (
                line_no,
                line_no,
                digest.hexdigest(),
                prefix.hexdigest(),
                time.monotonic() - start,
                str(p),
            ),
        )
        if metadata_only:
            db.execute(
                "UPDATE shards SET metadata_line=?,metadata_prefix_sha256=?,metadata_state='done' WHERE path=?",
                (line_no, prefix.hexdigest(), str(p)),
            )
        db.commit()
        LOG.info(
            "completed %s records=%d seconds=%.1f",
            shard["relative"],
            line_no,
            time.monotonic() - start,
        )
    except Exception as ex:
        from .resources import Paused

        if isinstance(ex, (ImportCancelled, Paused)):
            db.execute("UPDATE shards SET state='pending' WHERE path=?", (str(p),))
            if metadata_only:
                db.execute(
                    "UPDATE shards SET metadata_state='pending' WHERE path=?", (str(p),)
                )
            db.commit()
            raise ImportCancelled(str(ex)) from ex
        if metadata_only:
            db.execute(
                "UPDATE shards SET metadata_state='failed' WHERE path=?", (str(p),)
            )
        db.execute(
            "UPDATE shards SET state='failed',error=?,seconds=? WHERE path=?",
            (
                f"line {line_no}: {type(ex).__name__}: {ex}",
                time.monotonic() - start,
                str(p),
            ),
        )
        db.commit()
        from .diagnostics import incident

        incident(
            "import-shard",
            {
                "generation": g,
                "source": shard["relative"],
                "line": line_no,
                "error": repr(ex),
            },
        )
        LOG.exception("failed %s at line %d", shard["relative"], line_no)
        raise
    finally:
        db.close()
        b.close()


_usage_cache = None


def measured_project_usage(interval=300):
    """Shared-volume free-space changes are not attributable to this project."""
    global _usage_cache
    cache = ROOT / "var/run/disk-usage.json"
    if cache.exists():
        measured = json.loads(cache.read_text())
        if time.time() - measured["time"] > 900:
            raise RuntimeError(
                "Project disk monitor is stale; restore monitor before writing"
            )
        return measured["bytes"]
    now = time.monotonic()
    if _usage_cache is None or now - _usage_cache[0] >= interval:
        usage = project_usage_bytes()
        _usage_cache = (time.monotonic(), usage)
    return _usage_cache[1]


def guard_capacity(cfg, baseline, free_start):
    free = psutil.disk_usage(str(ROOT)).free
    if cfg.get("execution_profile") == "throughput96":
        from .resources import decide, Paused
        from .manage import resource_status

        reason = decide(free, resource_status()["total_rss_bytes"])
        if reason:
            raise Paused(reason)
        return
    if free < cfg.get("min_free_gib", 50) * 1024**3:
        raise RuntimeError("filesystem free space below configured reserve")
    usage = measured_project_usage()
    if cfg.get("disk_budget_gib") is not None and usage > cfg["disk_budget_gib"] * 1024**3 * 0.90:
        raise RuntimeError(
            f"90% project disk budget reached: measured project bytes={usage}; expansion required"
        )


class ImportCancelled(RuntimeError):
    pass


_cancel_event = None


def init_worker(event, requests=None, responses=None, parent=None):
    global _cancel_event
    _cancel_event = event
    if requests is not None:
        from .ledger import install

        install(requests, responses, parent)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def check_cancel():
    if _cancel_event is not None and _cancel_event.is_set():
        raise ImportCancelled("paused at safe batch boundary")


def run_import(g):
    p = generation_path(g)
    if (p / "resume-block.json").exists() and not (p / "generation.json").exists():
        raise RuntimeError("Build resume blocked pending storage validation")
    meta = read_generation(g)
    metadata_only = meta.get("pipeline_version") in (2, 3)
    if meta.get("pipeline_version") == 2:
        from .recovery import require_stage

        require_stage(g, "metadata")
    elif (p / "resume-block.json").exists():
        raise RuntimeError(
            "Build resume blocked pending storage validation: "
            + str(p / "resume-block.json")
        )
    with lock(p / "import.lock"):
        meta = read_generation(g)
        if not meta["mutable"] or meta.get("metadata_frozen"):
            raise RuntimeError("metadata generation is immutable")
        cfg = config()
        if inventory(meta["snapshot"])["manifest_sha256"] != meta["manifest_sha256"]:
            raise RuntimeError("snapshot manifests changed")
        b = Backend(writer=True)
        try:
            b.create(g, meta["shards"], search=not metadata_only)
        finally:
            b.close()
        meta.update(state="importing", validated=False)
        atomic_json(p / "generation.json", meta)
        baseline = measured_project_usage()
        free_start = psutil.disk_usage(str(ROOT)).free
        guard_capacity(cfg, baseline, free_start)
        with connect(g) as db:
            progress_column = "metadata_line" if metadata_only else "line"
            initial_records = int(
                db.execute(
                    f"SELECT coalesce(sum({progress_column}),0) FROM shards WHERE selected=1"
                ).fetchone()[0]
            )
            total_records = int(
                db.execute(
                    "SELECT sum(expected) FROM shards WHERE selected=1"
                ).fetchone()[0]
            )
            import_started = time.monotonic()
            column = "metadata_state" if metadata_only else "state"
            pending = [
                dict(r)
                for r in db.execute(
                    f"SELECT * FROM shards WHERE selected=1 AND {column}!='done' ORDER BY bytes"
                )
            ]
        event = multiprocessing.get_context("spawn").Event()
        previous = signal.signal(signal.SIGTERM, lambda *_: event.set())
        first_error = None
        failed = cancelled = completed = 0
        from .resume import Admission

        admission = Admission(cfg["workers"])
        manager = coordinator = requests = responses = None
        if cfg.get("execution_profile") == "throughput96":
            import threading
            from .ledger import coordinate

            manager = multiprocessing.Manager()
            requests, responses = manager.Queue(64), manager.dict()
            coordinator = threading.Thread(
                target=coordinate, args=(g, requests, responses), daemon=True
            )
            coordinator.start()
            admission.workers = min(4, cfg["workers"])
            from .resources import Ramp

            ramp = Ramp(
                [n for n in (4, 8, 16, 24, 32) if n <= cfg["workers"]]
                or [cfg["workers"]]
            )
            last_ramp_records = initial_records
            last_ramp_time = time.monotonic()
            works = [x for x in pending if x["entity"] == "works"]
            other = [x for x in pending if x["entity"] != "works"]
            pending = []
            while works or other:
                pending.extend(works[:3])
                del works[:3]
                pending.extend(other[:1])
                del other[:1]
        remaining = iter(pending)
        try:
            with ProcessPoolExecutor(
                max_workers=cfg["workers"],
                mp_context=multiprocessing.get_context("spawn"),
                initializer=init_worker,
                initargs=(event, requests, responses, os.getpid()),
            ) as pool:
                futures = {}

                def fill():
                    while not event.is_set() and len(futures) < admission.workers:
                        item = next(remaining, None)
                        if item is None:
                            break
                        f = pool.submit(
                            import_shard, g, item, baseline, free_start, metadata_only
                        )
                        futures[f] = item["relative"]

                fill()
                while futures:
                    done, _ = wait(futures, timeout=5, return_when=FIRST_COMPLETED)
                    for future in done:
                        source = futures.pop(future)
                        try:
                            future.result()
                            completed += 1
                        except ImportCancelled:
                            cancelled += 1
                        except Exception as ex:
                            failed += 1
                            first_error = first_error or dict(
                                source=source, error=str(ex)
                            )
                            event.set()
                    if event.is_set():
                        for f in list(futures):
                            if f.cancel():
                                cancelled += 1
                                futures.pop(f)
                    try:
                        sample = json.loads(
                            (ROOT / "var/run/resource-latest.json").read_text()
                        )
                    except (OSError, ValueError):
                        sample = {}
                    if cfg.get("execution_profile") != "throughput96":
                        admission.update(sample, time.time())
                    else:
                        from .resources import checkpoint, Paused

                        try:
                            checkpoint(g)
                        except Paused:
                            event.set()
                    fill()
                    with connect(g) as progress_db:
                        checkpointed = int(
                            progress_db.execute(
                                f"SELECT coalesce(sum({progress_column}),0) FROM shards WHERE selected=1"
                            ).fetchone()[0]
                        )
                    if (
                        cfg.get("execution_profile") == "throughput96"
                        and time.monotonic() - last_ramp_time >= 30
                    ):
                        from .resources import merge_backlog

                        backend = Backend(writer=True)
                        try:
                            sample = __import__(
                                "openalex_api.manage", fromlist=["resource_status"]
                            ).resource_status()
                            admission.workers = ramp.observe(
                                checkpointed - last_ramp_records,
                                sample,
                                merge_backlog(backend, g),
                            )
                        finally:
                            backend.close()
                        last_ramp_records = checkpointed
                        last_ramp_time = time.monotonic()
                    rate = (checkpointed - initial_records) / max(
                        1, time.monotonic() - import_started
                    )
                    atomic_json(
                        p / "import-heartbeat.json",
                        dict(
                            time=time.time(),
                            completed=completed,
                            failed=failed,
                            cancelled=cancelled,
                            pending=len(pending) - completed - failed - cancelled,
                            inflight=list(futures.values()),
                            first_error=first_error,
                            admitted_workers=admission.workers,
                            checkpointed_records=checkpointed,
                            records_per_second=rate,
                            estimated_remaining_seconds=(total_records - checkpointed)
                            / rate
                            if rate > 0
                            else None,
                        ),
                    )
        finally:
            if coordinator is not None:
                requests.put(None)
                coordinator.join()
                manager.shutdown()
            signal.signal(signal.SIGTERM, previous)
        meta.update(
            state="failed"
            if first_error
            else "paused"
            if event.is_set()
            else "metadata_imported"
            if metadata_only
            else "imported"
        )
        atomic_json(p / "generation.json", meta)
        if first_error:
            raise RuntimeError(
                f"{failed} failed, {cancelled} cancelled; first error: {first_error}"
            )
        if event.is_set():
            raise ImportCancelled("import paused")
    return status(g)


def status(g):
    meta = read_generation(g)
    build_state = generation_path(g) / "build.json"
    if build_state.exists():
        meta["build"] = json.loads(build_state.read_text())
    with connect(g) as db:
        state = "metadata_state" if meta.get("pipeline_version") == 2 else "state"
        records = "metadata_line" if meta.get("pipeline_version") == 2 else "records"
        meta["progress"] = [
            dict(r)
            for r in db.execute(
                f"SELECT entity,selected,{state} state,count(*) shards,sum(expected) expected,sum({records}) records FROM shards GROUP BY entity,selected,{state} ORDER BY entity"
            )
        ]
        meta["errors"] = [
            dict(r)
            for r in db.execute(
                f"SELECT relative,error FROM shards WHERE {state}='failed' LIMIT 20"
            )
        ]
    for filename in ("execution-run", "execution-plan", "retired-search"):
        path = generation_path(g) / (filename + ".json")
        if path.exists():
            value = json.loads(path.read_text())
            meta[filename] = {
                k: v
                for k, v in value.items()
                if k not in ("archive", "indexes", "names")
            }
    search = generation_path(g) / "search-apply-cursor.json"
    if search.exists():
        meta["search_progress"] = json.loads(search.read_text()).get("report")
    for name in (
        "import-heartbeat",
        "metadata-audit-cursor",
        "storage-progress",
        "continuous-storage",
        "postings-replay",
        "storage-repair",
        "audit-progress",
        "exact-progress",
        "search-progress",
        "resource-phase",
        "control",
    ):
        path = generation_path(g) / (name + ".json")
        if path.exists():
            value = json.loads(path.read_text())
            meta[name] = {k: v for k, v in value.items() if k != "entities"}
            if "updated" in value and value.get("state") != "complete":
                meta[name]["stalled"] = time.time() - value["updated"] > 600
            if (
                name == "metadata-audit-cursor"
                and (generation_path(g) / "audit-lineage.json").exists()
            ):
                meta[name]["inherited_prefix"] = True
                meta[name].pop("stalled", None)
    if meta.get("control", {}).get("state") == "running":
        latest = max(
            (meta.get(name, {}).get("updated", 0) for name in
             ("audit-progress", "exact-progress", "search-progress", "storage-progress", "storage-repair", "continuous-storage", "postings-replay")),
            default=0,
        )
        latest = max(latest, meta.get("import-heartbeat", {}).get("time", 0))
        if latest:
            meta["control"]["last_progress_at"] = latest
            meta["control"]["stalled"] = time.time() - latest > 600
    if "storage-repair" in meta:
        from .storage_repair import runtime_status
        meta["storage_repair_runtime"] = runtime_status(g)
    if config().get("execution_profile") == "throughput96":
        from .manage import resource_status

        meta["resources"] = resource_status()
        meta["stage_eta_seconds"] = {
            "metadata_audit": meta.get("audit-progress", {}).get("eta_seconds"),
            "metadata_import": meta.get("import-heartbeat", {}).get(
                "estimated_remaining_seconds"
            ),
            "storage_validation_minimum": max(
                0, 86400 - (meta["continuous-storage"].get("continuous_seconds", 0) or 0)
                if "continuous-storage" in meta else 86400 - meta.get("storage-progress", {}).get("active_seconds", 0)
            ),
            "post_publication_observation_minimum": 86400,
        }
    return meta


def validate(g):
    p = generation_path(g)
    with lock(p / "import.lock"):
        meta = read_generation(g)
        s = status(g)
        if any(r["selected"] and r["state"] != "done" for r in s["progress"]):
            raise RuntimeError("selected shards are incomplete")
        b = Backend(writer=True)
        db = database(g)
        report = dict(
            generation=g,
            checked=time.time(),
            entities={},
            errors=[],
            complete=meta["mode"] == "full",
        )
        try:
            b.os("POST", "/" + index_expression(g) + "/_refresh")
            b.os("POST", "/" + index_expression(g) + "/_flush?wait_if_ongoing=true")
            for e in ENTITIES:
                stats = b.ch(
                    f"SELECT count() unique_entities FROM {db}.entities FINAL WHERE entity={sql_string(e)} FORMAT JSONEachRow"
                )[0]
                audit = b.ch(
                    f"SELECT count() input_records FROM {db}.audit FINAL WHERE entity={sql_string(e)} FORMAT JSONEachRow"
                )[0]
                conflicts = b.ch(
                    f"SELECT count() conflicts FROM (SELECT id FROM {db}.audit FINAL WHERE entity={sql_string(e)} GROUP BY id HAVING uniqExact(digest)>1) FORMAT JSONEachRow"
                )[0]
                osn = b.os("GET", "/" + index(g, e) + "/_count")["count"]
                expected = sum(
                    r["expected"]
                    for r in s["progress"]
                    if r["selected"] and r["entity"] == e
                )
                result = {
                    k: int(v) for k, v in (stats | audit | conflicts).items()
                } | dict(
                    indexed_entities=osn,
                    selected_input_records=expected,
                    snapshot_records=meta["expected"][e]["records"],
                )
                report["entities"][e] = result
                if (
                    result["input_records"] != expected
                    or result["unique_entities"] != osn
                    or result["conflicts"]
                ):
                    report["errors"].append(
                        e + ": count mismatch or conflicting records"
                    )
            # Exact anti-joins, both directions; count equality alone cannot validate edge sets.
            report["edges"] = {}
            for left, right in [
                ("references_out", "references_in"),
                ("references_in", "references_out"),
            ]:
                row = b.ch(
                    f"SELECT count() n FROM {db}.{left} FINAL FORMAT JSONEachRow"
                )[0]
                report["edges"][left] = int(row["n"])
                missing = b.ch(
                    f"SELECT count() n FROM (SELECT citing,cited FROM {db}.{left} FINAL) l LEFT ANTI JOIN (SELECT citing,cited FROM {db}.{right} FINAL) r USING (citing,cited) FORMAT JSONEachRow"
                )[0]
                if int(missing["n"]):
                    report["errors"].append(left + ": edge set mismatch")
            if report["complete"] and any(r["selected"] == 0 for r in s["progress"]):
                report["errors"].append("full generation has excluded shards")
            # Deterministic per-entity source equivalence sampling, independent of text projection.
            report["raw_checks"] = {}
            for e in ENTITIES:
                rows = b.ch(
                    f"SELECT raw,digest FROM {db}.entities FINAL WHERE entity={sql_string(e)} AND id IN (SELECT id FROM {db}.entities FINAL WHERE entity={sql_string(e)} ORDER BY cityHash64(id) LIMIT 20) FORMAT JSONEachRow"
                )
                report["raw_checks"][e] = len(rows)
                if any(
                    hashlib.sha256(
                        orjson.dumps(
                            orjson.loads(r["raw"]), option=orjson.OPT_SORT_KEYS
                        )
                    ).hexdigest()
                    != r["digest"]
                    for r in rows
                ):
                    report["errors"].append(e + ": raw record hash mismatch")
        finally:
            b.close()
        atomic_json(p / "validation.json", report)
        if report["errors"]:
            raise RuntimeError("validation failed: " + str(report["errors"]))
        writer = Backend(writer=True)
        try:
            writer.os(
                "PUT",
                "/" + index_expression(g) + "/_settings",
                {"index.blocks.write": True},
            )
        finally:
            writer.close()
        meta.update(
            state="validated",
            validated=True,
            mutable=False,
            complete=report["complete"],
        )
        atomic_json(p / "generation.json", meta)
        return report


def metadata_release(g):
    """Publish only frozen, fully audited metadata; search proofs remain separate."""
    from .recovery import require_evidence

    meta = read_generation(g)
    if meta["mode"] != "full" or not meta.get("metadata_frozen"):
        raise RuntimeError("metadata publication requires a frozen full generation")
    proof = require_evidence(g, "metadata-complete")
    require_evidence(g, "metadata-audit")
    source = inventory(meta["snapshot"])
    if source["manifest_sha256"] != meta["manifest_sha256"] or any(
        row["missing"] or row["size_mismatch"] for row in source["entities"].values()
    ):
        raise RuntimeError("snapshot changed or source files are incomplete")
    with connect(g) as ledger:
        shards = list(ledger.execute("SELECT * FROM shards"))
    if not shards or any(
        not row["selected"] or row["metadata_state"] != "done"
        or row["metadata_line"] != row["expected"] for row in shards
    ):
        raise RuntimeError("metadata checkpoints are incomplete")
    b = Backend(writer=True)
    try:
        counts = {row["entity"]: int(row["n"]) for row in b.ch(
            f"SELECT entity,count() n FROM {database(g)}.entities FINAL GROUP BY entity FORMAT JSONEachRow"
        )}
    finally:
        b.close()
    counts = {e: counts.get(e, 0) for e in ENTITIES}
    if counts != {e: row["records"] for e, row in source["entities"].items()}:
        raise RuntimeError("metadata entity coverage does not match the snapshot")
    report = dict(
        generation=g, time=time.time(), passed=True,
        manifest_sha256=meta["manifest_sha256"],
        metadata_signature=proof["signature"],
        entities={e: dict(unique_entities=counts[e], snapshot_records=row["records"],
                          selected_input_records=row["records"], indexed_entities=None)
                  for e, row in source["entities"].items()},
    )
    atomic_json(generation_path(g) / "metadata-release.json", report)
    return report


def publish(g, partial=False, metadata_only=False):
    with lock(ROOT / "var/run/publish.lock"):
        meta = read_generation(g)
        if metadata_only:
            with lock(generation_path(g) / "import.lock"):
                metadata_release(g)
                meta["metadata_only"] = True
                atomic_json(generation_path(g) / "generation.json", meta)
        else:
            if not meta.get("validated"):
                raise RuntimeError("validate generation before publishing")
            if not meta.get("complete") and not partial:
                raise RuntimeError("partial coverage requires --allow-partial")
            if meta.pop("metadata_only", None):
                atomic_json(generation_path(g) / "generation.json", meta)
        now = time.time()
        active = ROOT / "var/active.json"
        previous = json.loads(active.read_text()) if active.exists() else None
        published = ROOT / "var/generations" / g / "published.json"
        if previous and previous["generation"] == g:
            previous = json.loads(published.read_text()).get("previous") if published.exists() else None
        atomic_json(
            published,
            dict(time=now, previous=previous),
        )
        if previous and previous["generation"] != g:
            atomic_json(
                generation_path(previous["generation"]) / "retired.json", dict(time=now)
            )
        retired = generation_path(g) / "retired.json"
        if retired.exists():
            retired.unlink()
        atomic_json(
            active,
            dict(
                generation=g,
                published=now,
                previous=previous["generation"] if previous else None,
            ),
        )
        return dict(generation=g, complete=meta.get("complete", False),
                    metadata_only=metadata_only, previous=previous)


def capacity(g):
    b = Backend(writer=True)
    meta = read_generation(g)
    db = database(g)
    try:
        tables = b.ch(
            f"SELECT table,sum(bytes_on_disk) bytes,sum(rows) physical_rows FROM system.parts WHERE active AND database={sql_string(db)} GROUP BY table FORMAT JSONEachRow"
        )
        ostats = b.os("GET", "/" + index_expression(g) + "/_stats/store,docs")[
            "indices"
        ]
        segment_stats = b.os("GET", "/" + index_expression(g) + "/_segments")["indices"]
        live_segments = {}
        for name, value in segment_stats.items():
            live_segments[name] = sum(
                segment["size_in_bytes"]
                for copies in value["shards"].values()
                for copy in copies
                if copy["routing"]["primary"]
                for segment in copy["segments"].values()
            )
        parts = b.ch(
            f"SELECT partition,sum(bytes_on_disk) bytes FROM system.parts WHERE active AND database={sql_string(db)} AND table='entities' GROUP BY partition FORMAT JSONEachRow"
        )
        with connect(g) as conn:
            state = "metadata_state" if meta.get("pipeline_version") == 2 else "state"
            records = "metadata_line" if meta.get("pipeline_version") == 2 else "records"
            counts = {
                r["entity"]: r["n"]
                for r in conn.execute(
                    f"SELECT entity,sum({records}) n FROM shards WHERE {state}='done' GROUP BY entity"
                )
            }
        per = {}
        total = 0
        for e in ENTITIES:
            n = counts.get(e, 0)
            osbytes = ostats[index(g, e)]["primaries"]["store"]["size_in_bytes"]
            chbytes = sum(int(r["bytes"]) for r in parts if r["partition"] == e)
            estimate = (
                (osbytes + chbytes) / n * meta["expected"][e]["records"] if n else None
            )
            per[e] = dict(
                imported_records=n,
                clickhouse_entity_bytes=chbytes,
                opensearch_bytes=osbytes,
                opensearch_live_segment_bytes=live_segments[index(g, e)],
                estimated_full_opensearch_bytes=osbytes
                / n
                * meta["expected"][e]["records"]
                if n
                else None,
                recommended_shards=max(
                    1,
                    math.ceil(
                        osbytes
                        / max(1, n)
                        * meta["expected"][e]["records"]
                        / (32 * 1024**3)
                    ),
                    math.ceil(
                        ostats[index(g, e)]["primaries"]["docs"]["count"]
                        / max(1, n)
                        * meta["expected"][e]["records"]
                        / 1000000000
                    ),
                ),
                estimated_full_bytes=estimate,
            )
            if estimate:
                total += estimate
        workn = counts.get("works", 0)
        # Conservative overhead scaling: auxiliary tables dominated by works but also include entity audit/IDs.
        aux_est = sum(
            int(t["bytes"])
            / max(
                1,
                workn if t["table"].startswith("references_") else sum(counts.values()),
            )
            * (
                meta["expected"]["works"]["records"]
                if t["table"].startswith("references_")
                else sum(x["records"] for x in meta["expected"].values())
            )
            for t in tables
            if t["table"] != "entities"
        )
        total += aux_est
        peak = total * 1.8
        update_peak = total * 2.8
        physical_store = sum(v["opensearch_bytes"] for v in per.values())
        retained_bytes = max(0, physical_store - sum(live_segments.values()))
        storage_settled = retained_bytes <= max(100 * 1024**2, physical_store * 0.05)
        result = dict(
            storage_settled=storage_settled,
            retained_opensearch_bytes=retained_bytes,
            generation=g,
            manifest_sha256=meta["manifest_sha256"],
            validated=meta.get("validated", False),
            measured_at=time.time(),
            entities=per,
            tables=tables,
            auxiliary_full_estimate=aux_est,
            estimated_steady_bytes=total,
            estimated_initial_peak_bytes=peak,
            estimated_rebuild_peak_bytes=update_peak,
            budget_bytes=config()["disk_budget_gib"] * 1024**3
            if config()["disk_budget_gib"]
            else None,
            within_initial_budget=peak <= config()["disk_budget_gib"] * 1024**3
            if config()["disk_budget_gib"]
            else None,
            available_bytes=psutil.disk_usage(str(ROOT)).free,
            caveats=[
                "Whole-shard pilot is not yet a statistically representative year/language/type sample.",
                "Only interpret this as a steady estimate when storage_settled is true; retained old segments can inflate physical store after merges.",
                "1.8x initial and 2.8x replacement peaks are explicit planning allowances, not measured maxima.",
            ],
        )
        atomic_json(generation_path(g) / "capacity.json", result)
        return result
    finally:
        b.close()


def cleanup(g, confirmation):
    import shutil

    generation_name(g)
    if confirmation != g:
        raise ValueError("--confirm must equal the generation name")
    with lock(ROOT / "var/run/publish.lock"), lock(generation_path(g) / "import.lock"):
        active_file = ROOT / "var/active.json"
        if (
            active_file.exists()
            and json.loads(active_file.read_text())["generation"] == g
        ):
            raise RuntimeError("cannot delete active generation")
        read_generation(g)
        published = generation_path(g) / "published.json"
        retired = generation_path(g) / "retired.json"
        if published.exists():
            if not retired.exists():
                raise RuntimeError("published generation has no retirement time")
            if (
                time.time() - json.loads(retired.read_text())["time"]
                < config()["cursor_ttl"] + config()["query_timeout"] + 60
            ):
                raise RuntimeError(
                    "wait for all cursors and in-flight queries to expire"
                )
        b = Backend(writer=True)
        try:
            for e in ENTITIES:
                response = b.http.delete(b.cfg["os_url"] + "/" + index(g, e))
                if response.status_code not in [200, 404]:
                    response.raise_for_status()
            b.ch("DROP DATABASE IF EXISTS " + database(g) + " SYNC")
        finally:
            b.close()
        archive = ROOT / "reports/retired" / g
        archive.mkdir(parents=True, exist_ok=True)
        for source in generation_path(g).glob("*.json"):
            shutil.copy2(source, archive / source.name)
        shutil.rmtree(generation_path(g))
        return {"deleted": g, "audit_metadata": str(archive)}
