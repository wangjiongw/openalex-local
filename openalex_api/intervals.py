"""Durable (lower, upper] audit tasks. Only the coordinator writes SQLite."""

import json
import logging
import multiprocessing
import queue
import sqlite3
import threading
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from .config import ENTITIES, atomic_json, config, generation_path
from .backend import Backend, database, sql_string
from .batches import bounded
from . import resources

LOG = logging.getLogger(__name__)


def connect(path):
    db = sqlite3.connect(path, timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=FULL")
    db.execute(
        'CREATE TABLE IF NOT EXISTS tasks(entity TEXT,lo TEXT,hi TEXT,expected INTEGER,last_id TEXT,checked INTEGER DEFAULT 0,state TEXT DEFAULT "pending",PRIMARY KEY(entity,lo))'
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS planning(entity TEXT PRIMARY KEY,last_id TEXT,done INTEGER)"
    )
    return db


def plan(db, b, g, legacy, size=250000, max_ranges=None):
    generated = 0
    for entity in ENTITIES:
        inherited = legacy.get("entities", {}).get(entity, {})
        if inherited.get("done"):
            continue
        saved = db.execute(
            "SELECT * FROM planning WHERE entity=?", (entity,)
        ).fetchone()
        if saved and saved["done"]:
            continue
        lower = saved["last_id"] if saved else inherited.get("last_id", "")
        # Read only IDs; retain at most one range's endpoints, never all IDs.
        while True:
            resources.checkpoint(g)
            last, n = lower, 0
            for row in b.ch_stream(
                f"SELECT id FROM {database(g)}.entities FINAL WHERE entity={sql_string(entity)} AND id>{sql_string(lower)} ORDER BY id LIMIT {int(size)} FORMAT JSONEachRow"
            ):
                if row["id"] <= last:
                    raise RuntimeError("unordered/non-unique audit IDs")
                last, n = row["id"], n + 1
            with db:
                if n:
                    db.execute(
                        "INSERT OR IGNORE INTO tasks(entity,lo,hi,expected,last_id) VALUES(?,?,?,?,?)",
                        (entity, lower, last, n, lower),
                    )
                db.execute(
                    "INSERT OR REPLACE INTO planning VALUES(?,?,?)",
                    (entity, last, int(n < size)),
                )
            if n:
                generated += 1
                if max_ranges is not None and generated >= max_ranges:
                    return
            if n < size:
                break
            lower = last


def prefetch(batches, depth=2):
    q = queue.Queue(maxsize=depth)
    stop = threading.Event()
    sentinel = object()

    def put(item):
        while not stop.is_set():
            try:
                q.put(item, timeout=0.2)
                return
            except queue.Full:
                pass

    def produce():
        try:
            for item in batches:
                if stop.is_set():
                    break
                put(item)
        except BaseException as error:
            put(error)
        finally:
            put(sentinel)

    thread = threading.Thread(target=produce, daemon=True)
    thread.start()
    try:
        while True:
            item = q.get()
            if item is sentinel:
                break
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()
        thread.join(timeout=2)


def outputs(rows, entity, kind, metrics=None):
    from . import semantics

    for r in rows:
        started = time.monotonic()
        record = semantics.verify(r)
        if metrics is not None:
            metrics["parse_seconds"] += time.monotonic() - started
        if kind == "ids":
            yield from semantics.identifiers(entity, r["id"], r["version"], record)
        elif entity == "works":
            yield from semantics.references(r["id"], r["version"], record)


def audit_batch(g, task, edge, ids):
    b = Backend(writer=True)
    try:
        resources.checkpoint(g)
        limit = config().get("audit_batch_rows", 5000)
        entity = task["entity"]
        budget = (64 if entity == "works" else 32) * 1024**2
        query = f"SELECT id,raw,digest,version,source,line FROM {database(g)}.entities FINAL WHERE entity={sql_string(entity)} AND id>{sql_string(task['last_id'])} AND id<={sql_string(task['hi'])} ORDER BY id LIMIT {limit} FORMAT JSONEachRow"
        count = size = 0
        metrics = {"parse_seconds": 0.0}
        last = task["last_id"]
        read_parse_write = time.monotonic()
        chunks = bounded(
            b.ch_stream(query),
            max_rows=limit,
            max_bytes=budget,
            encode=lambda row: row["raw"].encode(),
        )
        for rows in prefetch(chunks, depth=2):
            for table, kind in ((edge, "edges"), (ids, "ids")):
                for batch in bounded(outputs(rows, entity, kind, metrics)):
                    b.insert(g, table, batch)
            count += len(rows)
            size += sum(len(row["raw"].encode()) for row in rows)
            last = rows[-1]["id"]
        if not count:
            raise RuntimeError("range ended before its saved upper bound")
        LOG.info(
            "audit batch generation=%s entity=%s range=%s..%s rows=%d raw_bytes=%d parse_seconds=%.3f pipeline_seconds=%.3f",
            g,
            entity,
            task["lo"],
            task["hi"],
            count,
            size,
            metrics["parse_seconds"],
            time.monotonic() - read_parse_write,
        )
        return last, count
    finally:
        b.close()


def confirm(db, task, last, count):
    if not task["last_id"] < last <= task["hi"] or count <= 0:
        raise RuntimeError("invalid range checkpoint")
    checked = task["checked"] + count
    done = last == task["hi"]
    if checked > task["expected"] or (done and checked != task["expected"]):
        raise RuntimeError("range coverage changed")
    started = time.monotonic()
    with db:
        changed = db.execute(
            "UPDATE tasks SET last_id=?,checked=?,state=? WHERE entity=? AND lo=? AND last_id=?",
            (
                last,
                checked,
                "done" if done else "pending",
                task["entity"],
                task["lo"],
                task["last_id"],
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError("stale range result")
    LOG.info(
        "audit checkpoint entity=%s lower=%s upper=%s last_id=%s seconds=%.3f",
        task["entity"],
        task["lo"],
        task["hi"],
        last,
        time.monotonic() - started,
    )


def run(g, b, runhash, legacy, edge, ids):
    p = generation_path(g)
    path = p / ("audit-ranges-" + runhash[:12] + ".sqlite")
    identities = b.ch(
        f"SELECT name,uuid FROM system.tables WHERE database={sql_string(database(g))} AND name IN ({sql_string(edge)},{sql_string(ids)}) ORDER BY name FORMAT JSONEachRow"
    )
    identity_path = path.with_suffix(".identity.json")
    if identity_path.exists() and json.loads(identity_path.read_text()) != identities:
        raise RuntimeError("audit range output table identity changed")
    atomic_json(identity_path, identities)
    db = connect(path)
    initial = int(legacy.get("checked", 0))
    try:
        plan(db, b, g, legacy, max_ranges=4)
        start = time.monotonic()
        base = db.execute("SELECT coalesce(sum(checked),0) FROM tasks").fetchone()[0]
        workers = min(16, max(1, config().get("audit_workers", 2)))
        maximum = min(16, config().get("audit_max_workers", workers))
        ramp = resources.Ramp([n for n in (2, 4, 8, 16) if n <= maximum] or [1])
        workers = ramp.workers
        last_admission = time.monotonic()
        last_checked = base
        with ProcessPoolExecutor(
            max_workers=maximum, mp_context=multiprocessing.get_context("spawn")
        ) as pool:
            active = {}
            try:
                while True:
                    resources.checkpoint(g)
                    pending_count = db.execute(
                        'SELECT count(*) FROM tasks WHERE state!="done"'
                    ).fetchone()[0]
                    if pending_count < workers * 2:
                        plan(db, b, g, legacy, max_ranges=workers * 2)
                    busy = {(t["entity"], t["lo"]) for t in active.values()}
                    pending = db.execute(
                        'SELECT * FROM tasks WHERE state!="done" ORDER BY entity,lo'
                    )
                    for row in pending:
                        if len(active) >= workers:
                            break
                        task = dict(row)
                        if (task["entity"], task["lo"]) in busy:
                            continue
                        active[pool.submit(audit_batch, g, task, edge, ids)] = task
                    if not active:
                        break
                    done, _ = wait(active, timeout=2, return_when=FIRST_COMPLETED)
                    for future in done:
                        task = active.pop(future)
                        last, count = future.result()
                        confirm(db, task, last, count)
                    counts = db.execute(
                        'SELECT count(*) total,coalesce(sum(state="done"),0) done,coalesce(sum(checked),0) checked,coalesce(sum(expected),0) expected FROM tasks'
                    ).fetchone()
                    planned = {
                        r["entity"]
                        for r in db.execute("SELECT entity FROM planning WHERE done=1")
                    }
                    planning_complete = all(
                        e in planned
                        or legacy.get("entities", {}).get(e, {}).get("done")
                        for e in ENTITIES
                    )
                    rate = (counts["checked"] - base) / max(1, time.monotonic() - start)
                    if time.monotonic() - last_admission >= 30:
                        sample = resources.checkpoint(g)
                        workers = ramp.observe(
                            counts["checked"] - last_checked,
                            sample,
                            resources.merge_backlog(b, g),
                        )
                        last_checked = counts["checked"]
                        last_admission = time.monotonic()
                    atomic_json(
                        p / "audit-progress.json",
                        dict(counts)
                        | dict(
                            state="running",
                            confirmed=initial + counts["checked"],
                            workers=workers,
                            planning_complete=planning_complete,
                            records_per_second=rate,
                            eta_seconds=(counts["expected"] - counts["checked"]) / rate
                            if rate and planning_complete
                            else None,
                            updated=time.time(),
                        ),
                    )
            finally:
                # Results already acknowledged by sinks must be checkpointed even on pause.
                for future, task in active.items():
                    if future.cancel():
                        continue
                    try:
                        last, count = future.result()
                        confirm(db, task, last, count)
                    except resources.Paused:
                        pass
        confirmed = (
            initial
            + db.execute("SELECT coalesce(sum(checked),0) FROM tasks").fetchone()[0]
        )
        progress = p / "audit-progress.json"
        saved = json.loads(progress.read_text()) if progress.exists() else {}
        atomic_json(
            progress,
            saved
            | {
                "state": "complete",
                "confirmed": confirmed,
                "planning_complete": True,
                "eta_seconds": 0,
                "updated": time.time(),
            },
        )
        return confirmed
    finally:
        db.close()
