"""Direct projection for a journaled empty replacement index; final reconciliation remains mandatory."""

import json
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import multiprocessing
import orjson
from .backend import (
    Backend,
    index,
    index_expression,
    sql_string,
    database,
    request_slot,
)
from .config import ENTITIES, atomic_json, config, generation_path
from .model import project
from .batches import bounded
from . import resources


def project_batch(g, e, last, upper=None):
    b = Backend(writer=True)
    try:
        resources.checkpoint(g)
        bound = " AND id<=" + sql_string(upper) if upper is not None else ""
        stream = b.ch_stream(
            f"SELECT id,raw,version FROM {database(g)}.entities FINAL WHERE entity={sql_string(e)} AND id>{sql_string(last)}{bound} ORDER BY id LIMIT 5000 FORMAT JSONEachRow"
        )
        rows = []
        size = 0
        try:
            for row in stream:
                n = len(row["raw"].encode())
                if rows and size + n > 64 * 1024**2:
                    break
                rows.append(row)
                size += n
                if size >= 64 * 1024**2:
                    break
        finally:
            stream.close()
        if not rows:
            return last, 0

        def records():
            for row in rows:
                yield (
                    row["id"],
                    int(row["version"]),
                    project(e, orjson.loads(row["raw"])),
                )

        # Includes the exact bulk action line and newlines in the byte budget.
        def encode(item):
            key, version, projection = item
            return (
                orjson.dumps(
                    {
                        "index": {
                            "_index": index(g, e),
                            "_id": key,
                            "version": version,
                            "version_type": "external_gte",
                        }
                    }
                )
                + b"\n"
                + orjson.dumps(projection)
            )

        for batch in bounded(
            records(),
            max_rows=5000,
            max_bytes=min(config().get("search_bulk_bytes", 5 * 1024**2), 16 * 1024**2),
            encode=encode,
        ):
            with request_slot(
                "bulk", True, limit=min(8, config().get("search_bulk_concurrency", 2))
            ):
                b.bulk(g, e, batch)
        return rows[-1]["id"], len(rows)
    finally:
        b.close()


def run(g):
    from .search_build import index_epoch

    b = Backend(writer=True)
    path = generation_path(g) / "direct-search.json"
    try:
        epoch = index_epoch(b, g)
        identities = sorted(
            b.os(
                "GET",
                "/_cat/indices/" + index_expression(g) + "?format=json&h=index,uuid",
            ),
            key=lambda row: row["index"],
        )
        if path.exists():
            state = json.loads(path.read_text())
            if state.get("identities") != identities:
                raise RuntimeError(
                    "direct index identity changed; revalidation required"
                )
            state["epoch"] = epoch
            atomic_json(path, state)
            if state.get("complete"):
                return
        else:
            if not (generation_path(g) / "replacement-indexes.json").exists():
                return
            for e in ENTITIES:
                if b.os("GET", "/" + index(g, e) + "/_count")["count"] != 0:
                    return
            state = dict(
                epoch=epoch,
                identities=identities,
                entities={e: dict(last_id="", checked=0, done=False) for e in ENTITIES},
            )
            atomic_json(path, state)
        for e in ENTITIES:
            b.os(
                "PUT",
                "/" + index(g, e) + "/_settings",
                {"index.refresh_interval": "-1"},
            )
        from .intervals import connect, plan, confirm

        db = connect(generation_path(g) / "direct-search-ranges.sqlite")
        try:
            plan(db, b, g, {}, max_ranges=8)
            ramp = resources.Ramp(
                [4] if config().get("compatibility_mode") else [4, 8, 16]
            )
            workers = ramp.workers
            last_sample = time.monotonic()
            completed = 0
            with ProcessPoolExecutor(
                max_workers=16, mp_context=multiprocessing.get_context("spawn")
            ) as pool:
                active = {}
                try:
                    while True:
                        resources.checkpoint(g)
                        if (
                            db.execute(
                                'SELECT count(*) FROM tasks WHERE state!="done"'
                            ).fetchone()[0]
                            < workers * 2
                        ):
                            plan(db, b, g, {}, max_ranges=workers * 2)
                        busy = {(t["entity"], t["lo"]) for t in active.values()}
                        for row in db.execute(
                            'SELECT * FROM tasks WHERE state!="done" ORDER BY entity,lo'
                        ):
                            if len(active) >= workers:
                                break
                            task = dict(row)
                            if (task["entity"], task["lo"]) in busy:
                                continue
                            active[
                                pool.submit(
                                    project_batch,
                                    g,
                                    task["entity"],
                                    task["last_id"],
                                    task["hi"],
                                )
                            ] = task
                        if not active:
                            break
                        done, _ = wait(active, timeout=2, return_when=FIRST_COMPLETED)
                        for future in done:
                            task = active.pop(future)
                            last, n = future.result()
                            confirm(db, task, last, n)
                            completed += n
                        if time.monotonic() - last_sample >= 30:
                            workers = ramp.observe(
                                completed,
                                resources.checkpoint(g),
                                resources.merge_backlog(b, g),
                            )
                            completed = 0
                            last_sample = time.monotonic()
                            from .config import config as current_config

                            atomic_json(
                                __import__(
                                    "openalex_api.config", fromlist=["ROOT"]
                                ).ROOT
                                / "config/local.json",
                                current_config()
                                | {"search_bulk_concurrency": min(8, workers // 2)},
                            )
                        counters = dict(
                            db.execute(
                                'SELECT count(*) total,sum(state="done") done,sum(checked) checked FROM tasks'
                            ).fetchone()
                        )
                        atomic_json(
                            generation_path(g) / "search-progress.json",
                            dict(counters, workers=workers, updated=time.time()),
                        )
                finally:
                    for future, task in active.items():
                        if future.cancel():
                            continue
                        try:
                            last, n = future.result()
                            confirm(db, task, last, n)
                        except resources.Paused:
                            pass
        finally:
            db.close()
        for e in ENTITIES:
            b.os(
                "PUT",
                "/" + index(g, e) + "/_settings",
                {"index.refresh_interval": "1s"},
            )
            b.os("POST", "/" + index(g, e) + "/_refresh")
            b.os("POST", "/" + index(g, e) + "/_flush?wait_if_ongoing=true")
        state["complete"] = True
        atomic_json(path, state)
        progress = generation_path(g) / "search-progress.json"
        saved = json.loads(progress.read_text()) if progress.exists() else {}
        atomic_json(progress, saved | {"state": "complete", "updated": time.time()})
    finally:
        b.close()
