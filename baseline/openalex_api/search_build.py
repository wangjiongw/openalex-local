"""Reconcile search IDs/versions against immutable ClickHouse records."""

import hashlib
import json
import time
import orjson
from . import ingest, recovery, diagnostics
from .config import ENTITIES, generation_path, read_generation, atomic_json
from .backend import (
    Backend,
    database,
    index,
    sql_string,
    sql_list,
    TombstoneConflict,
)
from .model import project


def repair_batch(b, g, e, batch):
    # A recently deleted ID can retain a higher tombstone version. Wait for the
    # normal tombstone expiry; never overwrite an unexpected existing version.
    for attempt in range(5):
        try:
            return b.bulk(g, e, batch)
        except TombstoneConflict:
            if attempt == 4:
                raise
            time.sleep(30)


def index_epoch(b, g):
    all_data = (
        b.os(
            "GET",
            "/_cluster/state/metadata?filter_path=metadata.indices.*.primary_terms,metadata.indices.*.settings.index.uuid",
        )
        .get("metadata", {})
        .get("indices", {})
    )
    data = {index(g, e): all_data.get(index(g, e)) for e in ENTITIES}
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def replace_entity(g, e):
    """Explicit per-entity replacement keeps the previous physical index for recovery."""
    from .model import mapping

    if e not in ENTITIES:
        raise ValueError("unknown entity")
    with ingest.lock(generation_path(g) / "import.lock"):
        recovery.require_evidence(g, "metadata-complete")
        recovery.require_evidence(g, "storage-verification")
        if read_generation(g).get("validated"):
            raise RuntimeError("cannot replace published/validated indexes")
        b = Backend(writer=True)
        try:
            names = {entity: index(g, entity) for entity in ENTITIES}
            old = names[e]
            new = old + "-repair-" + str(int(time.time()))
            m = mapping(read_generation(g)["shards"].get(e, 1))
            m["settings"].update(
                {
                    "index.store.type": "niofs",
                    "index.translog.durability": "request",
                    "index.merge.scheduler.max_thread_count": 1,
                }
            )
            b.os("PUT", "/" + new, m)
            retired = generation_path(g) / "retired-indexes.json"
            saved = json.loads(retired.read_text()) if retired.exists() else []
            saved.append({"entity": e, "old": old, "new": new, "time": time.time()})
            atomic_json(retired, saved)
            names[e] = new
            atomic_json(generation_path(g) / "indexes.json", names)
            b.os("POST", "/" + old + "/_close", {})
            for filename in (
                "search-apply-cursor.json",
                "search-audit-cursor.json",
                "search-complete.json",
            ):
                (generation_path(g) / filename).unlink(missing_ok=True)
            return {"entity": e, "retained_index": old, "replacement": new}
        finally:
            b.close()


@diagnostics.guarded_stage
def reconcile(g, apply=False):
    with ingest.lock(generation_path(g) / "import.lock"):
        recovery.require_evidence(g, "metadata-complete")
        if apply:
            recovery.require_stage(g, "search")
        b = Backend(writer=True)
        db = database(g)
        report = {
            "checked": 0,
            "missing": 0,
            "outdated": 0,
            "unexpected_versions": 0,
            "repaired": 0,
            "extra": 0,
            "mode": "apply" if apply else "report",
        }
        name = "search-apply-cursor.json" if apply else "search-audit-cursor.json"
        path = generation_path(g) / name
        previous = json.loads(path.read_text()) if path.exists() else {}
        if apply:
            b.create(g, read_generation(g)["shards"])
        epoch = index_epoch(b, g)
        if (
            previous.get("signature") != recovery.signature(g)
            or previous.get("epoch") != epoch
            or previous.get("completed")
        ):
            # Recheck IDs after a shard restart; matching records are not rewritten.
            previous = {
                "entities": {},
                "report": report,
                "signature": recovery.signature(g),
                "epoch": epoch,
            }
        report = previous["report"]
        try:
            for e in ENTITIES:
                head = b.http.head(b.cfg["os_url"] + "/" + index(g, e))
                if head.status_code not in (200, 404):
                    head.raise_for_status()
                exists = head.status_code == 200
                state = previous["entities"].setdefault(
                    e, {"last_id": "", "done": False}
                )
                while not state["done"]:
                    ingest.guard_capacity(b.cfg, 0, 0)
                    rows = b.ch(
                        f"SELECT id,version FROM {db}.entities FINAL WHERE entity={sql_string(e)} AND id>{sql_string(state['last_id'])} ORDER BY id LIMIT 1000 FORMAT JSONEachRow"
                    )
                    if not rows:
                        state["done"] = True
                        atomic_json(path, previous)
                        break
                    docs = (
                        b.mget(g, e, [r["id"] for r in rows])
                        if exists
                        else [{"_id": r["id"], "found": False} for r in rows]
                    )
                    repair = []
                    for row, doc in zip(rows, docs):
                        if not doc.get("found"):
                            report["missing"] += 1
                            repair.append(row["id"])
                        elif int(doc["_version"]) < int(row["version"]):
                            report["outdated"] += 1
                            repair.append(row["id"])
                        elif int(doc["_version"]) > int(row["version"]):
                            report["unexpected_versions"] += 1
                            raise RuntimeError(
                                "Index version ahead of frozen metadata: " + row["id"]
                            )
                    if repair and apply:
                        raw = b.ch(
                            f"SELECT id,raw,version FROM {db}.entities FINAL WHERE entity={sql_string(e)} AND id IN {sql_list(repair)} FORMAT JSONEachRow"
                        )
                        batch = []
                        size = 0
                        for r in raw:
                            projection = project(e, orjson.loads(r["raw"]))
                            item = (r["id"], int(r["version"]), projection)
                            size += len(orjson.dumps(projection))
                            batch.append(item)
                            if size >= 16 * 1024**2:
                                repair_batch(b, g, e, batch)
                                batch = []
                                size = 0
                        if batch:
                            repair_batch(b, g, e, batch)
                        check = b.mget(g, e, [r["id"] for r in rows])
                        versions = {r["id"]: int(r["version"]) for r in rows}
                        if any(
                            not d.get("found")
                            or int(d["_version"]) != versions[d["_id"]]
                            for d in check
                        ):
                            raise RuntimeError("Post-repair version mismatch")
                        report["repaired"] += len(repair)
                    report["checked"] += len(rows)
                    state["last_id"] = rows[-1]["id"]
                    previous["updated"] = time.time()
                    atomic_json(path, previous)
                    with ingest.connect(g) as ledger:
                        ledger.execute(
                            "UPDATE search_tasks SET last_id=?,checked=checked+?,repaired=repaired+?,state='running',updated=? WHERE entity=?",
                            (
                                state["last_id"],
                                len(rows),
                                len(repair) if apply else 0,
                                time.time(),
                                e,
                            ),
                        )
                if not exists:
                    continue
                # Independent index -> metadata traversal detects extras. Restart this entity pass if interrupted.
                b.os("POST", "/" + index(g, e) + "/_refresh")
                pit = b.os(
                    "POST", "/" + index(g, e) + "/_search/point_in_time?keep_alive=10m"
                )["pit_id"]
                after = None
                try:
                    while True:
                        body = {
                            "size": 1000,
                            "pit": {"id": pit, "keep_alive": "10m"},
                            "sort": [{"id": "asc"}],
                            "_source": False,
                            "version": True,
                            "track_total_hits": False,
                        }
                        if after:
                            body["search_after"] = after
                        hits = b.os("POST", "/_search", body)["hits"]["hits"]
                        if not hits:
                            break
                        keys = [x["_id"] for x in hits]
                        found = b.ch(
                            f"SELECT id FROM {db}.entities FINAL WHERE entity={sql_string(e)} AND id IN {sql_list(keys)} FORMAT JSONEachRow"
                        )
                        extra = set(keys) - {r["id"] for r in found}
                        if extra:
                            report["extra"] += len(extra)
                            # Any deletion must first have a durable exact difference manifest.
                            with (generation_path(g) / "search-extra.jsonl").open(
                                "a"
                            ) as f:
                                f.write(
                                    json.dumps({"entity": e, "ids": sorted(extra)})
                                    + "\n"
                                )
                                f.flush()
                                __import__("os").fsync(f.fileno())
                            if apply:
                                payload = b"".join(
                                    orjson.dumps(
                                        {"delete": {"_index": index(g, e), "_id": k}}
                                    )
                                    + b"\n"
                                    for k in sorted(extra)
                                )
                                result = b.os("POST", "/_bulk", content=payload)
                                if len(result.get("items", [])) != len(extra) or any(
                                    v["delete"]["status"] not in (200, 404)
                                    for v in result["items"]
                                ):
                                    raise RuntimeError("Extra-record deletion failed")
                        after = hits[-1]["sort"]
                finally:
                    b.os("DELETE", "/_search/point_in_time", {"pit_id": [pit]})
                with ingest.connect(g) as ledger:
                    ledger.execute(
                        "UPDATE search_tasks SET state='done',updated=? WHERE entity=?",
                        (time.time(), e),
                    )
            if index_epoch(b, g) != epoch:
                raise RuntimeError(
                    "Index allocation changed during audit; recheck IDs before resuming"
                )
            previous["completed"] = True
            atomic_json(path, previous)
            if not apply and (
                report["missing"] or report["outdated"] or report["extra"]
            ):
                return recovery.evidence(g, "search-audit", passed=False, **report)
            return recovery.evidence(
                g,
                "search-complete" if apply else "search-audit",
                passed=True,
                epoch=epoch,
                **report,
            )
        finally:
            b.close()
