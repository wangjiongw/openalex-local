"""Persistent execution policy; independent metadata and search recovery gates."""

import json
import time
from .config import (
    ROOT,
    ENTITIES,
    atomic_json,
    generation_path,
    read_generation,
    config,
    search_is_active,
)
from . import recovery, ingest, manage
from .backend import Backend, index, index_expression

STAGES = (
    "preflight",
    "index-audit",
    "metadata-audit",
    "metadata-complete",
    "storage-verified",
    "search-complete",
    "complete",
)


def boundary(g, stage, until):
    if stage != until:
        return False
    from .build import mark

    mark(g, "paused", completed_stage=stage, requested_until=until)
    return True


def preflight(g):
    p = generation_path(g)
    plan = p / "execution-plan.json"
    policy = dict(
        version=1,
        old_index_failure="retire-and-rebuild",
        initial_workers=2,
        max_workers=min(4, config()["workers"]),
        check_threads=1,
        stable_seconds=3600,
    )
    if config().get("execution_profile") == "throughput96":
        policy = dict(
            version=2,
            old_index_failure="retire-and-rebuild",
            initial_workers=4,
            max_workers=32,
            audit_workers=[2, 4, 8, 16],
            check_threads=1,
            stable_seconds=900,
        )
    if (
        plan.exists()
        and json.loads(plan.read_text()) != policy
        and not (p / "throughput96-migration.json").exists()
    ):
        raise RuntimeError(
            "Execution policy changed; review existing run before replacement"
        )
    atomic_json(plan, policy)
    ingest.guard_capacity(config(), 0, 0)
    if (
        ingest.inventory(read_generation(g)["snapshot"])["manifest_sha256"]
        != read_generation(g)["manifest_sha256"]
    ):
        raise RuntimeError("Snapshot manifest changed")
    for name, state in manage.resource_status()["services"].items():
        if name != "recover-" + g and (
            name.startswith(("import-", "build-", "audit-", "verify-", "recover-"))
            or name == "canary"
        ):
            if state.get("running") or state.get("orphan_child"):
                raise RuntimeError("Concurrent workload: " + name)
    sample = json.loads((ROOT / "var/run/resource-latest.json").read_text())
    if time.time() - sample["time"] > 60:
        raise RuntimeError("Resource sampler is stale")
    return recovery.evidence(g, "preflight", passed=True, policy=policy)


def disposition(g):
    path = generation_path(g) / "retired-search.json"
    return json.loads(path.read_text()) if path.exists() else None


def retire(g, reason):
    """A durable alternative to validating old search, never an alternative to metadata audit."""
    if search_is_active(g):
        raise RuntimeError("Cannot retire active indexes")
    archive = recovery.require_evidence(g, "archive")
    p = generation_path(g) / "retired-search.json"
    with ingest.lock(generation_path(g) / "import.lock"):
        b = Backend(writer=True)
        try:
            saved = disposition(g)
            if saved is None:
                rows = b.os(
                    "GET",
                    "/_cat/indices/"
                    + index_expression(g)
                    + "?format=json&h=index,uuid",
                )
                names = {e: index(g, e) for e in ENTITIES}
                if {r["index"] for r in rows} != set(names.values()):
                    raise RuntimeError("Cannot retire incomplete index inventory")
                saved = dict(
                    generation=g,
                    manifest_sha256=read_generation(g)["manifest_sha256"],
                    names=names,
                    indexes=rows,
                    reason=reason,
                    time=time.time(),
                    archive=archive,
                    state="closing",
                    signature=recovery.signature(g),
                )
                atomic_json(p, saved)
            elif saved["manifest_sha256"] != read_generation(g)["manifest_sha256"]:
                raise RuntimeError("Retirement snapshot mismatch")
            for row in saved["indexes"]:
                current = b.os(
                    "GET", "/_cat/indices/" + row["index"] + "?format=json&h=index,uuid"
                )
                if current != [row]:
                    raise RuntimeError("Retired index identity changed")
                ensure_retired_index_closed(b, row["index"])
            saved.update(state="retired", signature=recovery.signature(g))
            atomic_json(p, saved)
            return saved
        finally:
            b.close()


def require_search_disposition(g):
    saved = disposition(g)
    if saved:
        if (
            saved.get("state") != "retired"
            or saved.get("signature") != recovery.signature(g)
            or saved.get("manifest_sha256") != read_generation(g)["manifest_sha256"]
        ):
            raise RuntimeError("Search retirement evidence stale or incomplete")
        return saved
    return recovery.require_evidence(g, "offline-index-check")


def prepare_search(g):
    """Create all replacement indexes before atomically selecting the new mapping."""
    recovery.require_evidence(g, "metadata-complete")
    recovery.require_evidence(g, "storage-verification")
    b = Backend(writer=True)
    try:
        retired = disposition(g)
        if not retired:
            settings = b.os("GET", "/" + index_expression(g) + "/_settings")
            if all(
                x["settings"]["index"].get("store", {}).get("type") == "niofs"
                for x in settings.values()
            ) and len(settings) == len(ENTITIES):
                return
            retired = retire(g, "Existing storage type not explicitly verified niofs")
        planned = generation_path(g) / "replacement-indexes.json"
        if planned.exists():
            names = json.loads(planned.read_text())
        else:
            names = {
                e: retired["names"][e] + "-rebuild-" + str(int(time.time()))
                for e in ENTITIES
            }
            atomic_json(planned, names)
        if set(names) != set(ENTITIES) or set(names.values()) & set(
            retired["names"].values()
        ):
            raise RuntimeError("Invalid replacement namespace")
        from .model import mapping

        for e, name in names.items():
            if config().get("execution_profile") == "throughput96":
                from .resources import checkpoint

                checkpoint(g)
            response = b.http.head(b.cfg["os_url"] + "/" + name)
            if response.status_code == 404:
                m = mapping(read_generation(g)["shards"].get(e, 1))
                m["settings"].update(
                    {
                        "index.store.type": "niofs",
                        "index.translog.durability": "request",
                        "index.merge.scheduler.max_thread_count": 1,
                    }
                )
                b.os("PUT", "/" + name, m)
            elif response.status_code != 200:
                response.raise_for_status()
            actual = b.os("GET", "/" + name + "/_settings")[name]["settings"]["index"]
            if actual.get("store", {}).get("type") != "niofs":
                raise RuntimeError("Replacement storage mismatch")
        atomic_json(generation_path(g) / "indexes.json", names)
    finally:
        b.close()


class Admission:
    def __init__(self, maximum):
        self.maximum = max(1, min(4, maximum))
        self.workers = min(2, self.maximum)
        self.stable = None

    def update(self, sample, now):
        healthy = (
            now - sample.get("time", 0) < 60
            and sample.get("aggregate_rss", 2**63) < 192 * 1024**3
        )
        healthy = healthy and all(
            p["nofile"][0] > 0 and p["fds"] < p["nofile"][0] * 0.7
            for p in sample.get("processes", [])
        )
        if not healthy:
            self.stable = None
            self.workers = min(2, self.maximum)
        else:
            self.stable = self.stable if self.stable is not None else now
            if now - self.stable >= 3600:
                self.workers = self.maximum
        return self.workers


def replay_unconfirmed_tails(g):
    """Finish only an already-observed uncheckpointed batch, never resume the whole shard."""
    import gzip, hashlib, orjson
    from .backend import database

    recovery.require_evidence(g, "archive")
    with ingest.lock(generation_path(g) / "import.lock"):
        with ingest.connect(g) as ledger:
            shards = [
                dict(r)
                for r in ledger.execute(
                    "SELECT * FROM shards WHERE selected=1 AND metadata_state!='done'"
                )
            ]
        b = Backend(writer=True)
        try:
            db = database(g)
            observed = {}
            for table in ("audit", "entities"):
                for row in b.ch(
                    f"SELECT source,max(line) n FROM {db}.{table} FINAL GROUP BY source FORMAT JSONEachRow"
                ):
                    observed[row["source"]] = max(
                        observed.get(row["source"], 0), int(row["n"])
                    )
            for s in shards:
                lower = s["metadata_line"]
                upper = observed.get(s["relative"], 0)
                if upper <= lower:
                    continue
                if upper > min(s["expected"], lower + config()["batch_size"]):
                    raise RuntimeError(
                        "Unconfirmed source tail exceeds one batch: " + s["relative"]
                    )
                if lower and not s["metadata_prefix_sha256"]:
                    raise RuntimeError("Cannot verify tail checkpoint prefix")
                from pathlib import Path

                if Path(s["path"]).stat().st_size != s["bytes"]:
                    raise RuntimeError("Source size changed")
                digest = hashlib.sha256()
                batch = []
                n = 0
                with gzip.open(s["path"], "rb") as stream:
                    for n, raw in enumerate(stream, 1):
                        digest.update(raw)
                        if (
                            n == lower
                            and digest.hexdigest() != s["metadata_prefix_sha256"]
                        ):
                            raise RuntimeError("Checkpoint source prefix changed")
                        if n > lower:
                            batch.append(
                                (n, s["ordinal"], s["relative"], raw, orjson.loads(raw))
                            )
                        if n == upper:
                            break
                if n != upper:
                    raise RuntimeError("Source truncated before unconfirmed tail")
                ingest.flush(b, g, s["entity"], batch, metadata_only=True)
                with ingest.connect(g) as ledger:
                    ledger.execute(
                        "UPDATE shards SET metadata_line=?,metadata_prefix_sha256=?,line=?,records=?,prefix_sha256=? WHERE path=?",
                        (
                            upper,
                            digest.hexdigest(),
                            upper,
                            upper,
                            digest.hexdigest(),
                            s["path"],
                        ),
                    )
                recovery.event(
                    g,
                    "tail-replay",
                    "confirmed",
                    s["relative"] + ": " + str(lower) + " -> " + str(upper),
                )
        finally:
            b.close()


def finish_derived_switches(g, b, state, path):
    from .backend import database, sql_string

    db = database(g)
    if state.get("signature") != recovery.signature(g):
        raise RuntimeError("Pending derived repair code signature changed")
    if (ROOT / "var/active.json").exists() and json.loads(
        (ROOT / "var/active.json").read_text()
    )["generation"] == g:
        raise RuntimeError("Cannot repair tables of the published generation")

    def uuid(table):
        rows = b.ch(
            f"SELECT toString(uuid) uuid FROM system.tables WHERE database={sql_string(db)} AND name={sql_string(table)} FORMAT JSONEachRow"
        )
        return rows[0]["uuid"] if rows else None

    for table, item in state["tables"].items():
        if item.get("switched"):
            continue
        old = item["retained_table"]
        staging = item["staging_table"]
        current = uuid(table)
        if (
            current == item["old_uuid"]
            and uuid(old) is None
            and uuid(staging) == item["new_uuid"]
        ):
            b.ch(f"RENAME TABLE {db}.{table} TO {db}.{old}")
            current = None
        if (
            current is None
            and uuid(old) == item["old_uuid"]
            and uuid(staging) == item["new_uuid"]
        ):
            b.ch(f"RENAME TABLE {db}.{staging} TO {db}.{table}")
            current = uuid(table)
        if current != item["new_uuid"] or uuid(old) != item["old_uuid"]:
            raise RuntimeError("Derived repair table UUID mismatch")
        item["switched"] = True
        atomic_json(path, state)


def audit_metadata(g):
    # Recover a rename gap before asking audit() to read the service table.
    with ingest.lock(generation_path(g) / "import.lock"):
        pending = list(generation_path(g).glob("derived-repair-*.json"))
        if pending:
            b = Backend(writer=True)
            try:
                for path in pending:
                    state = json.loads(path.read_text())
                    if any(
                        not item.get("switched") for item in state["tables"].values()
                    ):
                        finish_derived_switches(g, b, state, path)
            finally:
                b.close()
    result = recovery.audit(g, force=True)
    if result["passed"]:
        return result
    errors = result.get("errors", [])
    if not errors or any("unmatched rows" not in e for e in errors):
        raise RuntimeError(str(errors))
    # audit() returns only relation-difference errors after all raw/source checks pass.
    from .backend import database, sql_string

    p = generation_path(g) / ("derived-repair-" + result["runhash"][:12] + ".json")
    with ingest.lock(generation_path(g) / "import.lock"):
        db = database(g)
        token = result["runhash"][:12]
        state = (
            json.loads(p.read_text())
            if p.exists()
            else {
                "runhash": result["runhash"],
                "signature": recovery.signature(g),
                "tables": {},
            }
        )
        if state["runhash"] != result["runhash"]:
            raise RuntimeError("Derived repair identity changed; preserve prior repair")
        b = Backend(writer=True)

        def uuid(table):
            rows = b.ch(
                f"SELECT toString(uuid) uuid FROM system.tables WHERE database={sql_string(db)} AND name={sql_string(table)} FORMAT JSONEachRow"
            )
            return rows[0]["uuid"] if rows else None

        try:
            for table, expected in [
                ("references_out", "recovery_edges_" + token),
                ("references_in", "recovery_edges_" + token),
                ("identifiers", "recovery_ids_" + token),
            ]:
                if not any(table in error for error in errors):
                    continue
                replacement = "repair_" + table + "_" + token
                if table not in state["tables"]:
                    ingest.guard_capacity(config(), 0, 0)
                    b.ch(
                        f"CREATE TABLE IF NOT EXISTS {db}.{replacement} AS {db}.{table}"
                    )
                    b.ch(
                        f"INSERT INTO {db}.{replacement} SELECT * FROM {db}.{expected} FINAL"
                    )
                    state["tables"][table] = {
                        "old_uuid": uuid(table),
                        "new_uuid": uuid(replacement),
                        "retained_table": "retired_" + table + "_" + token,
                        "staging_table": replacement,
                    }
                    atomic_json(p, state)
                finish_derived_switches(g, b, state, p)
        finally:
            b.close()
    result = recovery.audit(g, force=True)
    if not result["passed"]:
        raise RuntimeError(str(result["errors"]))
    return result


def verify_published_api(g):
    import httpx

    c = config()
    with httpx.Client(
        timeout=15, trust_env=False, headers={"X-API-Key": c["api_keys"][0]}
    ) as h:
        response = h.get(f"http://127.0.0.1:{c['port']}/status")
        response.raise_for_status()
        state = response.json()
    if (
        state.get("generation") != g
        or not state.get("complete")
        or not all(
            state.get("backends", {}).get(k) for k in ("clickhouse", "opensearch")
        )
    ):
        raise RuntimeError("Published API generation/coverage/backend mismatch")


def observation_tick(state, now):
    gap = now - state.get("last", now)
    elapsed = state.get("seconds", 0) + gap if 0 <= gap <= 90 and "last" in state else 0
    return {"last": now, "seconds": elapsed}


def cleanup_audit_tables(g):
    import re
    from .backend import database, sql_string

    b = Backend(writer=True)
    db = database(g)
    try:
        names = [
            r["name"]
            for r in b.ch(
                f"SELECT name FROM system.tables WHERE database={sql_string(db)} FORMAT JSONEachRow"
            )
            if re.fullmatch(r"recovery_(edges|ids)_[0-9a-f]{12}", r["name"])
        ]
        atomic_json(
            generation_path(g) / "audit-cleanup.json",
            {"tables": names, "state": "planned"},
        )
        for name in names:
            b.ch(f"DROP TABLE IF EXISTS {db}.{name} SYNC")
        atomic_json(
            generation_path(g) / "audit-cleanup.json",
            {"tables": names, "state": "complete"},
        )
    finally:
        b.close()


def ensure_retired_index_closed(b, name):
    """An already closed index needs no new shard acknowledgements."""
    from .storage import assert_index_closed

    rows = b.os("GET", "/_cat/indices/" + name + "?format=json&h=index,status")
    if rows != [{"index": name, "status": "close"}]:
        result = b.os("POST", "/" + name + "/_close", {})
        if not result.get("acknowledged") or not result.get("shards_acknowledged"):
            raise RuntimeError("Retirement close not acknowledged")
    assert_index_closed(b, name)
