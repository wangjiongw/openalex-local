#!/usr/bin/env python3
"""Complete an already-authorized pilot build; never starts a full import."""

import argparse, json, time, logging
from concurrent.futures import ThreadPoolExecutor
from openalex_api import ingest, manage
from openalex_api.config import ROOT, read_generation, atomic_json
from openalex_api.backend import Backend, database, index_expression


def main(g):
    if read_generation(g)["mode"] != "pilot":
        raise RuntimeError("only pilot generations are supported")
    while True:
        state = ingest.status(g)
        rows = sum(r["records"] for r in state["progress"] if r["selected"])
        print("Import progress", state["state"], rows, flush=True)
        if state["state"] == "imported":
            break
        if state["state"] == "failed" or state["errors"]:
            raise RuntimeError("import errors must be resolved before finalizing")
        if state.get("validated"):
            break
        time.sleep(15)
    if not read_generation(g).get("validated"):
        print("Validating", flush=True)
        ingest.validate(g)

    # Compact this small, immutable pilot only. Do not automatically force-merge a full-scale database.
    def compact():
        b = Backend(writer=True)
        try:
            for table in [
                "entities",
                "audit",
                "identifiers",
                "references_out",
                "references_in",
            ]:
                b.ch(f"OPTIMIZE TABLE {database(g)}.{table} FINAL")
            # May take several minutes; retain an explicit writer timeout distinct from API budgets.
            r = b.http.post(
                b.cfg["os_url"] + "/" + index_expression(g) + "/_forcemerge",
                params={"max_num_segments": 1, "flush": "true"},
                timeout=1800,
            )
            r.raise_for_status()
            result = r.json()
            if result.get("_shards", {}).get("failed", 0):
                raise RuntimeError("force merge shard failures")
        finally:
            b.close()

    print("Measuring pilot compaction", flush=True)
    samples = []
    start = time.time()
    b = Backend(writer=True)
    try:
        with ThreadPoolExecutor(1) as pool:
            f = pool.submit(compact)
            while not f.done():
                ch = b.ch(
                    f"SELECT sum(bytes_on_disk) bytes FROM system.parts WHERE database='{database(g)}' FORMAT JSONEachRow"
                )[0]
                osstats = b.os(
                    "GET", "/" + index_expression(g) + "/_stats/store,translog"
                )
                total = osstats["_all"]["total"]
                samples.append(
                    dict(
                        time=time.time(),
                        clickhouse_all_parts_bytes=int(ch["bytes"]),
                        opensearch_store_bytes=total["store"]["size_in_bytes"],
                        opensearch_translog_bytes=total["translog"]["size_in_bytes"],
                        resources=manage.resource_status(),
                    )
                )
                time.sleep(5)
            f.result()
    finally:
        b.close()
    atomic_json(
        ROOT / "reports/pilot-compaction.json",
        dict(generation=g, seconds=time.time() - start, samples=samples),
    )
    ingest.validate(g)
    cap = ingest.capacity(g)
    deadline = time.monotonic() + 1200
    while not cap["storage_settled"]:
        if time.monotonic() > deadline:
            raise RuntimeError(
                "old index segments still retained; rerun capacity after cursor/reader expiry"
            )
        time.sleep(15)
        cap = ingest.capacity(g)
    print("Capacity", json.dumps(cap), flush=True)
    print("Publishing explicitly partial generation", flush=True)
    ingest.publish(g, partial=True)
    manage.stop("api")
    manage.start("api")
    print(
        "Pilot ready; full import is NOT started. Capacity gate:",
        cap["within_initial_budget"],
        flush=True,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    p = argparse.ArgumentParser()
    p.add_argument("generation")
    a = p.parse_args()
    main(a.generation)
