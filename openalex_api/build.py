"""Resumable full build, fail-closed validation, publication and bounded cleanup."""

import argparse
import json
import logging
import subprocess
import sys
import time

from . import ingest
from .backend import Backend, database, index_expression, sql_string
from .config import ROOT, atomic_json, config, generation_path, read_generation


def mark(g, stage, **extra):
    atomic_json(
        generation_path(g) / "build.json",
        dict(generation=g, stage=stage, updated=time.time(), **extra),
    )
    if config().get("execution_profile") == "throughput96":
        state = (
            stage
            if stage in ("initializing", "pausing", "paused", "failed", "complete")
            else "running"
        )
        atomic_json(
            generation_path(g) / "control.json",
            dict(state=state, stage=stage, updated=time.time()),
        )
    logging.info("Build %s: %s", g, stage)


def settle(g, timeout=86400):
    """Let engines retire temporary parts normally; never unlink engine files."""
    b = Backend(writer=True)
    deadline = time.monotonic() + timeout
    stable = 0
    try:
        b.os("POST", "/" + index_expression(g) + "/_flush?wait_if_ongoing=true")
        while time.monotonic() < deadline:
            merges = b.ch(
                f"SELECT count() n FROM system.merges WHERE database={sql_string(database(g))} FORMAT JSONEachRow"
            )[0]["n"]
            inactive = b.ch(
                f"SELECT sum(bytes_on_disk) n FROM system.parts WHERE NOT active AND database={sql_string(database(g))} FORMAT JSONEachRow"
            )[0]["n"]
            cap = ingest.capacity(g)
            os_merges = b.os("GET", "/" + index_expression(g) + "/_stats/merges")[
                "_all"
            ]["total"]["merges"]["current"]
            stable = (
                stable + 1
                if not merges
                and not inactive
                and not os_merges
                and cap["storage_settled"]
                else 0
            )
            mark(
                g,
                "settling",
                stable_checks=stable,
                clickhouse_merges=merges,
                inactive_bytes=inactive,
                opensearch_merges=os_merges,
            )
            if stable >= 3:
                return cap
            time.sleep(60)
        raise RuntimeError(
            "Background storage has not settled within 24 hours; inspect and resume build"
        )
    finally:
        b.close()


def run(g):
    if read_generation(g).get("pipeline_version") == 2:
        from .workflow import build

        return build(g)
    with ingest.lock(generation_path(g) / "build.lock"):
        meta = read_generation(g)
        if meta["mode"] != "full":
            raise ValueError("build requires a full generation")
        saved_path = generation_path(g) / "build.json"
        if (
            saved_path.exists()
            and json.loads(saved_path.read_text()).get("stage") == "complete"
        ):
            return
        plan_path = generation_path(g) / "build-plan.json"
        if not plan_path.exists():
            pointer = ROOT / "var/active.json"
            previous = (
                json.loads(pointer.read_text())["generation"]
                if pointer.exists()
                else None
            )
            atomic_json(plan_path, dict(previous=previous, created=time.time()))
        plan = json.loads(plan_path.read_text())
        try:
            if not meta.get("validated"):
                if meta["state"] != "imported":
                    mark(g, "importing")
                    ingest.run_import(g)
                mark(g, "validating")
                ingest.validate(g)
            settle(g)
            mark(g, "publishing")
            pointer = ROOT / "var/active.json"
            active_g = (
                json.loads(pointer.read_text())["generation"]
                if pointer.exists()
                else None
            )
            if active_g not in (None, plan["previous"], g):
                raise RuntimeError(
                    "Active generation changed independently; refusing to overwrite it"
                )
            if active_g != g:
                ingest.publish(g)
            mark(g, "benchmarking")
            label = "full-" + g
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/benchmark.py"),
                    "--seconds",
                    "1800",
                    "--qps",
                    "5",
                    "--concurrency",
                    "10",
                    "--label",
                    label,
                ],
                cwd=ROOT,
                check=True,
            )
            report = json.loads(
                (ROOT / "reports" / f"benchmark-{label}.json").read_text()
            )
            # Preserve the old generation on any failed performance/functional acceptance.
            assert_benchmark(report)
            mark(g, "cleaning")
            old = plan["previous"]
            if old and old != g and generation_path(old).exists():
                old_meta = read_generation(old)
                # Only the superseded pilot is disposable build data; never delete another full version.
                if (
                    old_meta["mode"] == "pilot"
                    and old_meta["manifest_sha256"]
                    == read_generation(g)["manifest_sha256"]
                ):
                    retired = json.loads(
                        (generation_path(old) / "retired.json").read_text()
                    )["time"]
                    until = (
                        retired
                        + config()["cursor_ttl"]
                        + config()["query_timeout"]
                        + 61
                    )
                    while time.time() < until:
                        time.sleep(min(30, until - time.time()))
                    ingest.cleanup(old, old)
            cap = settle(g)
            mark(
                g,
                "complete",
                complete=True,
                capacity_report=str(generation_path(g) / "capacity.json"),
                benchmark_report=str(ROOT / "reports" / f"benchmark-{label}.json"),
                storage_settled=cap["storage_settled"],
            )
        except Exception as error:
            # Roll back only our own publication, and retain all build data for diagnosis.
            pointer = ROOT / "var/active.json"
            if (
                pointer.exists()
                and json.loads(pointer.read_text())["generation"] == g
                and plan["previous"]
                and generation_path(plan["previous"]).exists()
            ):
                ingest.publish(plan["previous"], partial=True)
            mark(g, "failed", error=str(error))
            raise


def assert_benchmark(report):
    required = {"detail", "search", "references", "match"}
    if not required.issubset(report.get("summary", {})):
        raise RuntimeError("Benchmark missing required request classes")
    for kind, stats in report["summary"].items():
        if (
            not stats.get("requests")
            or sum(stats.get("status_counts", {}).values()) != stats["requests"]
        ):
            raise RuntimeError("Benchmark response count mismatch/errors")
        if any(str(code) != "200" and n for code, n in stats["status_counts"].items()):
            raise RuntimeError("Benchmark returned errors: " + kind)
        limit = {"detail": 0.5, "search": 2, "match": 3, "references": 1}.get(kind)
        if limit and (stats["p95_seconds"] is None or stats["p95_seconds"] > limit):
            raise RuntimeError("Benchmark p95 exceeded target: " + kind)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument("generation")
    run(parser.parse_args().generation)
