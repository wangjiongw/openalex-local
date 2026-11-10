"""Durable recovery/build orchestrator; stages never bypass failed evidence."""

import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from . import ingest, recovery, storage, search_build, manage, diagnostics, resume
from .config import ROOT, generation_path, read_generation
from .build import mark, assert_benchmark, settle


def selftest(g):
    try:
        return recovery.require_evidence(g, "recovery-tests")
    except RuntimeError:
        pass
    tested_signature = recovery.signature(g)
    junit = ROOT / "reports/recovery-tests.xml"
    with (ROOT / "reports/recovery-tests.log").open("w") as out:
        env = os.environ.copy()
        env["OA_LIVE_TESTS"] = "1"
        subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "--junitxml=" + str(junit)],
            cwd=ROOT,
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
            check=True,
        )
    if recovery.signature(g) != tested_signature:
        raise RuntimeError("Code/config changed while tests were running; rerun tests")
    suites = list(ET.parse(junit).getroot().iter("testsuite"))
    n = sum(int(s.attrib.get("tests", 0)) for s in suites)
    if not n or any(
        int(s.attrib.get(k, 0)) for s in suites for k in ("failures", "errors")
    ):
        raise RuntimeError("Recovery tests failed")
    for path in (ROOT / "var/generations").glob("test_*/generation.json"):
        meta = json.loads(path.read_text())
        if (
            __import__("pathlib")
            .Path(meta["snapshot"])
            .is_relative_to(ROOT / "var/test-fixtures")
        ):
            ingest.cleanup(meta["generation"], meta["generation"])
    return recovery.evidence(
        g, "recovery-tests", passed=True, tests=n, report=str(junit)
    )


@diagnostics.guarded_stage
def recover(g, until="complete"):
    if until not in resume.STAGES:
        raise ValueError("unknown stopping stage")
    with ingest.lock(generation_path(g) / "recovery-controller.lock"):
        from .config import atomic_json
        atomic_json(generation_path(g)/"execution-run.json", {"until":until,"started":time.time()})
        if read_generation(g).get("validated"):
            return build(g, until=until)
        manage.stop("api")
        manage.start("clickhouse")
        manage.start("opensearch")
        manage.start("monitor")
        recovery.migrate(g)
        mark(g, "preflight")
        selftest(g)
        resume.preflight(g)
        if resume.boundary(g,"preflight",until):return {"paused_at":until}
        mark(g, "preserving")
        storage.archive(g)
        mark(g, "checking-existing-indexes")
        if resume.disposition(g):
            resume.retire(g, "Resume recorded retirement")
        else:
            try:
                storage.check_indexes(g)
            except storage.IndexIntegrityFailure as error:
                resume.retire(g, str(error))
        if resume.boundary(g,"index-audit",until):return {"paused_at":until}
        manage.stop("opensearch")
        mark(g, "auditing-existing-metadata")
        resume.replay_unconfirmed_tails(g)
        result = resume.audit_metadata(g)
        if not result["passed"]:
            raise RuntimeError(str(result["errors"]))
        recovery.release(g, "metadata")
        if resume.boundary(g,"metadata-audit",until):return {"paused_at":until}
        return build(g, until=until)


@diagnostics.guarded_stage
def build(g, until="complete"):
    with ingest.lock(generation_path(g) / "build.lock"):
        p = generation_path(g)
        try:
            manage.start("monitor")
            recovery.require_stage(g, "metadata")
            if not read_generation(g).get("metadata_frozen"):
                manage.stop("api")
                manage.stop("opensearch")
                manage.start("clickhouse")
                mark(g, "metadata-importing")
                ingest.run_import(g)
                mark(g, "metadata-validating")
                recovery.metadata_complete(g)
            if resume.boundary(g,"metadata-complete",until):return {"paused_at":until}
            try:
                recovery.require_evidence(g, "storage-verification")
            except RuntimeError:
                mark(g, "storage-verifying")
                storage.verify(g)
            if resume.boundary(g,"storage-verified",until):return {"paused_at":until}
            manage.start("opensearch")
            storage.promote_profile(g)
            resume.prepare_search(g)
            recovery.release(g, "search")
            try:
                recovery.require_evidence(g, "search-complete")
            except RuntimeError:
                mark(g, "search-reconciling")
                search_build.reconcile(g, apply=True)
            try:
                recovery.require_evidence(g, "final-index-check")
            except RuntimeError:
                storage.check_indexes(g, final=True)
                # CheckIndex closes/reopens shards. Record the verified post-check allocation,
                # not an obsolete primary term from before that controlled transition.
                b = __import__("openalex_api.backend", fromlist=["Backend"]).Backend(writer=True)
                try:
                    verified = recovery.require_evidence(g, "search-complete")
                    payload = {k:v for k,v in verified.items() if k not in ("generation","signature","time","epoch")}
                    recovery.evidence(g, "search-complete", **payload, epoch=search_build.index_epoch(b,g), physical_verified=True)
                finally:
                    b.close()
            if resume.boundary(g,"search-complete",until):return {"paused_at":until}
            if not read_generation(g).get("validated"):
                mark(g, "validating")
                ingest.validate(g)
            mark(g, "settling")
            settle(g)
            try:
                recovery.require_evidence(g, "api-validation")
            except RuntimeError:
                mark(g, "api-validating")
                service = "validation-api-" + g
                manage.start(service)
                label = "full-" + g
                try:
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
                            "--generation",
                            g,
                            "--base-url",
                            "http://127.0.0.1:18088",
                        ],
                        cwd=ROOT,
                        check=True,
                    )
                    report = json.loads(
                        (
                            ROOT / "reports" / ("benchmark-" + label + ".json")
                        ).read_text()
                    )
                    assert_benchmark(report)
                    evaluation = json.loads(
                        (
                            ROOT / "reports" / ("evaluation-" + label + ".json")
                        ).read_text()
                    )
                    ids = evaluation["summary"]["detail"]
                    if ids["requests"] != 1000 or ids["hits"] != 1000 or ids["errors"]:
                        raise RuntimeError("Exact ID acceptance failed")
                    recovery.evidence(
                        g, "api-validation", passed=True, benchmark=report
                    )
                finally:
                    manage.stop(service)
            storage.archive(g, final=True)
            mark(g, "publishing")
            active = ROOT / "var/active.json"
            previous = (
                json.loads(active.read_text())["generation"]
                if active.exists()
                else None
            )
            if previous != g:
                ingest.publish(g)
            manage.start("api")
            resume.verify_published_api(g)
            pub = json.loads((p / "published.json").read_text())
            observation_path=p / "observation.json"
            observation=json.loads(observation_path.read_text()) if observation_path.exists() else {}
            observation=resume.observation_tick(observation,time.time())
            while observation["seconds"] < 86400:
                mark(g,"observing",observed_seconds=observation["seconds"],required_seconds=86400)
                resume.verify_published_api(g)
                if not all(
                    manage.health(s) for s in ("clickhouse", "opensearch", "api")
                ):
                    raise RuntimeError("Post-publication health check failed")
                b = __import__("openalex_api.backend", fromlist=["Backend"]).Backend(
                    writer=True
                )
                try:
                    expected = recovery.require_evidence(g, "search-complete")["epoch"]
                    if search_build.index_epoch(b, g) != expected:
                        raise RuntimeError(
                            "Published index allocation changed; re-audit required"
                        )
                finally:
                    b.close()
                observation=resume.observation_tick(observation,time.time())
                from .config import atomic_json
                atomic_json(observation_path,observation)
                time.sleep(30)
            mark(g, "cleaning")
            resume.cleanup_audit_tables(g)
            before = pub.get("previous")
            old = before["generation"] if before else None
            if old and old != g and generation_path(old).exists():
                m = read_generation(old)
                if (
                    m["mode"] == "pilot"
                    and m["manifest_sha256"] == read_generation(g)["manifest_sha256"]
                ):
                    ingest.cleanup(old, old)
            settle(g)
            owner = ROOT / "var/storage-canary/owner.json"
            if (
                owner.exists()
                and json.loads(owner.read_text())["generation"] == g
                and not manage.health("canary")
            ):
                __import__("shutil").rmtree(
                    ROOT / "var/storage-canary/data", ignore_errors=False
                )
            mark(g, "complete", complete=True)
            return {"generation": g, "complete": True}
        except Exception as error:
            pointer = ROOT / "var/active.json"
            publication = p / "published.json"
            if (
                pointer.exists()
                and publication.exists()
                and json.loads(pointer.read_text())["generation"] == g
            ):
                manage.stop("api")
                prior = json.loads(publication.read_text()).get("previous")
                if (
                    prior
                    and prior["generation"] != g
                    and generation_path(prior["generation"]).exists()
                ):
                    ingest.publish(prior["generation"], partial=True)
            recovery.event(g, "build", "failed", str(error))
            previous = json.loads((p / "build.json").read_text())
            mark(g, "failed", failed_stage=previous.get("failed_stage") or previous.get("stage"), diagnostic_path=previous.get("diagnostic_path"), error=str(error))
            raise
