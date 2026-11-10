"""Gated cutover. Archive old code/ledger, migrate the latest confirmed prefix, then resume."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openalex_api.config import ROOT, atomic_json, generation_name


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_atomic(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".install-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(source.read_bytes())
            out.flush()
            os.fsync(out.fileno())
        os.chmod(name, source.stat().st_mode & 0o777)
        os.replace(name, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("generation")
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    g = generation_name(args.generation)
    result = json.loads(args.benchmark.read_text())
    if (
        not result.get("passed")
        or not result.get("exact_fixture_verified")
        or not result.get("sample_coverage_passed")
        or not result.get("minimum_seconds_met")
        or result.get("improvement", 0) < 1.25
        or result.get("legacy_improvement", 0) < 1.25
        or not result.get("legacy_exact_fixture_verified")
    ):
        raise RuntimeError("complete-batch benchmark gate has not passed")
    validation = json.loads((ROOT / "reports/release-validation.json").read_text())
    current = {p.name: sha(p) for p in (ROOT / "openalex_api").glob("*.py")}
    junit = ROOT / "reports/throughput96-final-validation.xml"
    suites = list(ET.parse(junit).getroot().iter("testsuite"))
    test_count = sum(int(s.attrib.get("tests", 0)) for s in suites)
    if not test_count or any(int(s.attrib.get(k, 0)) for s in suites for k in ("failures", "errors", "skipped")):
        raise RuntimeError("complete isolated release suite has not passed")
    if validation.get("junit_sha256") != sha(junit):
        raise RuntimeError("release report changed after validation")
    if not validation.get("passed") or validation["code"] != current:
        raise RuntimeError(
            "release correctness evidence missing or code changed after tests"
        )
    target = ROOT.parent / "openalex-api"
    baseline = {p.name: sha(p) for p in (ROOT / "baseline/openalex_api").glob("*.py")}
    if result.get("legacy_code") != baseline:
        raise RuntimeError("legacy benchmark does not match preserved production code")
    actual = {p.name: sha(p) for p in (target / "openalex_api").glob("*.py")}
    journal = target / "var/generations" / g / "throughput96-install.json"
    state = json.loads(journal.read_text()) if journal.exists() else {}
    if state.get("state") == "complete":
        print(json.dumps(state))
        return
    if state and state.get("release") != current:
        raise RuntimeError("pending installation belongs to a different release")
    if not state and actual != baseline:
        raise RuntimeError("production code changed since baseline capture")
    print(
        json.dumps(
            dict(
                generation=g,
                benchmark=str(args.benchmark),
                improvement=result["improvement"],
                code_files=len(current),
                apply=args.apply,
            )
        ),
        flush=True,
    )
    if not args.apply:
        return
    python = target / ".venv/bin/python"

    def run(program, *extra, capture=False):
        return subprocess.run(
            [str(python), "-c", program, *extra],
            cwd=target,
            env=os.environ | {"OPENALEX_HOME": str(target), "PYTHONPATH": str(target)},
            check=True,
            text=True,
            capture_output=capture,
        )

    if not state:
        legacy = run(
            "from openalex_api.recovery import signature;import sys;print(signature(sys.argv[1]))",
            g,
            capture=True,
        ).stdout.strip()
        state = dict(
            state="prepared",
            legacy_signature=legacy,
            baseline=baseline,
            release=current,
            started=time.time(),
        )
        atomic_json(journal, state)
    if state["state"] == "prepared":
        if actual != baseline:
            raise RuntimeError(
                "cannot reconstruct incomplete backup from changed production code"
            )
        backup = target / "var/generations" / g / "before-throughput96"
        backup.mkdir(exist_ok=True)
        shutil.copytree(
            target / "openalex_api",
            backup / "openalex_api",
            ignore=shutil.ignore_patterns("__pycache__"),
            dirs_exist_ok=True,
        )
        copy_atomic(target / "config/local.json", backup / "local.json")
        shutil.copytree(target / "config", backup / "config", dirs_exist_ok=True)
        for filename in ("recovery-tests.xml", "recovery-tests.log"):
            previous = target / "reports" / filename
            if previous.exists():
                copy_atomic(previous, backup / "reports" / filename)
        # The v2 audit has no cooperative pause handler. Its durable cursor is the handoff
        # boundary; any unconfirmed output tail remains replayable in ReplacingMergeTree.
        run(
            'from openalex_api import manage;import sys;manage.stop("recover-"+sys.argv[1])',
            g,
        )
        run(
            "from openalex_api import manage;from openalex_api.config import ROOT;import sys;"
            'p=manage.identity(ROOT/"var/run"/("watch-"+sys.argv[1]+"-process.json"));'
            "p.terminate() if p else None; p.wait(timeout=40) if p else None",
            g,
        )
        state["state"] = "stopped"
        atomic_json(journal, state)
    if state["state"] == "stopped":
        # Always ensure the old controller is stopped before replacing any code.
        run(
            'from openalex_api import manage;import sys;manage.stop("recover-"+sys.argv[1])',
            g,
        )
        for directory in ("openalex_api", "tests", "scripts"):
            for source in (ROOT / directory).iterdir():
                if source.is_file():
                    copy_atomic(source, target / directory / source.name)
        shutil.copytree(ROOT / "baseline", target / "baseline", dirs_exist_ok=True)
        copy_atomic(ROOT / "THROUGHPUT96.md", target / "THROUGHPUT96.md")
        state["state"] = "installed"
        atomic_json(journal, state)
    if state["state"] == "installed":
        for filename in ("throughput96-final-validation.xml", "throughput96-final-validation.log", "release-validation.json"):
            copy_atomic(ROOT / "reports" / filename, target / "reports" / filename)
        run(
            """
import json,sys,os
from openalex_api import resources,manage,migration96
from openalex_api.backend import Backend
from openalex_api.config import ROOT
cpus=resources.configure()
b=Backend(writer=True)
try:
 state=json.loads((ROOT/'var/generations'/sys.argv[1]/'throughput96-install.json').read_text())
 migration96.migrate(sys.argv[1],b,state['legacy_signature'],state['baseline'])
finally:b.close()
# Change affinity only for identity-checked owned project processes and their threads.
for path in (ROOT/'var/run').glob('*.json'):
 proc=manage.identity(path)
 if proc:
  for child in [proc]+proc.children(recursive=True):
   try:
    for thread in child.threads():os.sched_setaffinity(thread.id,cpus)
   except ProcessLookupError:pass
manage.engine_configs()
b=Backend(writer=True)
try:b.ch('SYSTEM RELOAD CONFIG')
finally:b.close()
manage.stop('monitor')
manage.start('monitor')
""",
            g,
        )
        # Issue only the newly executed code-test proof. Storage/data proofs are not re-signed.
        run(
            "from openalex_api import recovery;from openalex_api.config import ROOT;import sys;"
            "recovery.evidence(sys.argv[1], 'recovery-tests', passed=True, tests=int(sys.argv[2]), "
            "report=str(ROOT/'reports/throughput96-final-validation.xml'), source='isolated-immutable-release-suite')",
            g, str(test_count),
        )
        state["state"] = "migrated"
        atomic_json(journal, state)
    if state["state"] == "migrated":
        run(
            'from openalex_api import manage;import sys;manage.start("recover-"+sys.argv[1],["--until","complete"])',
            g,
        )
        state.update(state="complete", completed=time.time())
        atomic_json(journal, state)
    print(json.dumps(state), flush=True)


if __name__ == "__main__":
    main()
