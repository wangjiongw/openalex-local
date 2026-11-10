from openalex_api.config import CODE_ROOT
import json
import pytest
from openalex_api import build, manage
from openalex_api.config import ROOT


def test_measured_benchmark_schema():
    report = json.loads((CODE_ROOT / "tests/fixtures/benchmark-warm.json").read_text())
    build.assert_benchmark(report)
    report["summary"]["detail"]["status_counts"]["503"] = 1
    with pytest.raises(RuntimeError, match="errors"):
        build.assert_benchmark(report)
    del report["summary"]["detail"]["status_counts"]["503"]
    report["summary"]["detail"]["p95_seconds"] = 0.6
    with pytest.raises(RuntimeError, match="p95"):
        build.assert_benchmark(report)


def test_build_managed_command():
    assert manage.state_path("build-full_test").name == "build-full_test.json"
    command, _ = manage.command("build-full_test", [])
    assert command[-2:] == ["openalex_api.build", "full_test"]


def test_validation_failure_never_publishes(tmp_path, monkeypatch):
    g = "full_test"
    folder = tmp_path / "var/generations" / g
    folder.mkdir(parents=True)
    monkeypatch.setattr(build, "ROOT", tmp_path)
    monkeypatch.setattr(build, "generation_path", lambda _: folder)
    monkeypatch.setattr(
        build, "read_generation", lambda _: {"mode": "full", "state": "imported"}
    )

    def invalid(_):
        raise RuntimeError("bad data")

    monkeypatch.setattr(build.ingest, "validate", invalid)
    monkeypatch.setattr(
        build.ingest, "publish", lambda *a, **k: pytest.fail("must not publish")
    )
    with pytest.raises(RuntimeError, match="bad data"):
        build.run(g)
    assert json.loads((folder / "build.json").read_text())["stage"] == "failed"
