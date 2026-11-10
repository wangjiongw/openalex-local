from types import SimpleNamespace
import pytest
from openalex_api import ingest


def test_other_users_space_growth_not_project_usage(monkeypatch):
    monkeypatch.setattr(
        ingest.psutil, "disk_usage", lambda _: SimpleNamespace(free=20 * 1024**4)
    )
    monkeypatch.setattr(ingest, "measured_project_usage", lambda: 400 * 1024**3)
    ingest.guard_capacity({"disk_budget_gib": 4096}, 400 * 1024**3, 28 * 1024**4)


def test_actual_project_budget_and_volume_reserve(monkeypatch):
    monkeypatch.setattr(
        ingest.psutil, "disk_usage", lambda _: SimpleNamespace(free=20 * 1024**4)
    )
    monkeypatch.setattr(ingest, "measured_project_usage", lambda: 3800 * 1024**3)
    with pytest.raises(RuntimeError, match="measured project bytes"):
        ingest.guard_capacity({"disk_budget_gib": 4096}, 0, 0)
    monkeypatch.setattr(
        ingest.psutil, "disk_usage", lambda _: SimpleNamespace(free=40 * 1024**3)
    )
    with pytest.raises(RuntimeError, match="filesystem free"):
        ingest.guard_capacity({"disk_budget_gib": 4096}, 0, 0)


def test_storage_incident_blocks_import_before_writes(tmp_path, monkeypatch):
    (tmp_path / "resume-block.json").write_text("{}")
    monkeypatch.setattr(ingest, "generation_path", lambda _: tmp_path)
    with pytest.raises(RuntimeError, match="resume blocked"):
        ingest.run_import("full_test")
