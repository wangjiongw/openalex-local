import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def continuation(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / "tests/fixtures/legacy_continue_build.py"
    spec = importlib.util.spec_from_file_location("release_continuation", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "generation_path", lambda _: tmp_path)
    monkeypatch.setattr(module.continuous, "stop_requested", False)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    progress = tmp_path / "reports/run/progress.json"
    progress.parent.mkdir(parents=True)
    progress.write_text(json.dumps({"generation":module.G,"profile":"postings-v1","state":"failed"}))
    (tmp_path / "postings-replay.json").write_text(json.dumps({"progress":str(progress)}))
    return module, progress


def test_transient_missing_state_is_retried_but_failed_state_stays_failed(continuation, monkeypatch):
    module, progress = continuation
    original = Path.read_text
    reads = []
    def read(path, *args, **kwargs):
        if path == progress:
            reads.append(path)
            if len(reads) < 3:
                raise FileNotFoundError("transient")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    _, path, document = module.diagnostic_progress()
    assert path == progress and len(reads) == 3
    with pytest.raises(RuntimeError, match="did not complete"):
        module.diagnostic_ready(document)


def test_permanently_missing_state_fails_with_path(continuation):
    module, progress = continuation
    progress.unlink()
    with pytest.raises(RuntimeError, match="diagnostic state unavailable") as caught:
        module.diagnostic_progress()
    assert str(progress) in str(caught.value)


def test_corrupt_or_wrong_generation_state_is_not_accepted(continuation):
    module, progress = continuation
    progress.write_text("{broken")
    with pytest.raises(json.JSONDecodeError): module.diagnostic_progress()
    progress.write_text(json.dumps({"generation":"other","profile":"postings-v1"}))
    with pytest.raises(RuntimeError, match="different run"): module.diagnostic_progress()


def test_diagnostic_state_cannot_escape_reports(continuation):
    module, progress = continuation
    (module.ROOT / "postings-replay.json").write_text(json.dumps({"progress":str(module.ROOT / "outside.json")}))
    with pytest.raises(RuntimeError, match="invalid diagnostic path"): module.diagnostic_progress()
