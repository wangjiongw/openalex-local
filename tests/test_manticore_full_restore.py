import hashlib
import json

import pytest

from openalex_api import operations as recovery
from openalex_api.manticore_build import restore


def test_isolation_rejects_production_and_existing_directories(tmp_path):
    source = tmp_path / "production"
    source.mkdir()
    existing = tmp_path / "used"
    existing.mkdir()
    (existing / "data").write_text("keep")
    for home in (source, existing):
        with pytest.raises(ValueError, match="empty directory"):
            recovery.prepare_home(source, home, 19318, 18089)
    assert (existing / "data").read_text() == "keep"


def test_isolation_keeps_credentials_private_and_production_config_unchanged(tmp_path):
    source, home = tmp_path / "source", tmp_path / "isolated"
    (source / "config").mkdir(parents=True)
    (source / "var").mkdir()
    home.mkdir()
    original = dict(api_keys=["test-key"], port=8088, manticore_url="http://127.0.0.1:19308")
    (source / "config/local.json").write_text(json.dumps(original))
    (source / "var/active.json").write_text('{"generation":"test"}')
    env = recovery.prepare_home(source, home, 19318, 18089)
    assert env["OPENALEX_HOME"] == str(home)
    assert json.loads((source / "config/local.json").read_text()) == original
    conf = home / "config/local.json"
    assert conf.stat().st_mode & 0o777 == 0o600
    assert json.loads(conf.read_text())["manticore_url"] == "http://127.0.0.1:19318"


@pytest.mark.parametrize("missing", [True, False])
def test_restore_rejects_missing_or_corrupt_snapshot_before_import(tmp_path, missing):
    if not missing:
        (tmp_path / "data").write_bytes(b"bad")
    segment = dict(index="test_table", snapshot=dict(path=".", files=[
        dict(name="data", sha256=hashlib.sha256(b"good").hexdigest())]))
    with pytest.raises(FileNotFoundError if missing else RuntimeError):
        restore(object(), tmp_path, segment)


def test_restore_rejects_record_count_mismatch(tmp_path):
    class Client:
        def rows(self, sql):
            return [{"Table": "test_table"}] if sql == "SHOW TABLES" else [{"n": 2}]
    segment = dict(index="test_table", count=3, batches=[], snapshot=dict(path=".", files=[]))
    with pytest.raises(RuntimeError, match="missing or extra"):
        restore(Client(), tmp_path, segment)


def test_inventory_detects_escaping_symlinks_and_wrong_size(tmp_path, monkeypatch):
    root = tmp_path / "persistent"
    root.mkdir()
    monkeypatch.setattr(recovery, "ROOT", root)
    outside = tmp_path / "outside"
    outside.write_bytes(b"abc")
    (root / "data").symlink_to(outside)
    m = dict(segments=[dict(index="test", batches=[], snapshot=dict(path=".", files=[
        dict(name="data", bytes=3, sha256="hash")]))])
    with pytest.raises(ValueError, match="outside persistent"):
        recovery.inventory(root, m)
    (root / "data").unlink()
    (root / "data").write_bytes(b"shorter")
    with pytest.raises(ValueError, match="size mismatch"):
        recovery.inventory(root, m)


def test_import_alias_handles_dotted_parent_without_copying_or_changing_source(tmp_path, monkeypatch):
    from openalex_api import manticore_build as build
    from unittest.mock import Mock
    directory = tmp_path / 'release.v1' / 'snapshot'
    directory.mkdir(parents=True)
    source = directory / 'test_table.meta'
    source.write_bytes(b'persisted snapshot')
    segment = dict(index='test_table', snapshot=dict(path='release.v1/snapshot', files=[
        dict(name=source.name, sha256=hashlib.sha256(source.read_bytes()).hexdigest())]))
    monkeypatch.setattr(build, 'tables', lambda _: set())
    verify = Mock()
    monkeypatch.setattr(build, 'verify_segment', verify)
    imported = []
    class Client:
        def sql(self, query):
            prefix = query.split(" FROM '", 1)[1][:-1]
            alias = __import__('pathlib').Path(prefix)
            assert '.' not in str(alias)
            assert alias.with_suffix('.meta').read_bytes() == b'persisted snapshot'
            imported.append(alias.parent)
    build.restore(Client(), tmp_path, segment)
    assert imported and not imported[0].exists()
    assert source.read_bytes() == b'persisted snapshot'
    verify.assert_called_once()
