import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from openalex_api import config as settings
from openalex_api.config import CODE_ROOT
from openalex_api import community_pipeline as pipeline
from openalex_api.demo import make_snapshot


def test_workspace_argument_overrides_environment_before_import(tmp_path):
    home = tmp_path / 'explicit'
    other = tmp_path / 'environment'
    env = os.environ | {'OPENALEX_HOME': str(other), 'PYTHONPATH': str(CODE_ROOT)}
    result = subprocess.run([sys.executable, '-m', 'openalex_api.cli', '--workspace', str(home), 'init'],
                            cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (home / 'config/local.json').exists()
    assert not (other / 'config/local.json').exists()
    assert not (home / 'openalex_api').exists()


def test_metadata_creation_does_not_create_opensearch(tmp_path, monkeypatch):
    from openalex_api import ingest
    snapshot = tmp_path / 'snapshot'
    make_snapshot(snapshot)
    workspace = tmp_path / 'workspace'
    backend = Mock()
    monkeypatch.setattr(ingest, 'ROOT', workspace)
    monkeypatch.setattr(ingest, 'config', lambda: settings.config() | {'snapshot': str(snapshot)})
    monkeypatch.setattr(ingest, 'generation_path', lambda g: workspace / 'var/generations' / g)
    monkeypatch.setattr(ingest, 'Backend', lambda **kwargs: backend)
    meta = ingest.create_generation('example', mode='full', metadata_only=True)
    assert meta['pipeline_version'] == 3 and not meta['snapshot_complete']
    backend.create.assert_called_once_with('example', {}, search=False)
    with ingest.connect('example') as db:
        assert 'metadata_state' in {r['name'] for r in db.execute('PRAGMA table_info(shards)')}


def test_unowned_service_is_not_replaced(monkeypatch):
    from openalex_api import manage
    start = Mock()
    monkeypatch.setattr(manage, 'identity', lambda _: None)
    monkeypatch.setattr(manage, 'health', lambda _: True)
    monkeypatch.setattr(manage, 'start', start)
    with pytest.raises(RuntimeError, match='unowned'):
        pipeline.ensure_service('clickhouse')
    start.assert_not_called()


def test_foreign_workspace_owner_is_not_overwritten(tmp_path, monkeypatch):
    from openalex_api import workspace
    monkeypatch.setattr(workspace, 'ROOT', tmp_path)
    path = tmp_path / 'var/run/workspace-owner.json'
    path.parent.mkdir(parents=True)
    path.write_text('{"host":"another-host"}')
    before = path.read_bytes()
    with pytest.raises(RuntimeError, match='another host'):
        workspace.claim()
    assert path.read_bytes() == before


def test_community_resource_reserve_is_not_the_legacy_500_gib_floor(tmp_path, monkeypatch):
    from openalex_api import resources, manage
    monkeypatch.setattr(settings, 'config', lambda: dict(execution_profile='community', min_free_gib=1, memory_gib=8))
    monkeypatch.setattr(resources, 'generation_path', lambda _: tmp_path)
    monkeypatch.setattr(manage, 'resource_status', lambda: dict(disk_free_bytes=10 * 1024**3, total_rss_bytes=1024**3))
    assert resources.checkpoint('example')['disk_free_bytes'] == 10 * 1024**3
    monkeypatch.setattr(manage, 'resource_status', lambda: dict(disk_free_bytes=100, total_rss_bytes=1024**3))
    with pytest.raises(resources.Paused, match='disk-reserve'):
        resources.checkpoint('example')


def test_pinned_engine_hash_is_checked_before_extraction(tmp_path, monkeypatch):
    from openalex_api import engines
    cache = tmp_path / 'cache'
    cache.mkdir()
    (cache / 'engine.tgz').write_bytes(b'wrong archive')
    resource = tmp_path / 'resources'
    resource.mkdir()
    (resource / 'platform-engines.json').write_text(json.dumps({'amd64': {'clickhouse': dict(
        directory='clickhouse-compat', filename='engine.tgz', algorithm='sha256', digest='wrong', kind='tgz')}}))
    monkeypatch.setattr(engines, 'ROOT', tmp_path / 'workspace')
    monkeypatch.setattr(engines, 'RESOURCE_ROOT', resource)
    monkeypatch.setattr(engines, 'architecture', lambda: 'amd64')
    with pytest.raises(RuntimeError, match='checksum mismatch'):
        engines.ensure(offline=True, cache=cache)
    assert not (tmp_path / 'workspace/var/engines/clickhouse-compat').exists()


def test_projection_compatibility_is_explicit_and_rejects_changed_semantics(monkeypatch):
    from openalex_api import compatibility
    entries = json.loads((settings.RESOURCE_ROOT / 'projection-compatibility.json').read_text())
    signature, entry = next(iter(entries.items()))
    value = dict(engine='manticore', engine_version='29.9.0', projection_signature=signature, physical_layout=entry['layout'])
    assert compatibility.supports(value)
    assert not compatibility.supports(value | {'projection_signature': 'unregistered'})
    monkeypatch.setattr(compatibility, 'engine_semantics', lambda _: 'changed')
    assert not compatibility.supports(value)


def test_adopted_workspace_rejects_backend_lifecycle(monkeypatch):
    from openalex_api import manage
    monkeypatch.setattr(manage, 'config', lambda: {'adopted_from': {'workspace': '/example'}})
    with pytest.raises(ValueError, match='cannot manage'):
        manage.start('clickhouse')


def test_direct_backend_start_checks_workspace_ownership(monkeypatch):
    from openalex_api import manage, workspace
    monkeypatch.setattr(manage, 'config', lambda: {'execution_profile': 'community'})
    claim = Mock(side_effect=RuntimeError('workspace belongs to another host'))
    monkeypatch.setattr(workspace, 'claim', claim)
    with pytest.raises(RuntimeError, match='another host'):
        manage.start('clickhouse')
    claim.assert_called_once()


def test_collection_admission_uses_community_budget():
    from openalex_api.resources import decide, GiB
    policy = dict(execution_profile='community', min_free_gib=1, memory_gib=8)
    assert decide(10 * GiB, GiB, policy=policy) is None
    assert decide(GiB // 2, GiB, policy=policy) == 'disk-reserve'
    assert decide(10 * GiB, 8 * GiB, policy=policy) == 'memory-budget'
    with pytest.raises(ValueError, match='negative'):
        decide(10 * GiB, GiB, additional=-1, policy=policy)


def test_default_plan_does_not_load_earth_rules(tmp_path, monkeypatch):
    snapshot = tmp_path / 'snapshot'
    make_snapshot(snapshot)
    monkeypatch.setattr(pipeline, 'config', lambda: {'snapshot': str(snapshot)})
    monkeypatch.setattr(pipeline, 'RESOURCE_ROOT', tmp_path / 'no-rules')
    plan = pipeline.plan('local_data')
    assert plan['profile'] == 'metadata'
    assert plan['stages'] == ['metadata', 'published']
    assert plan['build'] is None and plan['rule_signature'] is None


def test_metadata_ready_does_not_require_optional_search(monkeypatch):
    from openalex_api import api
    monkeypatch.setattr(api, 'generation', lambda: dict(generation='example', metadata_only=True))
    monkeypatch.setattr(api, 'availability', lambda _: dict(metadata_ready=True, search_ready=False))
    assert api.readyz().status_code == 200
    monkeypatch.setattr(api, 'generation', lambda: dict(generation='example', metadata_only=True, _search_error='broken published index'))
    assert api.readyz().status_code == 503
