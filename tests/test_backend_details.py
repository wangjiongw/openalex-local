import json

import pytest

from openalex_api import config, official, recovery
from openalex_api.backend import Backend, BackendError


@pytest.mark.parametrize('state,final', [
    ('audited', False), ('mutable', True), ('stale', True),
    ('count_changed', True), ('unpublished', True),
])
def test_details_uses_frozen_uniqueness_proof(tmp_path, monkeypatch, state, final):
    monkeypatch.setattr(config, 'generation_path', lambda g: tmp_path)
    monkeypatch.setattr(official, 'generation_path', lambda g: tmp_path)
    if state != 'unpublished':
        config.atomic_json(tmp_path/'generation.json', dict(generation='test', metadata_frozen=state!='mutable'))
    config.atomic_json(tmp_path/'metadata-release.json', dict(passed=True, metadata_signature='sig',
        entities={'works': {'unique_entities': 3}}))
    monkeypatch.setattr(recovery, 'require_evidence', lambda *a: {'signature': 'stale' if state=='stale' else 'sig'})
    record = {'id': 'W1', 'future_field': {'preserved': True}}
    queries = []
    def ch(sql):
        queries.append(sql)
        return [{'n': 4 if state=='count_changed' else 3}] if 'system.parts' in sql else [{'id': 'W1', 'raw': json.dumps(record)}]
    b = Backend.__new__(Backend)
    b.ch = ch
    assert b.details('test', 'works', ['W1', 'W9']) == {'W1': record}
    assert ('entities FINAL' in queries[-1]) is final
    assert "entity='works'" in queries[-1]
    assert b.details('test', 'works', []) == {}


def test_details_rejects_duplicate_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'generation_path', lambda g: tmp_path)
    b = Backend.__new__(Backend)
    b.ch = lambda sql: [{'id': 'W1', 'raw': '{"id":"W1"}'}]*2
    with pytest.raises(BackendError, match='duplicate metadata IDs'):
        b.details('test', 'works', ['W1'])
