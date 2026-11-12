from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from openalex_api import api, directories, official
from openalex_api.config import atomic_json
from openalex_api.query import query
from openalex_api.exports import prepare


@pytest.fixture
def directory_backend(tmp_path, monkeypatch):
    cfg = dict(cursor_secret='directory-test-secret', cursor_ttl=600, auth_required=False, qps=0, concurrency=0, query_timeout=15)
    meta = dict(generation='test_directories', metadata_only=True, metadata_frozen=True)
    records = {'fields/1': dict(id='https://openalex.org/fields/1', display_name='Earth Science'),
               'fields/2': dict(id='https://openalex.org/fields/2', display_name='Earth Systems')}
    b = Mock()
    b.details.side_effect = lambda g, e, keys: {k: records[k] for k in keys}
    monkeypatch.setattr(official, 'config', lambda: cfg)
    monkeypatch.setattr(official, 'read_generation', lambda g: meta)
    monkeypatch.setattr(official, 'generation_path', lambda g: tmp_path)
    monkeypatch.setattr(official, 'source_table', lambda *args: 'metadata.entities')
    monkeypatch.setattr(api, 'config', lambda: cfg)
    monkeypatch.setattr(api, 'generation', lambda: meta.copy())
    monkeypatch.setattr(api, 'backend', lambda: b)
    atomic_json(tmp_path/'metadata-release.json', dict(entities={'fields': dict(unique_entities=99)}))
    return b, meta


def test_directory_filtered_counts_cursor_and_query_binding(directory_backend):
    b, meta = directory_backend
    b.ch.side_effect = [[{'n': 2}], [{'id': 'fields/1', 's0': 'fields/1'}, {'id': 'fields/2', 's0': 'fields/2'}],
                        [{'n': 2}], [{'id': 'fields/2', 's0': 'fields/2'}]]
    params = dict(search='Earth', filter='domain.id:domains/3', cursor='*', per_page=1)
    first = official.execute(b, meta, 'fields', params)
    assert first['meta']['count'] == 2  # Do not use the unfiltered frozen receipt count.
    token = first['meta']['next_cursor']
    second = official.execute(b, meta, 'fields', params | dict(cursor=token))
    assert second['results'][0]['id'].endswith('fields/2')
    assert second['meta']['next_cursor'] is None
    assert "id>'fields/1'" in b.ch.call_args_list[-1].args[0]
    for call in b.ch.call_args_list:
        assert 'positionCaseInsensitiveUTF8' in call.args[0]
        assert 'domains/3' in call.args[0]
    with pytest.raises(ValueError, match='different query'):
        official.execute(b, meta, 'fields', params | dict(search='different', cursor=token))


def test_directory_literal_name_and_unsupported_text_syntax():
    value = "x' OR 1=1 --"
    assert "x\\' OR 1=1 --" in directories.name_predicate(value)
    assert directories.name_terms({}, [('display_name.search', '"Earth, Science"')]) == ['Earth, Science']
    with pytest.raises(ValueError, match='literal'):
        directories.name_terms({'search.title': 'Earth'}, [])
    with pytest.raises(ValueError, match='2000'):
        directories.name_terms({'search': 'a' * 2001}, [])


def test_both_api_contracts_search_without_a_text_index(directory_backend):
    b, _ = directory_backend
    b.ch.side_effect = lambda sql: [{'n': 0}] if 'count()' in sql else []
    with TestClient(api.app) as client:
        for path in ['/v1/fields?search=Earth', '/fields?filter=display_name.search:Earth', '/fields?search=Earth']:
            response = client.get(path)
            assert response.status_code == 200, response.text
            assert response.json()['meta']['count'] == 0
        assert client.get('/v1/fields?search_field=abstract&search=Earth').status_code == 422


def test_navigation_work_filters_keep_export_semantics(monkeypatch):
    from openalex_api import manticore_api
    filters = 'domain:domains/3,field:fields/19,subfield:subfields/1901,country:US,license:cc-by'
    body = query('works', filters=filters)
    meta = dict(generation='test_directories', _search_build=dict(engine='manticore', build='fixture', physical_layout='native_v2'))
    monkeypatch.setattr(manticore_api, 'scope', lambda *args: [dict(index='fixture')])
    plan = prepare(dict(query=dict(route='/v1/works', params=dict(filter=filters))), meta)
    assert plan['body'] == body
    assert plan['query']['params']['filter'] == filters
    assert plan['search_build'] == 'fixture'
    request = manticore_api.search_request('works', plan['body'], ['fixture'], 5, layout='native_v2')
    assert 'domains/3' in str(request) and 'fields/19' in str(request) and 'subfields/1901' in str(request)
