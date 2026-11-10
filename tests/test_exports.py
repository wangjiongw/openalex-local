import gzip
import json
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from openalex_api import api, exports, catalogs


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(exports, 'ROOT', tmp_path)
    monkeypatch.setattr(exports, 'config', lambda: dict(export_batch_rows=2, export_shard_rows=2, min_free_gib=0))
    exports.STOP.clear()
    backend = Mock()
    monkeypatch.setattr('openalex_api.backend.Backend', lambda **kwargs: backend)
    return tmp_path, backend


def make_job():
    return exports.create(dict(identifiers=['W1'], formats=list(exports.FORMATS)), dict(generation='example', metadata_frozen=True))


def fake_rows(plan, after, backend):
    records = [('1', dict(id='https://openalex.org/W1', doi='https://doi.org/10.1234/one', title='=unsafe', authorships=[])),
               ('2', dict(id='https://openalex.org/W2', title='Two')),
               ('3', dict(id='https://openalex.org/W3', doi='10.1234/three', title='Three'))]
    return [(k, r) for k, r in records if int(k) > int(after or 0)][:2]


def test_export_multiple_shards_and_hashes(workspace, monkeypatch):
    monkeypatch.setattr(exports, 'rows', fake_rows)
    job = make_job()
    state = exports.run(job['id'])
    assert state['state'] == 'completed' and state['exported'] == 3 and state['missing_doi'] == 1
    exports.verify(exports.path(job['id']), state)
    records = []
    for f in state['files']:
        if f['name'].endswith('.jsonl.gz') and 'missing-doi' not in f['name']:
            with gzip.open(exports.download(job['id'], f['name']), 'rt') as stream:
                records.extend(json.loads(line) for line in stream)
    assert [r['title'] for r in records] == ['=unsafe', 'Two', 'Three']
    csv_file = next(f for f in state['files'] if f['name'].endswith('.csv'))
    assert "'=unsafe" in exports.download(job['id'], csv_file['name']).read_text()
    assert exports.download(job['id'], 'manifest.json').is_file()
    with pytest.raises(FileNotFoundError):
        exports.download(job['id'], 'plan.json')
    assert exports.run(job['id']) == state


def test_cancel_resume_is_exact_and_ignores_browser_cursor(workspace, monkeypatch):
    calls = []
    def controlled(plan, after, backend):
        calls.append(after)
        result = fake_rows(plan, after, backend)
        if after is None:
            exports.control(plan['job'], 'cancel')
        return result
    monkeypatch.setattr(exports, 'rows', controlled)
    job = make_job()
    state = exports.run(job['id'])
    assert state['state'] == 'cancelled' and state['exported'] == 2
    exports.control(job['id'], 'resume')
    assert exports.run(job['id'])['exported'] == 3
    assert calls[0] is None and calls[1] == '2' and calls.count(None) == 1


def test_damaged_committed_file_stops_recovery(workspace, monkeypatch):
    monkeypatch.setattr(exports, 'rows', fake_rows)
    job = make_job()
    exports.control(job['id'], 'cancel')
    assert exports.run(job['id'])['state'] == 'cancelled'
    exports.control(job['id'], 'resume')
    def interrupted(plan, after, backend):
        batch = fake_rows(plan, after, backend)
        exports.STOP.set()
        return batch
    monkeypatch.setattr(exports, 'rows', interrupted)
    paused = exports.run(job['id'])
    assert paused['state'] == 'paused'
    (exports.path(job['id']) / paused['files'][0]['name']).write_bytes(b'corrupt')
    exports.STOP.clear()
    exports.control(job['id'], 'resume')
    result = exports.run(job['id'])
    assert result['state'] == 'failed' and 'checksum' in result['error']


def test_identifier_resolution_reports_missing_and_ambiguous(workspace):
    _, backend = workspace
    backend.resolve_many.side_effect = lambda g,e,s,values: {v: ['W2','W3'] if v.endswith('ambiguous') else ['W1'] for v in values}
    backend.details.side_effect = lambda g,e,keys: {k: dict(id='https://openalex.org/'+k) for k in keys if k != 'W99'}
    job = exports.create(dict(identifiers=['W1','10.1234/one','10.1234/ambiguous','W99','A2']), dict(generation='example', metadata_frozen=True))
    state = exports.run(job['id'])
    assert state['state'] == 'completed' and state['exported'] == 1
    resolved = json.loads((exports.path(job['id'])/'resolved.json').read_text())
    assert {r['status'] for r in resolved['unresolved']} == {'not_found','ambiguous','invalid'}


@pytest.mark.parametrize('payload', [
    {}, {'identifiers':['W1'],'formats':[{}]}, {'identifiers':['W1'], 'query':{}}, {'identifiers':['W1'], 'formats':['pdf']},
    {'identifiers':['W1'], 'select':'authors.name'}, {'identifiers':['W1'], 'csv_fields':['x.y']},
    {'query':{'route':'https://example.org','params':{}}},
    {'query':{'route':'/works','params':{'api_key':'secret'}}},
    {'query':{'route':'/works','params':{'cursor':'expired'}}},
])
def test_export_request_validation(payload):
    with pytest.raises(ValueError):
        exports.prepare(payload, dict(generation='example'))


def test_metadata_query_preserves_full_scope():
    plan = exports.prepare(dict(query=dict(route='/works', params={'filter':'publication_year:2020', 'sort':'publication_year:desc'})),dict(generation='example'))
    assert plan['kind'] == 'metadata' and plan['search_build'] is None
    assert '2020' in plan['where']


def test_specific_export_routes_precede_entity_routes(workspace, monkeypatch):
    cfg = api.config() | dict(qps=0, concurrency=0)
    monkeypatch.setattr(api,'config',lambda:cfg)
    monkeypatch.setattr(api,'generation',lambda:dict(generation='example', metadata_frozen=True))
    with TestClient(api.app, headers={'X-API-Key':cfg['api_keys'][0]}) as client:
        created = client.post('/v1/exports',json={'identifiers':['W1']})
        assert created.status_code == 202
        job = created.json()['id']
        assert client.get('/v1/exports').json()['count'] == 1
        assert client.get('/v1/exports/'+job).json()['state'] == 'queued'
        assert client.post('/v1/exports/'+job+'/cancel',json={}).status_code == 200
        assert client.get('/v1/exports/'+job+'/files/plan.json').status_code == 404
        assert client.post('/v1/exports',json={}).status_code == 422


def test_catalog_author_membership_is_unique():
    assert catalogs.author_ids([{'authorships':'[{"author_id":"A2"},{"author_id":"A1"},{"author_id":"A2"}]'}]) == ['A1','A2']
    with pytest.raises(ValueError):
        catalogs.author_ids([{'authorships':[{'author_id':'W1'}]}])


def test_new_catalog_ids_use_native_filter():
    from openalex_api.manticore_native_query import exact
    assert exact('authors',{'term':{'id':'A123'}}) == {'equals':{'id':123}}
    assert exact('institutions',{'term':{'id':'I123'}}) == {'equals':{'id':123}}


def test_supervisor_uses_selected_code_instead_of_workspace_checkout(tmp_path, monkeypatch):
    from openalex_api import manage
    from openalex_api.config import atomic_json
    monkeypatch.setattr(manage, 'ROOT', tmp_path)
    monkeypatch.setattr(manage, 'identity', lambda *args, **kwargs: None)
    monkeypatch.setattr(manage, 'config', lambda: {'execution_profile':'community'})
    state = tmp_path/'var/run/build-catalogs-example.json'
    atomic_json(state, {'exit_code':0})
    spawn = Mock(return_value=Mock(poll=lambda:0))
    monkeypatch.setattr(manage.subprocess, 'Popen', spawn)
    assert manage.start('build-catalogs-example')['completed']
    assert spawn.call_args.kwargs['cwd'] == manage.CODE_ROOT
    assert spawn.call_args.kwargs['env']['OPENALEX_HOME'] == str(tmp_path)


def test_large_stream_export_crosses_default_shard_boundary(workspace, monkeypatch):
    total = 200003
    def stream(plan, after, backend):
        start = int(after or 0) + 1
        return [(str(i), {'id':f'https://openalex.org/W{i}', 'title':f'Paper {i}'}) for i in range(start, min(start+1000,total+1))]
    monkeypatch.setattr(exports,'rows',stream)
    monkeypatch.setattr(exports,'config',lambda:dict(export_shard_rows=100000,min_free_gib=0))
    job=exports.create({'identifiers':['W1'],'formats':['ids.txt']},dict(generation='example',metadata_frozen=True))
    state=exports.run(job['id'])
    assert state['state']=='completed' and state['exported']==total and len(state['receipts'])==3
    index=1
    for f in state['files']:
        if f['name'].endswith('.ids.txt'):
            with exports.download(job['id'],f['name']).open() as stream:
                for line in stream:
                    assert line.strip()==f'https://openalex.org/W{index}'
                    index+=1
    assert index==total+1


def test_file_verification_retries_transient_missing_file(tmp_path,monkeypatch):
    file=tmp_path/'committed'
    file.write_bytes(b'content')
    original=exports.file_hash
    calls=[]
    def transient(path):
        calls.append(path)
        if len(calls)==1:raise FileNotFoundError(path)
        return original(path)
    monkeypatch.setattr(exports,'file_hash',transient)
    monkeypatch.setattr(exports.time,'sleep',lambda delay:None)
    assert exports.hash_file(file)==original(file) and len(calls)==2


def test_csv_distinguishes_invalid_missing_and_present_abstract():
    columns=['title','abstract_status','abstract']
    assert exports.csv_record({'display_name':'Fallback'},columns)==['Fallback','missing','']
    assert exports.csv_record({'abstract_inverted_index':{'word':[0]}},columns)==[None,'present','word']
    assert exports.csv_record({'abstract_inverted_index':{'word':[None]}},columns)[1]=='invalid'


def test_csv_shards_obey_byte_target_with_narrow_json_selection(workspace,monkeypatch):
    def rows(plan,after,backend):
        start=int(after or 0)+1
        return [(str(i),{'id':f'W{i}','title':'x'*200}) for i in range(start,min(start+2,5))]
    monkeypatch.setattr(exports,'rows',rows)
    monkeypatch.setattr(exports,'SHARD_BYTES',128)
    monkeypatch.setattr(exports,'config',lambda:dict(export_shard_rows=100000,min_free_gib=0))
    job=exports.create({'identifiers':['W1'],'formats':['csv'],'select':'id'},dict(generation='example',metadata_frozen=True))
    state=exports.run(job['id'])
    assert state['state']=='completed' and state['exported']==4 and len(state['receipts'])==2


def test_id_only_shards_measure_written_output_without_serializing_ignored_fields(workspace, monkeypatch):
    monkeypatch.setattr(exports, 'config', lambda: dict(export_shard_rows=100000, min_free_gib=0))
    monkeypatch.setattr(exports, 'SHARD_BYTES', 128)
    def records(plan, after, backend):
        return [(key, value | {'unused': 'x' * 4096}) for key, value in fake_rows(plan, after, backend)]
    monkeypatch.setattr(exports, 'rows', records)
    original = exports.json.dumps
    def encode(value, *args, **kwargs):
        assert 'unused' not in value, 'ID exports must not serialize ignored source fields'
        return original(value, *args, **kwargs)
    monkeypatch.setattr(exports.json, 'dumps', encode)
    job = exports.create(dict(identifiers=['W1'], formats=['ids.txt']), dict(generation='example', metadata_frozen=True))
    state = exports.run(job['id'])
    assert state['state'] == 'completed' and state['exported'] == 3 and len(state['receipts']) == 1
