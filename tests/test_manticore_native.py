import json
import os
import uuid

import pytest

from openalex_api.manticore_api import search_request
from openalex_api.manticore_engine import Client
from openalex_api.manticore_formats import create, project_native, check_indexes
from openalex_api.query import query
from openalex_api.manticore_query import request_body


@pytest.fixture
def native():
    url = os.environ.get('MANTICORE_TEST_URL')
    if not url:
        pytest.skip('isolated Manticore required')
    c = Client(url)
    name = 'oa_native_test_' + uuid.uuid4().hex[:16]
    create(c, name)
    authors = [dict(author=dict(id='A1', display_name='John Smith'), institutions=[dict(id='I1')]),
               dict(author=dict(id='A2', display_name='Alice Jones'), institutions=[dict(id='I2')])]
    records = [dict(id='W1', title='Climate change', publication_year=2020, open_access={'is_oa': False},
                    topics=[{'id':'T1'}, {'id':'T2'}], primary_topic={'id':'T1'},
                    primary_location={'source':{'id':'S9'}}, authorships=authors),
               dict(id='W2', title='Climate ocean', publication_year=2021, open_access={'is_oa':True},
                    topics=[{'id':'T2'}], locations=[{'source':{'id':'S8'}}]),
               dict(id='W3', title='Climate future'), dict(id='W4', title='Ocean')]
    c.bulk(name, [project_native('works',dict(raw=json.dumps(r),version=1),'',[]) for r in records])
    try:
        yield c, name
    finally:
        c.sql('DROP TABLE '+name)
        c.close()


def test_native_layout_semantics_and_pagination(native):
    c, name = native
    filters = ['topic:T1','topic:T1|T2','topic:!T1','source:S9','source:S8|S9','source:!S9',
               'primary_topic:T1','primary_source:S9','year:2020..2021,is_oa:false','year:null',
               'is_oa:false','is_oa:!false','is_oa:null','is_oa:!null','id:W1|W2,topic:T1',
               'id:!W1','topic:null','topic:!null','year:>2020','author:A1,institution:I2']
    bodies = [query('works',filters=f) for f in filters]
    bodies += [query('works', 'climate','title','topic:T2'),
               query('works',author_search='John Smith'),query('works',author_search='John Jones')]
    bodies += [{'query': {'bool': {'should':[{'term':{'topic_id':'T1'}},{'term':{'publication_year':2021}}],
                                 'must':[{'term':{'is_oa':False}}], 'minimum_should_match':1}}}]
    for sealed in (False, True):
        if sealed:
            c.sql('FLUSH RAMCHUNK '+name)
            assert check_indexes(c,name)['disk_chunks'] == 1
        for body in bodies:
            expected = c.search(request_body(body,[name],10))['hits']
            actual = c.search(search_request('works',body,[name],10,layout='native_v2'))['hits']
            assert actual['total'] == expected['total'], body
            assert [h['_source']['original_id'] for h in actual['hits']] == [h['_source']['original_id'] for h in expected['hits']], body
        body = query('works', filters='topic:T2',sort='id:asc')
        first=c.search(search_request('works',body,[name],1,layout='native_v2'))
        second=c.search(search_request('works',body,[name],1,scroll=first['scroll'],layout='native_v2'))
        assert first['hits']['total']==2
        assert [h['_source']['original_id'] for h in first['hits']['hits']+second['hits']['hits']]==['W1','W2']
        optimized=search_request('works',query('works',filters='year:2020,is_oa:false,topic:T1'),[name],10,layout='native_v2')
        assert 'oa_predicate' not in optimized['expressions']


def test_catalog_with_empty_work_attributes_has_indexes(native):
    c,name=native
    c.bulk(name,[project_native('sources',dict(raw=json.dumps({'id':'S9','display_name':'Geology'}),version=1),'',[])])
    c.sql('FLUSH RAMCHUNK '+name)
    check_indexes(c,name)


def test_layout_signature_preserves_existing_artifacts():
    from openalex_api.manticore_build import signature
    assert signature() == '36a680859cdf6e146ea4453fbda31227c1167db8beb5d8a9113252cc5053fbed'
    assert signature('native_v2') != signature()


@pytest.mark.parametrize('change', ['none', 'hash', 'duplicate', 'missing'])
def test_rebuild_receipts_require_exact_source_members(tmp_path, monkeypatch, change):
    import gzip
    from openalex_api import manticore_membership as members
    from openalex_api.manticore_build import file_hash
    monkeypatch.setattr(members,'build_path',lambda g,b:tmp_path/b)
    monkeypatch.setattr(members,'runtime_root',lambda:tmp_path)
    rows=[dict(original_id='W1',source_version='1',source_hash='hash'),
          dict(original_id='W2',source_version='1',source_hash='hash2')]
    def manifest(name, rows):
        root=tmp_path/name;root.mkdir()
        receipt=root/'receipt.gz';receipt.write_bytes(gzip.compress(json.dumps(rows).encode()))
        return dict(build=name,entities={'works':{'count':2}},segments=[dict(entity='works',batches=[
                    dict(path='receipt.gz',sha256=file_hash(receipt))])])
    source=manifest('source',rows)
    if change=='hash': rows=[rows[0]|{'source_hash':'changed'},rows[1]]
    elif change=='duplicate': rows=[rows[0],rows[0]]
    elif change=='missing': rows=rows[:1]
    target=manifest('target',rows)
    if change=='none': assert members.reconcile('g',source,target)['records']==2
    else:
        with pytest.raises(RuntimeError,match='rebuild'):
            members.reconcile('g',source,target)


@pytest.mark.parametrize('fail_smoke', [False,True])
def test_publication_smoke_and_rollback(tmp_path,monkeypatch,fail_smoke):
    from openalex_api import manticore_qualification as q
    from openalex_api.config import atomic_json
    import copy
    monkeypatch.setattr(q,'build_path',lambda g,b:tmp_path/b)
    monkeypatch.setattr(q,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(q,'config',lambda:{'manticore_observation_seconds':0})
    monkeypatch.setattr(q,'signature',lambda *a:'sig')
    base=dict(generation='g',state='validated',projection_signature='sig',metadata_signature='m',collection_signature='c',segments=[])
    monkeypatch.setattr(q,'read',lambda g,b:base|{'build':b})
    atomic_json(tmp_path/'new'/'observation.json',dict(state='complete',errors=0,elapsed_seconds=1,
                long_observation_skipped=True,required_seconds=0,projection_signature='sig'))
    atomic_json(tmp_path/'new'/'qualification/acceptance.json',dict(passed=True,performance_passed=True,code={}))
    pointer=tmp_path/'search-active.json'
    previous=dict(build='old',engine='manticore',published=123)
    atomic_json(pointer,previous)
    cursor={'build':'old'}
    monkeypatch.setattr(q,'capture_cursor',lambda p:cursor if p==previous else None)
    calls=[]
    def smoke(g,m,old):
        assert json.loads(pointer.read_text())['build']=='new'
        assert old==cursor
        calls.append('smoke')
        if fail_smoke: raise RuntimeError('injected smoke failure')
    monkeypatch.setattr(q,'smoke',smoke)
    if fail_smoke:
        with pytest.raises(RuntimeError,match='injected'):
            q.publish('g','new')
        assert json.loads(pointer.read_text())==previous
        assert not (tmp_path/'new/published.json').exists()
    else:
        q.publish('g','new')
        assert json.loads(pointer.read_text())['previous']==previous
        assert (tmp_path/'new/published.json').exists()
    assert calls==['smoke']
    assert json.loads((tmp_path/'new/publication.json').read_text())['state']==('rolled_back' if fail_smoke else 'published')


def test_continuation_waits_for_publication_and_starts_independent_mixed_test(tmp_path,monkeypatch):
    from openalex_api import manticore_pipeline as p, manage
    monkeypatch.setattr(p,'build_path',lambda g,b:tmp_path/b)
    parent=dict(build='earth_20m_mc_v3',physical_layout='native_v2',entities={'works':{'count':20000000}},earth_expected=76850149)
    monkeypatch.setattr(p,'parent_ready',lambda *a,**k:False)
    with pytest.raises(RuntimeError,match='published parent'):p.continue_earth('g',parent)
    calls=[]
    monkeypatch.setattr(p,'parent_ready',lambda *a,**k:True)
    monkeypatch.setattr(manage,'start',lambda *a:calls.append(a))
    assert p.continue_earth('g',parent)=='earth_full_mc_v3'
    assert calls[0][0]=='build-manticore-earth_full_mc_v3'
    assert calls[1][1]==['mixed','g','earth_20m_mc_v3','--child','earth_full_mc_v3']


@pytest.mark.parametrize('serving_passes', [False, True])
def test_mixed_failure_pauses_before_serving_retest(tmp_path,monkeypatch,serving_passes):
    from openalex_api import manticore_pipeline as p, earth_delivery, search_versions
    from openalex_api.config import atomic_json
    monkeypatch.setattr(p,'build_path',lambda g,b:tmp_path/b)
    monkeypatch.setattr(p,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(p,'config',lambda:{'port':8088,'api_keys':['test']})
    parent=dict(build='new',entities={'works':{'count':20}},state='validated')
    monkeypatch.setattr(search_versions,'read',lambda g,b:parent if b=='new' else {'state':'validated'})
    atomic_json(tmp_path/'new/qualification/cases.json',dict(cases=[],records={}))
    atomic_json(tmp_path/'child/progress.json',dict(state='building',entities={'works':{'count':21}}))
    atomic_json(tmp_path/'search-active.json',dict(build='new'))
    atomic_json(tmp_path/'new/publication.json',dict(previous={'build':'old'}))
    calls=[]
    async def load(*args):
        calls.append(True)
        passed=serving_passes and len(calls)>1
        if len(calls)>1:
            assert (tmp_path/'child/pause.json').exists()
        return dict(errors=0 if passed else 1,requests=1200,minimum_samples_passed=True,
                    p95_seconds=1,p99_seconds=1,by_kind={'real-title':dict(p95_seconds=1,p99_seconds=1)})
    monkeypatch.setattr(earth_delivery,'load_cases',load)
    def sleep(seconds):
        assert (tmp_path/'child/pause.json').exists()
        atomic_json(tmp_path/'child/progress.json',dict(state='paused_by_mixed_load',entities={'works':{'count':21}}))
    from openalex_api import manage
    monkeypatch.setattr(manage,'identity',lambda *a,**k:True)
    monkeypatch.setattr(p.time,'sleep',sleep)
    p.mixed_check('g','new','child')
    assert len(calls)==2
    assert json.loads((tmp_path/'search-active.json').read_text())['build']==('new' if serving_passes else 'old')
