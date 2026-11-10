import copy
import json
import os
import time
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from openalex_api import search_scopes as scopes, search_versions as versions, search_projection, api
from openalex_api.config import config, ROOT


def topic(n, field, name='topic', subfield=None):
    return dict(id='https://openalex.org/T'+str(n),display_name=name,
                field=dict(id='https://openalex.org/fields/'+str(field)),
                subfield=dict(id='https://openalex.org/subfields/'+str(subfield or 0)))


def test_scope_rules_include_secondary_primary_soil_and_cross_discipline():
    ts=[topic(1,19),topic(2,23),topic(3,11,'Soil Carbon',1111),topic(4,17,'Seismology'),topic(5,27,'Cancer biology')]
    rules=scopes.topic_rules(ts)
    assert [r['id'] for r in rules]==['T1','T2','T3','T4']
    ids={r['id'] for r in rules}
    assert scopes.classify(dict(topics=[ts[4],ts[0]]),ids)==([19,27],scopes.EARTH)
    assert scopes.classify(dict(primary_topic=ts[3]),ids)==([17],scopes.EARTH)
    assert scopes.classify(dict(topics=[ts[4],topic(6,11)]),ids)==([11,27],'field_11')
    assert scopes.classify({},ids)==([],'unclassified')


def test_scope_manifest_selects_unique_physical_blocks_and_keeps_global_incomplete():
    manifest=dict(incremental=True,indexes={'works':'earth,agriculture'},ready_scopes=[scopes.EARTH,'field_11'],segments=[
        dict(entity='works',owner=scopes.EARTH,index='earth',count=10),
        dict(entity='works',owner='field_11',index='agriculture',count=20)])
    meta={'generation':'test_scope','_search_build':manifest}
    assert scopes.scope_info(meta)['indexes']=='earth,agriculture'
    assert scopes.scope_info(meta,scope=scopes.EARTH)['indexes']=='earth'
    assert scopes.scope_info(meta,scope='field_11')['scope_filter']=={'term':{'scope_field_ids':'fields/11'}}
    assert not scopes.scope_info(meta)['global_search_complete']
    with pytest.raises(ValueError):scopes.scope_info(meta,scope='../bad')
    with pytest.raises(Exception) as e:scopes.scope_info(meta,scope='field_12')
    assert e.value.status_code==503
    with pytest.raises(Exception) as e:api.require_search(meta,'authors')
    assert e.value.status_code==503


def test_incremental_log_scanner_detects_partial_append_truncation_rotation(tmp_path):
    from openalex_api.storage import StorageLogMonitor
    p=tmp_path/'engine.log';p.write_text('normal\n')
    monitor=StorageLogMonitor(tmp_path);monitor.check()
    with p.open('a') as f:f.write('arrayindexoutof')
    monitor.check()
    with p.open('a') as f:f.write('boundsexception\n')
    with pytest.raises(RuntimeError):monitor.check()
    p.write_text('checksum failed\n')
    with pytest.raises(RuntimeError):monitor.check()
    p.unlink();p.write_text('mergeexception\n')
    with pytest.raises(RuntimeError):monitor.check()


def test_parallel_bulk_keeps_candidate_manifest_in_worker_context(tmp_path,monkeypatch):
    from openalex_api.backend import index
    monkeypatch.setattr(versions,'generation_path',lambda g:tmp_path)
    b=Mock();b.cfg={'search_bulk_concurrency':4}
    observed=[]
    b.bulk.side_effect=lambda g,e,rows:observed.append((index(g,e),[r[0] for r in rows]))
    rows=[dict(id='W'+str(n),version=1,raw=json.dumps(dict(id='W'+str(n),title='one'))) for n in range(8)]
    docs=[dict(_id=r['id'],_version=1,found=True,fields={'projection_hash':[search_projection.project('works',json.loads(r['raw']))['projection_hash']]}) for r in rows]
    monkeypatch.setattr(versions,'mget',lambda *a:docs)
    versions.write_documents(b,'test_scope',{'indexes':{'works':'candidate'}},'works',rows)
    assert len(observed)==4 and {x[0] for x in observed}=={'candidate'}
    assert sorted(k for _,keys in observed for k in keys)==sorted(r['id'] for r in rows)


def test_partial_indexes_never_route_global_metadata_filters(monkeypatch):
    from openalex_api import official
    from openalex_api.config import config
    g='test_scope_route'
    meta=dict(generation=g,metadata_only=True,_search_build=dict(incremental=True,indexes={'works':'earth'},entities={'works':{'count':1}}))
    monkeypatch.setattr(official,'read_generation',lambda g:{'expected':{'works':{'records':3}}})
    monkeypatch.setattr(official,'source_table',lambda *a:'fixture.entities')
    b=Mock()
    def execute(q):
        assert 'fixture.entities' in q
        if 'count()' in q:return [{'n':3}]
        return []
    b.ch.side_effect=execute
    b.details.return_value={}
    result=official.execute(b,meta,'works',{'filter':'publication_year:2020'})
    assert result['meta']['count']==3
    b.os.assert_not_called()


@pytest.mark.skipif(os.environ.get('OA_LIVE_TESTS')!='1',reason='isolated actual engine required')
def test_actual_engine_capture_permission_and_original_merge_error():
    from openalex_api.backend import Backend
    b=Backend(writer=True)
    name='oa-capture-injection-'+str(time.time_ns())
    capture=ROOT/'var/log/opensearch/merge-failures'
    try:
        b.os('PUT','/'+name,{'settings':{'number_of_shards':1,'number_of_replicas':0},'mappings':{'properties':{'value':{'type':'keyword'}}}})
        for n in range(3):b.os('PUT','/'+name+'/_doc/'+str(n)+'?refresh=true',{'value':str(n)})
        capture.mkdir(parents=True,exist_ok=True)
        (capture/'inject-once').write_text('test only')
        try:b.os('POST','/'+name+'/_forcemerge?max_num_segments=1')
        except Exception:pass
        deadline=time.monotonic()+30
        captured=[]
        while time.monotonic()<deadline:
            captured=[p for p in capture.glob('merge-*.txt') if 'OA_CAPTURE_INJECTED' in p.read_text()]
            if captured:break
            time.sleep(.2)
        assert captured,'actual merge failed to capture injected exception'
        text='\n'.join(p.read_text() for p in captured)
        assert 'numValues=' in text and 'count=' in text
        assert (capture/'agent-write-probe.txt').exists()
    finally:
        (capture/'inject-once').unlink(missing_ok=True)
        b.http.delete(b.cfg['os_url']+'/'+name)
        b.close()

@pytest.mark.skipif(os.environ.get('OA_LIVE_TESTS')!='1',reason='isolated actual backends required')
def test_real_catalog_disjoint_incremental_build_and_restore_gate(monkeypatch):
    import uuid,httpx
    from openalex_api.backend import Backend,database
    from openalex_api.config import ENTITIES,atomic_json,generation_path
    from openalex_api import search_incremental as incremental,search_storage,search_acceptance
    monkeypatch.setattr(incremental,'yield_to_queries',lambda g:None)
    g='test_scope_'+uuid.uuid4().hex[:12]
    b=Backend(writer=True)
    records=[dict(id='https://openalex.org/W1',title='Climate water study',topics=[topic(1,19)]),
             dict(id='https://openalex.org/W2',title='Soil carbon study',topics=[topic(2,11,'Soil carbon',1111),topic(3,27)]),
             dict(id='https://openalex.org/W3',title='Plant biology study',topics=[topic(4,11)])]
    entities=[dict(entity='works',id='W'+str(n+1),version=1,digest=str(n)*64,raw=json.dumps(r)) for n,r in enumerate(records)]
    entities += [dict(entity='topics',id='T'+str(n),version=1,raw=json.dumps(t)) for n,t in [(1,topic(1,19)),(2,topic(2,11,'Soil carbon',1111)),(3,topic(3,27)),(4,topic(4,11))]]
    expected={e:{'records':3 if e=='works' else 4 if e=='topics' else 0} for e in ENTITIES}
    atomic_json(generation_path(g)/'generation.json',dict(generation=g,metadata_frozen=True,metadata_only=True,mode='full',manifest_sha256='fixture',expected=expected))
    monkeypatch.setattr(versions,'require_metadata',lambda g:{'signature':'fixture'})
    monkeypatch.setattr(search_storage,'require_qualification',lambda g:dict(path='fixture',catalog_signature=scopes.load(g)['signature'],samples={e:dict(bytes=1000,count=3,lucene_docs=3) for e in ENTITIES}))
    monkeypatch.setattr(search_storage,'check_runtime',lambda b:{'data_path':str(ROOT/'var/data/opensearch')})
    names=set()
    try:
        b.create(g,{},search=False);b.insert(g,'entities',entities)
        catalog=scopes.build_catalog(g)
        assert catalog['state']=='complete' and sum(r['count'] for r in catalog['owners'])==3
        assert catalog['owners']==[{'owner':'earth_sciences','count':2},{'owner':'field_11','count':1}]
        # Re-running the catalog is a read-only completed operation.
        assert scopes.build_catalog(g)['signature']==catalog['signature']
        first=incremental.build(g,'earth','earth_sciences');names.update(scopes.all_indexes(first))
        assert first['state']=='sealed' and first['entities']['works']['count']==2
        monkeypatch.setattr(api,'generation',lambda:versions.attach(json.loads((generation_path(g)/'generation.json').read_text()),'earth',allow_sealed=True))
        monkeypatch.setattr(api,'backend',lambda:b)
        with TestClient(api.app,headers={'X-API-Key':config()['api_keys'][0]}) as h:
            assert h.get('/status').json()['search_ready']
            r=h.get('/v1/works',params={'search':'study','search_scope':'earth_sciences','sort':'id:asc'})
            assert r.status_code==200,r.text
            assert r.json()['meta']['count']==2 and not r.json()['meta']['global_search_complete']
            r=h.get('/authors',params={'search':'Smith'});assert r.status_code==503
            r=h.get('/works',params={'filter':'type:null'});assert r.status_code==200 and r.json()['meta']['count']==3
        def insufficient(g,build,url):
            with httpx.Client(base_url=url,headers={'X-API-Key':config()['api_keys'][0]},timeout=20) as h:
                r=h.get('/v1/works',params={'search':'study','search_scope':'earth_sciences'})
                assert r.status_code==200,r.text
                assert r.json()['meta']['count']==2
            return dict(passed=True,fixture_only=True)
        monkeypatch.setattr(search_acceptance,'run',insufficient)
        with pytest.raises(RuntimeError,match='acceptance is incomplete'):versions.validate(g,'earth')
        first=versions.read(g,'earth')
        assert first['physical_checked'] and first['restore_checked'] and versions.active(g) is None
        monkeypatch.setattr(versions,'active',lambda _:first)
        second=incremental.build(g,'agriculture','field_11');names.update(scopes.all_indexes(second))
        assert second['entities']['works']['count']==3
        assert second['segments'][0]['index']==first['segments'][0]['index']
        assert second['segments'][0]['physical_checked']
        assert len(scopes.all_indexes(second))==2
        assert scopes.scope_info({'_search_build':second})['indexes']==second['views']['works']
        with versions.pinned(g,second):
            value=b.os('POST','/'+second['indexes']['works']+'/_search',{'query':{'match_all':{}},'size':10,'sort':[{'id':'asc'}]})
            assert [h['_id'] for h in value['hits']['hits']]==['W1','W2','W3']
    finally:
        for name in names:b.http.delete(b.cfg['os_url']+'/'+name)
        b.ch('DROP DATABASE IF EXISTS '+database(g));b.close()


def test_qualification_blocks_are_disjoint_and_updates_do_not_increase_count(monkeypatch):
    from openalex_api.search_storage import Qualification
    monkeypatch.setattr(scopes,'BLOCK_ROWS',2)
    q=object.__new__(Qualification);q.g='test_blocks';q.catalog={'signature':'fixture'}
    q.s={};q.manifest={'indexes':{'works':'unused'}};q.b=Mock();q.b.cfg={'os_url':'http://fixture'}
    q.b.http.head.return_value.status_code=404
    written=[]
    monkeypatch.setattr(versions,'write_documents',lambda b,g,m,e,rows:written.append((m['indexes'][e],[r['id'] for r in rows])))
    q.write('works',[{'id':'W1'},{'id':'W2'}]);q.write('works',[{'id':'W3'},{'id':'W4'}])
    q.write('works',[{'id':'W1'},{'id':'W2'}])
    assert [x['count'] for x in q.s['blocks']]==[2,2]
    assert q.manifest['indexes']['works']=='oa-qualify-works-0,oa-qualify-works-1'
    assert written[0][0]==written[2][0] and written[1][0]!=written[0][0]


def test_qualification_prefix_restore_requires_exact_scope_and_boundaries(monkeypatch):
    from openalex_api import search_incremental as inc, search_storage
    receipt=dict(blocks=[dict(low='',high='W2',count=2,index='qualified')],snapshot={'repository':'repo','name':'snapshot'})
    monkeypatch.setattr(search_storage,'require_qualification',lambda g:receipt)
    b=Mock();b.os.return_value={'count':2}
    segment=dict(entity='works',owner=scopes.EARTH,low='',high='W2',expected=2,index='candidate')
    assert inc.restore_prefix(b,'g',{},segment)
    assert any('/_restore' in call.args[1] for call in b.os.call_args_list)
    b.reset_mock()
    assert not inc.restore_prefix(b,'g',{},segment|{'high':'W3'})
    b.os.assert_not_called()


def test_incremental_manifest_cannot_inherit_global_complete(tmp_path,monkeypatch):
    from openalex_api.config import atomic_json
    monkeypatch.setattr(versions,'generation_path',lambda g:tmp_path/g)
    monkeypatch.setattr(versions,'signature',lambda:'fixture')
    versions.save(dict(generation='g',build='earth',state='validated',incremental=True,projection_signature='fixture',indexes={'works':'earth'}))
    atomic_json(tmp_path/'g/search-active.json',{'build':'earth'})
    atomic_json(tmp_path/'g/searches/earth/observation.json',{'state':'complete'})
    assert not versions.attach(dict(generation='g',complete=True))['complete']


def test_log_scanner_catches_fatal_unterminated_line(tmp_path):
    from openalex_api.storage import StorageLogMonitor
    (tmp_path/'engine.log').write_text('mergeexception')
    with pytest.raises(RuntimeError):StorageLogMonitor(tmp_path).check()


def test_failed_candidate_health_does_not_disable_published_indexes(monkeypatch):
    from openalex_api import manage
    monkeypatch.setattr(manage,'health',lambda name:name=='clickhouse')
    b=Mock();b.os.return_value={'status':'green'}
    monkeypatch.setattr(api,'backend',lambda:b)
    meta=dict(metadata_only=True,_search_build=dict(incremental=True,indexes={'works':'published'},views={'works':'published'}))
    result=api.availability(meta)
    assert result['metadata_ready'] and result['search_ready']
    b.os.assert_called_once_with('GET','/_cluster/health/published')


def test_scope_views_pin_earth_after_available_expands():
    manifest=dict(incremental=True,ready_scopes=['earth_sciences','field_11'],views={'works':'all-v2'},scope_views={'earth_sciences':'earth-v1'},segments=[dict(entity='works',owner='earth_sciences',index='physical1',count=2),dict(entity='works',owner='field_11',index='physical2',count=3)])
    assert scopes.scope_info({'_search_build':manifest})['indexes']=='all-v2'
    assert scopes.scope_info({'_search_build':manifest},scope='earth_sciences')['indexes']=='earth-v1'
