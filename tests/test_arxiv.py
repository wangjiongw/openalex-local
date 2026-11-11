import json
from unittest.mock import Mock

import pytest

from openalex_api import arxiv_source as source, arxiv_index as index, arxiv_pipeline as pipeline, arxiv_api, exports


def raw(key='1001.0001', year=2010):
    return dict(id=key,title='Climate forecast',abstract='A climate model',authors='A. Example',
                categories='physics.ao-ph cs.AI',doi='10.1234/example',extra={'keep':True},
                versions=[dict(version='v1',created=f'Fri, 1 Jan {year} 00:00:00 GMT')])


@pytest.mark.parametrize('value,expected',[('arxiv:2304.02948v2','2304.02948'),('https://arxiv.org/abs/hep-th/9901001v3','hep-th/9901001'),('https://arxiv.org/pdf/2304.02948.pdf','2304.02948')])
def test_identifier(value,expected):
    assert source.identifier(value)==expected
    with pytest.raises(ValueError):source.identifier('W123')


def test_jsonl_lossless_and_derived_parity(tmp_path):
    r=raw(); p=tmp_path/'snapshot.json';p.write_text(json.dumps(r)+'\n')
    assert list(source.records(p,'jsonl'))==[(1,r)]
    normal=source.normalize(r,'jsonl')
    assert normal['raw_source']['extra']=={'keep':True}
    assert normal['references_status']=='not_provided_by_source'
    assert 'referenced_works' not in normal
    derived=dict(r,arxiv_id=r['id'],first_created=r['versions'][0]['created'],latest_created=r['versions'][0]['created'],first_created_year=2010,first_version='v1',latest_version='v1')
    n=source.normalize(derived,'duckdb')
    for k in ('id','doi','title','abstract','authors','first_submitted','publication_year','categories'):assert n[k]==normal[k]
    assert n['versions'] is None and not n['provenance']['raw_available']
    with pytest.raises(ValueError):source.normalize(dict(r,versions=[]),'jsonl')
    with pytest.raises(ValueError):source.normalize(dict(derived,first_created_year=2011),'duckdb')


def test_exact_links_ambiguity_and_bibliography():
    r=source.normalize(raw(),'jsonl');b=Mock()
    b.resolve_many.return_value={'10.1234/example':['W1'],'10.48550/arxiv.1001.0001':['W2','W3']}
    b.details.return_value={'W1':dict(id='W1',doi='10.1234/example',type='article',referenced_works=['W4'])}
    result=pipeline.exact_links([r],b,'example')[0]
    assert result['preferred_openalex_id']=='W1' and result['ambiguous'][0]['openalex_ids']==['W2','W3']
    assert 'referenced_works' not in r
    b.resolve_many.return_value={}
    assert pipeline.exact_links([r],b,'example')[0]['status']=='unmatched'


def test_ingest_duplicate_conflict_resume_and_boundary(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    directory=pipeline.path('fixture');directory.mkdir(parents=True)
    p=tmp_path/'input.json';r=raw();p.write_text('\n'.join(json.dumps(v) for v in [dict(raw('0901.0001',2009),title=''),r,r]))
    m=dict(release='fixture',input_path=str(p),input_format='jsonl',min_year=2010)
    pipeline.ingest(m)
    assert (m['processed'],m['selected'],m['duplicates'])==(3,1,1)
    pipeline.ingest(m)
    assert m['selected']==1 and m['duplicates']==1
    with p.open('a') as f:f.write('\n'+json.dumps(dict(r,title='Conflict')))
    with pytest.raises(ValueError,match='conflicting'):pipeline.ingest(m)
    with pipeline.connect('fixture') as c:assert c.execute('SELECT count(*) FROM records').fetchone()[0]==1


def test_projection_and_query_validation():
    r=source.normalize(raw(),'jsonl');n,d=index.project(1,r)
    assert n==1 and d['original_id']=='arxiv:1001.0001'
    assert d['source_hash'] and d['projection_hash']
    m={'segment':{'index':'arxiv_fixture'}}
    body=index.request(m,dict(year='2010..2020',category='cs.AI',search='climate'),10)
    assert body['query']['bool']['must'][-1]=={'equals':{'any(categories)':index.category_id('cs.AI')}}
    for params in (dict(year='2020..2010'),dict(category="';DROP"),dict(search_field='topic')):
        with pytest.raises(ValueError):index.parameters(params)


def test_arxiv_csv_does_not_fabricate_oa():
    r=source.normalize(raw(),'jsonl')
    assert exports.csv_record(r,['id','authors','abstract','source','is_oa'])==['arxiv:1001.0001','A. Example','A climate model','arxiv',None]


def test_query_cursor_pins_release_and_total(monkeypatch):
    monkeypatch.setattr(index,'config',lambda:dict(cursor_secret='secret',cursor_ttl=600))
    c=Mock();c.search.return_value=dict(hits=dict(total=3,total_relation='eq',hits=[{'_id':1}]),scroll='native')
    m=dict(release='example',segment=dict(index='arxiv_example'))
    hits,cursor=index.search(c,m,{},1)
    assert cursor and hits['total']==3
    c.search.return_value=dict(hits=dict(total=2,total_relation='eq',hits=[{'_id':2},{'_id':3}]))
    hits,next_cursor=index.search(c,m,{},1,cursor)
    assert hits['total']==3 and next_cursor is None
    with pytest.raises(ValueError):index.search(c,dict(m,release='other'),{},1,cursor)


def test_export_source_plan(monkeypatch):
    m=dict(release='fixture',signature='hash',generation='demo')
    monkeypatch.setattr(arxiv_api,'current',lambda *args:m)
    p=exports.prepare(dict(source='arxiv',query=dict(route='/v1/arxiv/works',params={'search':'climate'})),dict(generation='demo'))
    assert p['source_release']=='fixture' and p['arxiv_query']['sort']=='id:asc'
    with pytest.raises(ValueError):exports.prepare(dict(source='arxiv',query=dict(route='/works',params={})),dict(generation='demo'))


def test_optional_source_status_is_fast_and_failure_isolated(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    monkeypatch.setattr(pipeline,'published',lambda: (_ for _ in ()).throw(AssertionError('absent pointer must not be retried')))
    assert pipeline.status()==dict(ready=False,error='not published')


def test_optional_duckdb_adapter(tmp_path):
    duckdb=pytest.importorskip('duckdb')
    r=raw();r.update(arxiv_id=r.pop('id'),first_created=r['versions'][0]['created'],latest_created=r['versions'][0]['created'],first_created_year=2010,first_version='v1',latest_version='v1')
    p=tmp_path/'derived.duckdb'
    c=duckdb.connect(str(p))
    c.execute('CREATE TABLE arxiv_metadata AS SELECT '+','.join('? AS '+key for key in r), list(json.dumps(v) if isinstance(v,(dict,list)) else v for v in r.values()))
    c.close()
    rows=list(source.records(p,'duckdb'))
    assert rows[0][1]['arxiv_id']=='1001.0001'
    assert source.normalize(rows[0][1],'duckdb')['provenance']['record_kind']=='derived_record'


def test_arxiv_recovery_failure_does_not_raise(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    (tmp_path/'var').mkdir()
    (tmp_path/'var/arxiv-active.json').write_text(json.dumps(dict(release='fixture')))
    monkeypatch.setattr(pipeline,'published',lambda:dict(release='fixture',segment={}))
    client=Mock()
    monkeypatch.setattr('openalex_api.manticore_engine.Client',lambda **kw:client)
    def fail(*a):raise RuntimeError('bad source snapshot')
    monkeypatch.setattr('openalex_api.manticore_build.restore',fail)
    pipeline.recover()
    state=json.loads((tmp_path/'var/run/arxiv-recovery.json').read_text())
    assert state['state']=='failed' and state['error']=='bad source snapshot'
    client.close.assert_called_once()


def test_changed_input_refuses_resume(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    monkeypatch.setattr('openalex_api.workspace.claim',lambda:None)
    p=tmp_path/'input.json';p.write_text('changed')
    directory=pipeline.path('fixture');directory.mkdir(parents=True)
    monkeypatch.setattr(pipeline,'read',lambda release:dict(release=release,state='paused',input_path=str(p),input_bytes=1,input_sha256='old'))
    with pytest.raises(ValueError,match='input changed'):pipeline.run('fixture')


def test_exact_arxiv_alias_can_differ_from_primary_doi():
    r=source.normalize(raw(),'jsonl');b=Mock()
    b.resolve_many.return_value={'10.48550/arxiv.1001.0001':['W1']}
    b.details.return_value={'W1':dict(id='W1',doi='10.9999/published',ids={'doi':'https://doi.org/10.48550/arxiv.1001.0001'},type='article')}
    result=pipeline.exact_links([r],b,'example')[0]
    assert result['confirmed'][0]['evidence']=='unique_exact_doi'
    assert result['preferred_openalex_id'] is None


def test_parallel_link_commits_ordered_checkpoints(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    monkeypatch.setattr(pipeline,'config',lambda:dict(arxiv_link_workers=2,arxiv_link_batch_rows=2,min_free_gib=0))
    reader=Mock();reader.resolve_many.return_value={}
    monkeypatch.setattr(pipeline,'Backend',lambda **kwargs:reader)
    pipeline.path('fixture').mkdir(parents=True)
    with pipeline.connect('fixture') as c:
        for i in range(5):c.execute('INSERT INTO records(id,hash,record) VALUES (?,?,?)',(str(i),'h',json.dumps(dict(id=str(i),arxiv_id='1001.000'+str(i),doi=None))))
    m=dict(release='fixture',generation='demo')
    pipeline.link(m,Mock())
    assert m['linked']==5
    with pipeline.connect('fixture') as c:
        assert pipeline.checkpoint(c,'link')==5
        assert all(json.loads(r[0])['status']=='unmatched' for r in c.execute('SELECT links FROM records ORDER BY number'))
        assert pipeline.checkpoint(c,'link-identifiers-count')==5
    pipeline.link(m,Mock())
    assert m['linked']==5


def test_cached_evidence_matches_direct_linking_and_preserves_ambiguity(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    pipeline.path('fixture').mkdir(parents=True)
    r=source.normalize(raw(),'jsonl')
    mapping={'10.1234/example':['W1'],'10.48550/arxiv.1001.0001':['W2','W3']}
    works={'W1':dict(id='W1',doi='10.1234/example',type='article',title='Published climate study')}
    b=Mock();b.resolve_many.return_value=mapping;b.details.return_value=works
    with pipeline.connect('fixture') as c:
        c.execute('CREATE TABLE link_values(value TEXT PRIMARY KEY,candidates TEXT)')
        c.execute('CREATE TABLE link_works(id TEXT PRIMARY KEY,raw TEXT)')
        c.executemany('INSERT INTO link_values VALUES (?,?)',[(k,json.dumps(v)) for k,v in mapping.items()])
        c.executemany('INSERT INTO link_works VALUES (?,?)',[(k,json.dumps(v)) for k,v in works.items()])
        cache=pipeline.LinkEvidence(c)
        assert pipeline.exact_links([r],cache,'demo')==pipeline.exact_links([r],b,'demo')
        c.execute('DELETE FROM link_works')
        with pytest.raises(RuntimeError,match='incomplete'):pipeline.exact_links([r],cache,'demo')
