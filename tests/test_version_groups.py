import json
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from openalex_api import version_groups as rules, versions_pipeline as pipeline, versions_api


def decision(source='arxiv:1001.0001', pre='W1',pub='W2',**extra):
    links=dict(confirmed=[dict(openalex_id=pre,kind='preprint',evidence='unique_exact_doi'),dict(openalex_id=pub,kind='publication',evidence='unique_exact_doi')],ambiguous=[])
    return rules.seed_decision(source,links,{pre:dict(type='preprint'),pub:dict(type='article',**extra)})


def test_confirmed_stars_not_transitive_conflicts():
    one=decision();two=decision('arxiv:1001.0002','W3','W2')
    groups,conflicts=rules.groups('fixture',[one,two])
    assert len(groups)==1 and groups[0]['members']==['W1','W2','W3'] and not conflicts
    assert groups==rules.groups('fixture',[one,two])[0]
    bad=decision('arxiv:1001.0003','W3','W4')
    groups,conflicts=rules.groups('fixture',[one,two,bad])
    assert groups==[] and len(conflicts)==2


def test_single_preprint_missing_and_notice_do_not_invent_publication():
    d=rules.seed_decision('arxiv:1001.0001',dict(confirmed=[dict(openalex_id='W1',kind='preprint')]),{'W1':{}})
    assert d['state']=='unmatched' and rules.groups('fixture',[d])==([],[])
    with pytest.raises(RuntimeError,match='missing'):decision_ = rules.seed_decision('x',dict(confirmed=[dict(openalex_id='W1',kind='publication')]),{})
    for extra in [dict(is_retracted=True),dict(title='Correction: Climate observations')]:
        d=decision(**extra)
        assert d['state']=='review' and not rules.groups('fixture',[d])[0]


def test_duplicate_doi_and_distinct_same_title_never_confirm():
    d=rules.seed_decision('arxiv:1001.0001',dict(confirmed=[],ambiguous=[dict(doi='10.0000/example',openalex_ids=['W1','W2'])]),{})
    assert d['state']=='conflict' and not rules.groups('fixture',[d])[0]
    assert not rules.groups('fixture',[])[0]


def test_work_view_retains_requested_and_actual_record_fields(monkeypatch):
    m=dict(release='fixture',generation='demo');monkeypatch.setattr(versions_api,'current',lambda:m)
    g=rules.groups('fixture',[decision()])[0][0]
    raw={'W1':dict(id='W1',referenced_works=[],cited_by_count=3),'W2':dict(id='W2',referenced_works=['W3'],cited_by_count=5)}
    b=Mock();b.details.side_effect=lambda gen,entity,keys:{k:raw[k] for k in keys if k in raw}
    b.ch.side_effect=lambda q:[dict(group_id=g['group_id'])] if '.members ' in q else [dict(payload=json.dumps(g))]
    result=versions_api.work(b,'W1')
    assert result['requested_record']==raw['W1'] and result['preferred_openalex_id']=='W2'
    assert result['records'][0]['record']['referenced_works']==[]
    b.ch.return_value=[];b.ch.side_effect=None
    assert versions_api.work(b,'W1')['state']=='unmatched'
    raw['W1']['is_retracted']=True
    assert versions_api.work(b,'W1')['automatic_display_allowed'] is False
    with pytest.raises(HTTPException) as e:versions_api.work(b,'W999')
    assert e.value.status_code==404
    with pytest.raises(ValueError):versions_api.group(b,"x' OR 1=1")


def test_optional_module_absence(monkeypatch,tmp_path):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    assert pipeline.status()['ready'] is False
    with pytest.raises(HTTPException) as e:versions_api.current()
    assert e.value.status_code==503


def test_checkpoint_replay_and_input_identity(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    d=pipeline.path('fixture');d.mkdir(parents=True)
    with pipeline.connect('fixture') as c:
        c.execute('INSERT INTO works(id) VALUES (?)',('W1',));pipeline.commit(c,'targets','W1')
    with pipeline.connect('fixture') as c:
        assert pipeline.checkpoint(c,'targets')=='W1'
        assert c.execute('SELECT id FROM works').fetchall()==[('W1',)]
    m=dict(release='fixture',signature=pipeline.signature());pipeline.save(m)
    assert pipeline.read('fixture')['release']=='fixture'
    monkeypatch.setattr(pipeline,'signature',lambda:'changed')
    with pytest.raises(RuntimeError,match='rules changed'):pipeline.read('fixture')


def test_stop_does_not_destroy_committed_state(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path);monkeypatch.setattr(pipeline,'config',lambda:{'min_free_gib':0})
    d=pipeline.path('fixture');d.mkdir(parents=True);(d/'stop.json').write_text('{}')
    with pytest.raises(InterruptedError):pipeline.protect(dict(release='fixture'))
    (d/'stop.json').unlink();pipeline.protect(dict(release='fixture'))


def test_store_replay_and_complete_reconciliation(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path);monkeypatch.setattr(pipeline,'protect',lambda m:None)
    pipeline.path('fixture').mkdir(parents=True);m=dict(release='fixture')
    with pipeline.connect('fixture') as c:
        c.execute('INSERT INTO groups VALUES (?,?)',('g','{}'));c.commit()
    b=Mock();b.ch.side_effect=lambda q,*args:[dict(id='g',payload='{}')] if 'SELECT id,payload' in q else []
    pipeline.store(m,b);count=b.ch.call_count
    pipeline.store(m,b)
    assert b.ch.call_count==count+4  # only idempotent DDL, no acknowledged rows rewritten


def test_stratified_sample_is_deterministic_and_pinned(tmp_path,monkeypatch):
    import gzip
    from openalex_api import search_versions
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    monkeypatch.setattr(pipeline,'protect',lambda m:None)
    monkeypatch.setattr(search_versions,'build_path',lambda *a:tmp_path)
    segments=[]
    for group in pipeline.GROUPS:
        rows=[dict(original_id='W'+str(i+1+len(segments)*10),source_hash=str(i)) for i in range(4)]
        p=tmp_path/(group+'.gz');p.write_bytes(gzip.compress(json.dumps(rows).encode()))
        segments.append(dict(entity='works',group=group,count=4,batches=[dict(path=p.name,count=4,sha256=pipeline.file_hash(p))]))
    monkeypatch.setattr(pipeline,'inputs',lambda m:(None,dict(segments=segments)))
    samples=[]
    for release in ['one','two']:
        pipeline.path(release).mkdir(parents=True)
        m=dict(release=release,generation='demo',search_build='fixture',seed=42,quotas={g:2 for g in pipeline.GROUPS},counts={})
        pipeline.sample(m,None)
        with pipeline.connect(release) as c:samples.append(c.execute('SELECT * FROM sample ORDER BY id').fetchall())
        pipeline.sample(m,None)  # committed strata are not resampled on replay
        assert len(samples[-1])==6
    assert samples[0]==samples[1]


def test_seed_batch_replay_uses_all_confirmed_targets(tmp_path,monkeypatch):
    from openalex_api import arxiv_pipeline,arxiv_source
    monkeypatch.setattr(pipeline,'ROOT',tmp_path);monkeypatch.setattr(arxiv_pipeline,'ROOT',tmp_path)
    monkeypatch.setattr(pipeline,'protect',lambda m:None)
    arxiv_pipeline.path('source').mkdir(parents=True);pipeline.path('fixture').mkdir(parents=True)
    r=arxiv_source.normalize(dict(id='1001.0001',title='Synthetic climate paper',authors='Example',abstract='A study',categories='physics.ao-ph',doi='10.0000/publication',versions=[dict(version='v1',created='Fri, 1 Jan 2010 00:00:00 GMT')]),'jsonl')
    works={'W1':dict(id='W1',doi='10.48550/arxiv.1001.0001',type='preprint',title=r['title']),
           'W2':dict(id='W2',doi=r['doi'],type='article',title=r['title'])}
    b=Mock();b.resolve_many.return_value={works[k]['doi']:[k] for k in works};b.details.return_value=works
    links=arxiv_pipeline.exact_links([r],b,'demo')[0]
    with arxiv_pipeline.connect('source') as c:
        c.execute('CREATE TABLE link_values(value TEXT PRIMARY KEY,candidates TEXT)')
        c.execute('CREATE TABLE link_works(id TEXT PRIMARY KEY,raw TEXT)')
        c.executemany('INSERT INTO link_values VALUES (?,?)',[(works[k]['doi'],json.dumps([k])) for k in works])
        c.executemany('INSERT INTO link_works VALUES (?,?)',[(k,json.dumps(raw)) for k,raw in works.items()])
        c.execute('INSERT INTO records(id,hash,record,links) VALUES (?,?,?,?)',(r['id'],'hash',json.dumps(r),json.dumps(links)));c.commit()
    m=dict(release='fixture',generation='demo',counts={});src=arxiv_pipeline.path('source')/'staging.sqlite'
    pipeline.seed_batches(m,src);pipeline.seed_batches(m,src)
    with pipeline.connect('fixture') as c:
        assert c.execute('SELECT count(*) FROM decisions').fetchone()[0]==1
        assert c.execute('SELECT id FROM works ORDER BY id').fetchall()==[('W1',),('W2',)]
        assert pipeline.checkpoint(c,'seeds')==1


def test_pilot_duplicate_doi_remains_candidate(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path);monkeypatch.setattr(pipeline,'protect',lambda m:None)
    pipeline.path('fixture').mkdir(parents=True)
    with pipeline.connect('fixture') as c:
        c.execute('INSERT INTO works(id,raw) VALUES (?,?)',('W1',json.dumps(dict(doi='10.0000/repeated',title='Same title',type='article'))))
        c.execute('INSERT INTO sample(id,stratum) VALUES (?,?)',('W1','core'));c.commit()
    b=Mock();b.resolve_many.return_value={'10.0000/repeated':['W1','W2']}
    pipeline.pilot(dict(release='fixture',generation='demo',counts={}),b)
    with pipeline.connect('fixture') as c:
        r=json.loads(c.execute('SELECT payload FROM sample').fetchone()[0])
        assert r['state']=='candidate' and r['automatic_confirmation'] is False
        assert r['duplicate_doi_candidates']==['W2']


def test_lineage_keeps_previous_release_readonly(tmp_path,monkeypatch):
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    for release in ['one','two']:pipeline.path(release).mkdir(parents=True)
    with pipeline.connect('one') as c:
        c.execute('INSERT INTO groups VALUES (?,?)',('old',json.dumps(dict(members=['W1','W2']))));c.commit()
    with pipeline.connect('two') as c:
        c.execute('INSERT INTO members VALUES (?,?)',('W1','new'));c.commit()
    old=pipeline.file_hash(pipeline.path('one')/'staging.sqlite')
    pipeline.lineage(dict(release='two'),dict(release='one'))
    r=json.loads((pipeline.path('two')/'lineage.json').read_text())
    assert r['mappings'][0]['current_group_ids']==['new']
    assert pipeline.file_hash(pipeline.path('one')/'staging.sqlite')==old
