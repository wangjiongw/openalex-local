import json
from unittest.mock import Mock

import pytest

from openalex_api import version_matching as r, versions_full as full, versions_reader, versions_api


def f(key='W1',kind='preprint',title='Global high resolution weather forecasting',authors=('A1','A2'),year=2020,**extra):
    return dict(id=key,kind=kind,title=title,normalized=r.normalize_title(title),authors=list(authors),orcids=[],names=[],identifiers=[],year=year,blocked=False,stratum='core',source_hash=key,**extra)


def test_full_author_identity_numeric_negation_and_roles():
    a=f();b=f('W2','article')
    assert r.proposed(r.evidence(a,b))
    assert not r.proposed(r.evidence(a,f('W2','article',authors=('A1','A3'))))
    assert r.proposed(r.evidence(f(authors=('A1',)),f('W2','article',authors=('A1',))))
    assert 'numeric_conflict' in r.evidence(f(title='Climate 1'),f('W2','article',title='Climate 2'))['conflicts']
    assert 'negation_conflict' in r.evidence(f(title='Climate without warming'),f('W2','article',title='Climate warming'))['conflicts']
    assert 'publication_duplicate_or_extension' in r.evidence(f(kind='article'),b)['conflicts']
    assert 'missing_year' in r.evidence(a,f('W2','article',year=0))['conflicts']
    assert 'year_window' in r.evidence(a,f('W2','article',year=2031))['conflicts']
    assert not r.proposed(r.evidence(a,b|dict(blocked=True)))
    assert r.pair_id('W1','W2')==r.pair_id('W2','W1')


def test_feature_preserves_all_authors_and_id_integrity():
    row=dict(id=1,original_id='W1',source_hash='hash',attrs=dict(name_literal='Climate',type='preprint',authorships=[dict(author_id='A1',author_name=['one']),dict(author_id='A2',author_name=['two'],orcid='0000')]))
    value=r.feature(row,'core')
    assert value['authors']==['A1','A2'] and value['orcids']==['0000']
    assert 't:climate' in r.blocks(value)
    with pytest.raises(ValueError):r.feature(row|dict(id=2),'core')


def test_calibration_requires_real_labels_and_independent_holdout_counts():
    e=r.evidence(f(),f('W2','article'))
    rows=[dict(id=str(i),role=role,label='same_study',reviewer='independent',evidence=e,unique_anchor=True,stratum='core') for role,n in [('development',500),('blind',600),('hard_negative',400)] for i in range(n)]
    report=r.calibrate(rows,.99,'package')
    assert report['passed'] and report['precision_lower_95']>.995 and report['allowed_strata']==['core']
    rows[501]['label']='different_study'
    assert not r.calibrate(rows,.99,'package')['passed']
    rows[501]['label']=None
    with pytest.raises(ValueError):r.calibrate(rows,.99,'package')
    with pytest.raises(ValueError):r.calibrate([],0.5,'package')
    assert not r.calibrate([],0.99,'package')['passed']


def test_schema_dispatch_preserves_legacy_signature(tmp_path,monkeypatch):
    from openalex_api import versions_pipeline as legacy
    monkeypatch.setattr(legacy,'ROOT',tmp_path)
    for name,schema in [('old',1),('new',2),('unknown',99)]:
        d=legacy.path(name);d.mkdir(parents=True);(d/'release.json').write_text(json.dumps(dict(schema=schema)))
    assert versions_reader.module('old') is legacy
    assert versions_reader.module('new') is full
    with pytest.raises(RuntimeError):versions_reader.module('unknown')


def test_candidate_cursor_pins_release_and_work(monkeypatch):
    m=dict(schema=2,release='fixture',generation='demo',calibration={})
    monkeypatch.setattr(versions_api,'current',lambda:m)
    monkeypatch.setattr(versions_reader,'published',lambda release:m if release=='fixture' else None)
    b=Mock();b.ch.side_effect=lambda q:[dict(id='W1:W2',payload='{}'),dict(id='W1:W3',payload='{}')] if '.candidates ' in q else []
    result=versions_api.candidates(b,'W1',per_page=1)
    assert result['processing_state']=='not_processed' and result['next_cursor']
    versions_api.candidates(b,'W1',cursor=result['next_cursor'])
    with pytest.raises(ValueError):versions_api.candidates(b,'W2',cursor=result['next_cursor'])
    with pytest.raises(ValueError):versions_api.candidates(b,'W1',release='other',cursor=result['next_cursor'])
    with pytest.raises(ValueError):versions_api.candidates(b,'W1',cursor='broken')


def test_no_annotation_never_adds_groups_and_parent_is_retained(monkeypatch):
    m=dict(release='new',parent_release='old',counts={})
    monkeypatch.setattr(full,'inputs',lambda m:dict(release='old'))
    monkeypatch.setattr(full,'save',lambda m:None)
    monkeypatch.setattr(full,'candidate_rows',lambda m,b:iter([dict(id='pair',stratum='core',evidence={},unique_anchor=True)]))
    writes=[];b=Mock()
    b.ch.side_effect=lambda q,*data:([dict(n=7)] if q.startswith('SELECT count') else writes.append((q,data)) or [])
    full.groups(m,b)
    assert any('INSERT INTO oa_versions_new.groups SELECT * FROM oa_versions_old.groups FINAL'==q for q,_ in writes)
    assert not any(q=='INSERT INTO oa_versions_new.groups FORMAT JSONEachRow' for q,_ in writes)
    assert any('.reviews FORMAT' in q for q,_ in writes)


def test_calibrated_pair_is_verified_against_raw_anchor(monkeypatch):
    from openalex_api.semantics import digest
    raw={'W1':dict(id='W1',title='Global high resolution weather forecasting',type='preprint'),'W2':dict(id='W2',title='Global high resolution weather forecasting',type='article')}
    a=f()
    a['source_hash']=digest(raw['W1']);bfeature=f('W2','article');bfeature['source_hash']=digest(raw['W2'])
    row=dict(id=r.pair_id('W1','W2'),work_id='W1',target_id='W2',anchor='W2',stratum='core',source=a,target=bfeature,evidence=r.evidence(a,bfeature),unique_anchor=True)
    m=dict(release='new',generation='demo',parent_release='old',counts={},review_identity='audit',calibration=dict(passed=True,allowed_strata=['core'],threshold=.99))
    monkeypatch.setattr(full,'inputs',lambda m:dict(release='old'))
    monkeypatch.setattr(full,'save',lambda m:None);monkeypatch.setattr(full,'protect',lambda m:None)
    monkeypatch.setattr(full,'candidate_rows',lambda m,b:iter([row]))
    writes=[];b=Mock();b.details.return_value=raw
    b.ch.side_effect=lambda q,*data:([dict(n=1)] if q.startswith('SELECT count') else writes.append((q,data)) or [])
    full.groups(m,b)
    grouped=[json.loads(data[0].splitlines()[0]) for q,data in writes if q=='INSERT INTO oa_versions_new.groups FORMAT JSONEachRow']
    assert json.loads(grouped[0]['payload'])['members']==['W1','W2']
    b.details.return_value=raw|{'W2':raw['W2']|dict(title='changed')}
    with pytest.raises(RuntimeError,match='checksum'):full.groups(m,b)


def test_transport_retry_is_bounded_and_semantic_errors_fail(monkeypatch):
    import httpx
    monkeypatch.setattr(full.time,'sleep',lambda seconds:None)
    c=Mock();c.rows.side_effect=[httpx.ConnectError('temporary'),[dict(id=1)]]
    assert full.native_rows(c,'query')==[dict(id=1)] and c.rows.call_count==2
    c.rows.reset_mock();c.rows.side_effect=httpx.ConnectError('persistent')
    with pytest.raises(httpx.ConnectError):full.native_rows(c,'query')
    assert c.rows.call_count==4
    c.rows.reset_mock();c.rows.side_effect=RuntimeError('checksum mismatch')
    with pytest.raises(RuntimeError):full.native_rows(c,'query')
    assert c.rows.call_count==1


def test_sample_ranks_only_ids_and_resumes_verified_hydration(tmp_path,monkeypatch):
    monkeypatch.setattr(full,'ROOT',tmp_path);monkeypatch.setattr(full,'save',lambda m:None)
    monkeypatch.setattr(full,'protect',lambda m:None);monkeypatch.setattr(full,'inputs_parent',lambda m:{'schema':1})
    full.path('fixture').mkdir(parents=True)
    m=dict(release='fixture',feature_signature='features',parent_signature='parent',seed=42,pilot_size=2,feature_database='features',counts={})
    ids=[dict(id='W1'),dict(id='W2')];rows=[v|dict(payload=json.dumps(v)) for v in ids];queries=[]
    def query(sql,*args):
        queries.append(sql)
        if 'sum(cityHash64(id))' in sql:return [dict(n=2,checksum=42)]
        if sql.startswith('SELECT id FROM '):return ids if "id>''" in sql else []
        if sql.startswith('SELECT id,payload'):return rows
        if sql.startswith('SELECT count()'):return [dict(n=2)]
        return []
    b=Mock();b.ch.side_effect=query
    full.sample(m,b)
    ranked=[q for q in queries if q.startswith('INSERT INTO oa_versions_fixture.sample_ids')]
    assert len(ranked)==1 and 'SELECT id FROM features.features FINAL ORDER BY sipHash64' in ranked[0]
    assert 'payload' not in ranked[0] and 'max_memory_usage=8589934592' in ranked[0]
    assert m['selected_works']==2 and json.loads((full.path('fixture')/'sample-progress.json').read_text())['processed']==2
    queries.clear();full.sample(m,b)
    assert not any(q.startswith('INSERT INTO oa_versions_fixture.sample_ids') for q in queries)
    assert not any(q.startswith('INSERT INTO oa_versions_fixture.sample FORMAT') for q in queries)
    (full.path('fixture')/'sample-selection.json').write_text(json.dumps(dict(identity='changed',n=2,checksum=42)))
    with pytest.raises(RuntimeError,match='identity'):full.sample(m,b)


def test_candidate_key_batches_bound_escaped_utf8_bytes_and_rows():
    from openalex_api.backend import sql_list
    values=["论文'\\"+str(i)+'气候'*500 for i in range(160)]
    batches=list(full.key_batches(values))
    assert len(batches)>1
    assert [v for batch in batches for v in batch]==sorted(set(values))
    assert all(len(sql_list(batch).encode('utf-8'))<132000 for batch in batches)
    assert len(list(full.key_batches(['W'+str(i) for i in range(full.BATCH+1)])))==2
    assert list(full.key_batches([]))==[]
    with pytest.raises(ValueError,match='byte budget'):list(full.key_batches(['x'*131073]))


def test_candidate_feature_lookup_splits_large_sets_without_losing_targets(monkeypatch):
    monkeypatch.setattr(full,'BATCH',2);b=Mock();calls=[]
    def query(sql):
        calls.append(sql)
        import re
        return [dict(id=key,payload='{}') for key in re.findall(r"'(W[1-9])'",sql)]
    b.ch.side_effect=query
    result=full.fetch_features(dict(feature_database='features'),b,['W1','W2','W3','W4','W5'])
    assert len(calls)==3 and set(result)=={'W1','W2','W3','W4','W5'}


def test_parallel_reads_preserve_query_order_and_surface_failures():
    b=Mock();b.ch.side_effect=lambda q:[dict(id=q)]
    assert list(full.read_batches(b,['q1','q2','q3'],3))==[dict(id='q1'),dict(id='q2'),dict(id='q3')]
    b.ch.side_effect=RuntimeError('read failed')
    with pytest.raises(RuntimeError,match='read failed'):list(full.read_batches(b,['query'],2))


def test_fast_candidate_filter_is_identical_to_original_full_comparison():
    titles=['Global high resolution weather forecasting','Climate change in Europe','全球气候预报','Weather','', 'a'*150+' forecast']
    for title in titles:
        for other in titles:
            for year in (0,1990,2021):
                for methods in (['author_title_block'],['shared_doi_review']):
                    a=f(title=title);b=f('W2','article',title=other,year=year)
                    expected=r.evidence(a,b,methods)
                    if 'shared_doi_review' not in methods and ((expected['year_distance'] is not None and expected['year_distance']>r.YEAR_WINDOW) or expected['title_similarity']<0.6):expected=None
                    assert full.candidate_evidence(a,b,methods)==expected
