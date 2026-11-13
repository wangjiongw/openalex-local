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
