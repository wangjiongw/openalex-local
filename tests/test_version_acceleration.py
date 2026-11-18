import copy
import json
import re
from unittest.mock import Mock

import pytest

from openalex_api import versions_full as f, version_scoring as scoring, version_resume as resume
from openalex_api import version_matching as rules
from openalex_api.version_group_batch import write_stars


def feature(key,kind='article',title='Global high resolution weather forecasting',year=2020):
    return dict(id=key,kind=kind,title=title,normalized=rules.normalize_title(title),year=year,
                authors=['A1','A2'],orcids=[],names=[],identifiers=[],blocked=False,stratum='core',source_hash=key)


def task():
    source={'W1':feature('W1','preprint'),'W3':feature('W3','preprint',year=0)}
    found={'W2':feature('W2'),'W4':feature('W4',title='Global high resolution weather forecasting 2')}
    selected={k:['W2','W4'] for k in source};methods={(k,v):['normalized_title'] for k in source for v in selected[k]}
    return source,selected,found,methods,set(),{}


def test_spawn_scoring_is_exactly_equal_and_keeps_conflicts():
    args=task();serial=scoring.score_batch(*args)
    with scoring.pool(2) as pool:
        parallel=scoring.score_batch(*args,executor=pool)
    assert serial==parallel
    rows=[json.loads(r['payload']) for r in serial[0]]
    good=next(r for r in rows if r['work_id']=='W1' and r['target_id']=='W2')
    assert good['evidence']['exact_title'] and good['unique_anchor']
    assert any('numeric_conflict' in r['evidence']['conflicts'] for r in rows)
    assert any('missing_year' in r['evidence']['conflicts'] for r in rows)
    overflow=task();overflow[-2].add('W1')
    assert all(not r['unique_anchor'] for r in map(lambda x:json.loads(x['payload']),scoring.score_batch(*overflow)[0]) if r['work_id']=='W1')


def test_scoring_worker_failure_is_not_swallowed():
    args=task();del args[0]['W1']['year']
    with scoring.pool(2) as pool:
        with pytest.raises(KeyError):scoring.score_batch(*args,executor=pool)


def setup_group(tmp_path,monkeypatch):
    monkeypatch.setattr(f,'ROOT',tmp_path);f.path('new').mkdir(parents=True)
    monkeypatch.setattr(f,'protect',lambda m:None);monkeypatch.setattr(f,'save',lambda m:None)
    m=dict(release='new',signature='newcode',parent_signature='oldcode',group_batch_size=2,review_mode='automatic',automatic_review={'identity':'review'})
    rows={}
    def insert(b,table,values):
        for row in values:rows.setdefault(table,{})[row['id']]=copy.deepcopy(row)
    monkeypatch.setattr(f,'insert',insert)
    def query(sql):
        name=re.search(r'FROM ([\w.]+)',sql)[1];keys=re.findall(r"'([^']+)'",sql)
        return [rows[name][k] for k in keys if k in rows.get(name,{})]
    backend=Mock();backend.ch.side_effect=query
    record=dict(work_id='W1',target_id='W2',evidence={'exact_title':True})
    return m,backend,rows,{'W2':{'W1':record}},{'W1':{'W2'}},{'W1':True,'W2':True}


def test_group_readback_checkpoint_and_idempotent_resume(tmp_path,monkeypatch):
    m,b,rows,stars,seen,verified=setup_group(tmp_path,monkeypatch)
    write_stars(m,b,{'release':'parent'},stars,seen,verified)
    result=copy.deepcopy(rows);assert json.loads((f.path('new')/'groups-progress.json').read_text())['after_anchor']=='W2'
    write_stars(m,b,{'release':'parent'},stars,seen,verified)
    assert rows==result
    g=json.loads(next(iter(rows['oa_versions_new.groups'].values()))['payload'])
    assert g['members']==['W1','W2'] and g['preferred_openalex_id']=='W2'
    assert g['assessment_mode']=='automatic_rules'


def test_failed_group_reconciliation_does_not_advance_checkpoint(tmp_path,monkeypatch):
    m,b,rows,stars,seen,verified=setup_group(tmp_path,monkeypatch);original=b.ch.side_effect
    b.ch.side_effect=lambda sql:[] if 'oa_versions_new.members' in sql else original(sql)
    with pytest.raises(RuntimeError,match='reconciliation'):write_stars(m,b,{'release':'parent'},stars,seen,verified)
    assert not (f.path('new')/'groups-progress.json').exists()
    b.ch.side_effect=original
    write_stars(m,b,{'release':'parent'},stars,seen,verified)
    assert len(rows['oa_versions_new.groups'])==1


def test_conflicting_anchor_is_not_confirmed(tmp_path,monkeypatch):
    m,b,rows,stars,seen,verified=setup_group(tmp_path,monkeypatch);seen['W1'].add('W9')
    write_stars(m,b,{'release':'parent'},stars,seen,verified)
    assert not rows


def test_known_historical_producer_requires_matching_validation(tmp_path,monkeypatch):
    monkeypatch.setattr(f,'ROOT',tmp_path);p=f.path('old');p.mkdir(parents=True)
    signature='70f5b87bd21e0e86eb6c6e24552cc01f937c3f7126c8c602c4c24cb8ffbe2fd7'
    proof=dict(passed=True,signature=signature)
    m=dict(schema=2,release='old',state='published',signature=signature,validation=proof)
    (p/'release.json').write_text(json.dumps(m));(p/'validation.json').write_text(json.dumps(proof))
    assert resume.historical_read('old')==m
    (p/'validation.json').write_text(json.dumps(proof|{'passed':False}))
    with pytest.raises(RuntimeError,match='validation'):resume.historical_read('old')
    (p/'release.json').write_text(json.dumps(m|{'signature':'unknown'}))
    with pytest.raises(RuntimeError,match='unsupported'):resume.historical_read('old')


def test_migration_rejects_live_source_and_semantic_changes(tmp_path,monkeypatch):
    from openalex_api import workspace
    monkeypatch.setattr(workspace,'claim',lambda:None);monkeypatch.setattr(f,'ROOT',tmp_path)
    for name in ('old','new'):f.path(name).mkdir(parents=True)
    (f.path('old')/'release.json').write_text(json.dumps({'signature':'current'}))
    monkeypatch.setattr(f,'signature',lambda:'current')
    monkeypatch.setattr(f,'read',lambda name:dict(state='running',completed=['features','sample']) if name=='old' else {})
    with pytest.raises(ValueError,match='pause'):resume.migrate('old','new')


def test_group_parent_import_is_not_repeated_on_resume(tmp_path,monkeypatch):
    from openalex_api import version_auto_review as auto
    monkeypatch.setattr(f,'ROOT',tmp_path);f.path('new').mkdir(parents=True)
    monkeypatch.setattr(f,'inputs',lambda m:dict(release='parent'));monkeypatch.setattr(f,'tables',lambda m,b:None)
    monkeypatch.setattr(f,'protect',lambda m:None);monkeypatch.setattr(f,'save',lambda m:None)
    monkeypatch.setattr(f,'candidate_rows',lambda m,b:iter([]))
    m=dict(release='new',parent_release='parent',parent_signature='old',generation='demo',signature='code',
           feature_signature='feature',candidate_signature='candidate',confirmation_signature='policy',selected_works=0,
           group_batch_size=2,review_mode='automatic',counts=dict(pairs=0))
    m['automatic_review']=auto.prepare(m)
    b=Mock();b.ch.side_effect=lambda sql,*args:[dict(n=0)] if sql.startswith('SELECT count') else []
    f.groups(m,b)
    assert sum(call.args[0].startswith('INSERT INTO') for call in b.ch.call_args_list)==3
    b.ch.reset_mock();f.groups(m,b)
    assert not any(call.args[0].startswith('INSERT INTO') for call in b.ch.call_args_list)
    m['parent_signature']='changed'
    with pytest.raises(RuntimeError,match='parent receipt'):f.groups(m,b)


def test_optional_missing_progress_does_not_disable_associations(tmp_path,monkeypatch):
    from openalex_api import version_progress as progress
    monkeypatch.setattr(f,'ROOT',tmp_path);(tmp_path/'var').mkdir()
    (tmp_path/'var/versions-job-active.json').write_text(json.dumps(dict(release='missing',signature='code')))
    result=progress.for_status(dict(progress_summary=dict(associated_records=2)))
    assert result['associated_records']==2 and result['progress_error']


def test_progress_distinguishes_candidate_visits_from_published_coverage(tmp_path,monkeypatch):
    from openalex_api import version_progress as progress,versions_reader
    monkeypatch.setattr(f,'ROOT',tmp_path);f.path('new').mkdir(parents=True)
    (f.path('new')/'candidates-progress.json').write_text(json.dumps(dict(processed=7)))
    parent=dict(counts=dict(covered_works=10),progress_summary=dict(associated_records=2))
    monkeypatch.setattr(versions_reader,'read',lambda name:parent)
    m=dict(release='new',parent_release='old',state='running',stage='candidates',expected_works=100,counts=dict(pairs=15,overflow=1))
    result=progress.summary(m)
    assert result['associated_records']==2 and result['fully_checked_records']==10
    assert result['visited_records']==17 and result['total_candidate_records']==100
