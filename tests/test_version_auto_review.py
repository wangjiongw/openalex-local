import json
from unittest.mock import Mock

import pytest

from openalex_api import model, version_auto_review as auto, version_matching as rules, versions_full as full
from openalex_api.semantics import digest


def pair():
    title='High resolution global weather forecasting'
    raw={key:dict(id=key,title=title,type=kind,publication_year=2020,
                  authorships=[dict(author=dict(id=a,display_name=a)) for a in ('A1','A2')])
         for key,kind in [('W1','preprint'),('W2','article')]}
    features={key:rules.feature(dict(id=int(key[1:]),original_id=key,source_hash=digest(record),
              attrs=model.project('works',record)|dict(name_literal=title.lower())),'core') for key,record in raw.items()}
    a,b=features['W1'],features['W2']
    return dict(id=rules.pair_id('W1','W2'),work_id='W1',target_id='W2',anchor='W2',
                source=a,target=b,stratum='core',evidence=rules.evidence(a,b),unique_anchor=True,known_pair=False),raw


@pytest.mark.parametrize('reason',['notice','weak_author','numeric','overflow','margin','title','year','known','anchor','generic'])
def test_automatic_assessment_rejects_conflicts_and_weak_evidence(reason):
    row,_=pair();assert auto.allows(row)
    if reason in ('notice','weak_author','numeric','overflow'):row['evidence']['conflicts'].append(reason)
    elif reason=='margin':row['evidence']['candidate_margin']=.01
    elif reason=='title':row['evidence']['exact_title']=False
    elif reason=='year':row['evidence']['year_distance']=4
    elif reason=='known':row['known_pair']=True
    elif reason=='anchor':row['unique_anchor']=False
    elif reason=='generic':row['source']['normalized']='climate change'
    assert not auto.allows(row)


def manifest():
    return dict(release='new',parent_release='old',generation='demo',signature='implementation',
                feature_signature='feature',candidate_signature='candidate',confirmation_signature='automatic',
                selected_works=1,review_mode='automatic',counts=dict(pairs=1))


def test_automatic_report_does_not_claim_independent_precision():
    m=manifest();m['automatic_review']=auto.prepare(m)
    assert auto.require(m)['independent_review'] is False
    assert m['automatic_review']['precision_lower_95'] is None
    assert 'passed' not in m['automatic_review']
    m['automatic_review']['independent_review']=True
    with pytest.raises(RuntimeError,match='identity changed'):auto.require(m)


@pytest.mark.parametrize('changed_raw',[False,True])
def test_automatic_groups_verify_raw_and_record_provenance(tmp_path,monkeypatch,changed_raw):
    m=manifest();row,raw=pair();m['automatic_review']=auto.prepare(m)
    monkeypatch.setattr(full,'ROOT',tmp_path);full.path('new').mkdir(parents=True)
    monkeypatch.setattr(full,'inputs',lambda m:dict(release='old'))
    monkeypatch.setattr(full,'tables',lambda m,b:None)
    monkeypatch.setattr(full,'protect',lambda m:None)
    monkeypatch.setattr(full,'save',lambda m:None)
    monkeypatch.setattr(full,'candidate_rows',lambda m,b:iter([row]))
    writes=[];monkeypatch.setattr(full,'insert',lambda b,name,rows:writes.extend((name,r) for r in rows))
    backend=Mock();backend.ch.side_effect=lambda q,*args:[dict(n=0)] if q.startswith('SELECT count') else []
    if changed_raw:raw['W2']['title']='Changed underlying source'
    backend.details.return_value=raw
    if changed_raw:
        with pytest.raises(RuntimeError,match='raw checksum mismatch'):full.groups(m,backend)
        assert not [r for name,r in writes if name.endswith('.groups')]
        return
    full.groups(m,backend)
    groups=[json.loads(r['payload']) for name,r in writes if name.endswith('.groups')]
    assert len(groups)==1 and groups[0]['assessment_mode']=='automatic_rules'
    evidence=groups[0]['evidence'][-1]
    assert evidence['method']=='automatic_bibliographic' and evidence['independent_review'] is False
    assert m['automatic_review']['state']=='complete'
    assert m['automatic_review']['evaluated']==m['automatic_review']['eligible']==1
    assert not m.get('calibration',{}).get('passed')


def test_automatic_review_has_no_human_sample_quota(tmp_path,monkeypatch):
    m=manifest();monkeypatch.setattr(full,'ROOT',tmp_path);full.path('new').mkdir(parents=True)
    monkeypatch.setattr(full,'save',lambda m:None)
    full.review(m,Mock())
    assert auto.require(m)['state']=='ready_for_full_assessment'
    assert not (full.path('new')/'review-labels.json').exists()
