import json

import httpx
import pytest

from openalex_api import earth_collections as ec, earth_delivery as delivery, earth_audit as audit
from openalex_api.config import atomic_json


def record(key,group):
    return dict(openalex_id='https://openalex.org/'+key,group=group,open_access={},has_doi=False,has_abstract=False,
                abstract_status='missing',authors=[],journal_matches=[],reasons=[],best_oa_location=None,primary_source=None)


def test_receipt_weighted_sampling_covers_both_phases_and_rejects_tampering(tmp_path):
    state=dict(blocks=0,phase='direct',after='',author_after='',scanned=0,author_scanned=0,
               counts={g:0 for g in ec.GROUPS},missing={g:dict(doi=0,abstract=0,oa=0,invalid_abstract=0) for g in ec.GROUPS})
    r=ec.write_block(tmp_path,state,'W2',2,iter([record('W1',ec.GROUPS[0]),record('W2',ec.GROUPS[1])]))
    state=ec.apply_receipt(state,r);state['phase']='author'
    r=ec.write_block(tmp_path,state,'W3',1,iter([record('W3',ec.GROUPS[2])]))
    state=ec.apply_receipt(state,r)
    samples=delivery.sample_exports(tmp_path,state,10)
    assert [s['record']['group'] for s in samples]==list(ec.GROUPS)
    assert delivery.sample_exports(tmp_path,state,10)==samples
    f=tmp_path/samples[0]['file'];f.write_bytes(b'broken')
    with pytest.raises(RuntimeError,match='hash'):delivery.sample_exports(tmp_path,state,10)


def test_response_checks_identity_fields_filters_graph_and_pagination():
    known={'W1':dict(id='https://openalex.org/W1',title='Climate study',doi=None,publication_year=2020,open_access={'is_oa':False})}
    case=dict(expected=dict(shape='list',ids=['W1'],year=2020,oa=False))
    response=httpx.Response(200,json={'results':[known['W1']]})
    delivery.validate_response(case,response,known)
    with pytest.raises(AssertionError):delivery.validate_response(case,httpx.Response(200,json={'results':[]}),known)
    changed=dict(known['W1'],doi='10.1/fake')
    with pytest.raises(AssertionError,match='source field'):delivery.validate_response(case,httpx.Response(200,json={'results':[changed]}),known)
    graph=dict(expected=dict(shape='graph',ids=['W1','W2'],more=False))
    delivery.validate_response(graph,httpx.Response(200,json={'meta':{'next_cursor':None},'results':[
        dict(id=known['W1']['id'],resolved=True,work=known['W1']),dict(id='https://openalex.org/W2',resolved=False,work=None)]}),known)
    with pytest.raises(AssertionError):delivery.validate_response(case,httpx.Response(503,json={}),known)


def test_pending_search_is_not_a_pass_and_unsafe_case_is_rejected(tmp_path,monkeypatch):
    fixture=dict(schema=1,generation='g',records={},cases=[dict(name='search',phase='search',path='/v1/works',params={},expected=dict(shape='search'))])
    atomic_json(tmp_path/'cases.json',fixture)
    class Client:
        def __init__(self,*a,**kw):pass
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def get(self,*a,**kw):return httpx.Response(200,request=httpx.Request('GET','http://fixture/status'),json=dict(generation='g',search_ready=False,search_build=None))
    monkeypatch.setattr(delivery.httpx,'Client',Client)
    monkeypatch.setattr(delivery.httpx,'get',lambda *a,**kw:httpx.Response(401,json={}))
    result=delivery.check(tmp_path/'cases.json',tmp_path/'result')
    assert result['status']=='pending' and not result['passed'] and len(result['pending'])==1
    fixture['search_build']='candidate'
    atomic_json(tmp_path/'cases.json',fixture)
    with pytest.raises(RuntimeError,match='search build differs'):
        delivery.check(tmp_path/'cases.json',tmp_path/'wrong-build')
    del fixture['search_build']
    fixture['cases'][0]['path']='https://external.example/steal'
    atomic_json(tmp_path/'cases.json',fixture)
    with pytest.raises(ValueError,match='unsafe'):delivery.check(tmp_path/'cases.json',tmp_path/'bad')


def test_journal_draft_never_changes_active_policy(tmp_path,monkeypatch):
    policy=tmp_path/'policy.json';policy.write_text(json.dumps(dict(version=1,journals=[])));before=policy.read_bytes()
    monkeypatch.setattr(ec,'POLICY',policy)
    atomic_json(tmp_path/'audit.json',dict(structural_passed=True))
    (tmp_path/'review-items.jsonl').write_text(json.dumps(dict(id='W1',group='negative_context'))+'\n')
    atomic_json(tmp_path/'journal-coverage.json',[dict(id='S1',name='New Geology',current_scope='pending',identity_verified=True,issn_l='1234-5678',issn=[]),
        dict(id='S2',name='Legacy JGR',current_scope='core',identity_verified=True,issn_l='0148-0227',issn=[])])
    atomic_json(tmp_path/'reviews.json',[dict(id='W1',label='related',evidence='Watershed study',reviewer_type='agent')])
    atomic_json(tmp_path/'journals.json',[dict(id='S1',scope='core',url='https://publisher.example/scope',rationale='Solid Earth processes'),
        dict(id='S2',scope='cross',url='https://publisher.example/history',rationale='Legacy umbrella identity includes planetary physics')])
    result=audit.finalize(tmp_path,tmp_path/'reviews.json',tmp_path/'journals.json')
    assert result['topic_review_complete'] and not result['human_reviewed'] and result['draft_additions']==['S1']
    assert policy.read_bytes()==before
    draft=tmp_path/'earth-journal-scopes-v2.draft.json';value=json.loads(draft.read_text())
    assert value['version']==2 and value['base_policy_version']==1
    assert value['source_scope_overrides']['S2']['scope']=='cross'
    assert result['draft_source_overrides']==['S2']
    monkeypatch.setattr(ec,'POLICY',draft)
    from openalex_api import recovery
    monkeypatch.setattr(recovery,'require_evidence',lambda *a:dict(signature='frozen'))
    monkeypatch.setattr(ec,'load_catalog',lambda *a:dict(metadata_signature='frozen'))
    with pytest.raises(ValueError,match='invalid journal scope policy'):ec.initialize(None,'g','c')
    monkeypatch.setattr(ec,'POLICY',policy)
    atomic_json(tmp_path/'reviews.json',[dict(id='W2',label='earth',evidence='wrong',reviewer_type='agent')])
    with pytest.raises(ValueError):audit.finalize(tmp_path,tmp_path/'reviews.json',tmp_path/'journals.json')


def test_priority_balances_earth_profile_and_scale():
    def journal(n):return dict(works_count=n,topic_profile=[dict(count=10,field={'id':'fields/19'})])
    assert audit.journal_rank(journal(10000))>audit.journal_rank(journal(1))
