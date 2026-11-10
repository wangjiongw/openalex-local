import pytest

from openalex_api import catalog_build as cb
from openalex_api.earth_control import read_json


def test_out_of_order_blocks_do_not_skip_holes_or_double_count(monkeypatch):
    monkeypatch.setattr(cb,'save',lambda m:None)
    base=dict(index='prefix',entity='authors',count=1)
    plan=dict(base=dict(after='A1002',selected=2,missing=1,missing_sample=['A1002']),base_segments=[base])
    m=dict(segments=[base,dict(entity='works',index='works',count=10)],catalog_progress={},entities={},expected_authors=5)
    states=[dict(after='A1002',selected=0,missing=0,missing_sample=[],state='building',segment=dict(index='one',entity='authors',count=0)),
            dict(after='A1005',selected=2,missing=1,missing_sample=['A1004'],state='complete',segment=dict(index='two',entity='authors',count=1))]
    result=cb.merge(m,plan,states)
    assert result['selected']==4 and result['after']=='A1002' and not result['done']
    states[0].update(after='A1003',selected=1,state='complete',segment=dict(index='one',entity='authors',count=1))
    result=cb.merge(m,plan,states)
    assert result['done'] and result['after']=='A1005' and result['selected']==5 and result['missing']==2
    assert m['entities']['authors']['count']==3
    cb.merge(m,plan,states)
    assert m['entities']['authors']['count']==3 and len({s['index'] for s in m['segments']})==4
    states[1]['segment']['count']=2
    with pytest.raises(RuntimeError,match='coverage mismatch'):
        cb.merge(m,plan,states)


def test_block_plan_has_disjoint_frozen_ranges_and_keeps_prefix(tmp_path,monkeypatch):
    monkeypatch.setattr(cb,'build_path',lambda g,b:tmp_path)
    monkeypatch.setattr(cb,'config',lambda:dict(cpus=4,memory_gib=16,catalog_workers=8))
    class Backend:
        def __init__(self):self.bounds=iter(['A1004','A1006','A1008','A1010']);self.optimized=False
        def ch(self,query):
            if query.startswith('OPTIMIZE'):
                self.optimized=True;return []
            return [{'id':next(self.bounds)}]
    backend=Backend()
    prefix=dict(entity='authors',index='prefix',count=1,sealed=True)
    m=dict(generation='example',build='catalogs',expected_authors=10,segments=[prefix],
           metadata_signature='metadata',parent_manifest_sha256='parent',author_membership_table='example.members',
           physical_layout='native_v2',catalog_progress=dict(authors=dict(after='A1002',selected=2,missing=1,missing_sample=['A1002'])))
    plan=cb.prepare(backend,None,m)
    states=[read_json(tmp_path/name) for name in plan['blocks']]
    assert backend.optimized and plan['base_segments']==[prefix]
    assert [(s['lower'],s['upper']) for s in states]==[('A1002','A1004'),('A1004','A1006'),('A1006','A1008'),('A1008','A1010')]
    assert sum(s['expected'] for s in states)+plan['base']['selected']==10
    assert len({s['segment']['index'] for s in states})==4
    assert cb.prepare(backend,None,m)==plan

    m['metadata_signature']='changed'
    with pytest.raises(RuntimeError,match='inputs changed'):
        cb.prepare(backend,None,m)
