import copy
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from openalex_api import version_assessment as a,versions_full as f
from openalex_api.semantics import digest
from test_version_acceleration import setup_group


def record(source,target,anchor='W2',accepted=True):
    return dict(id=source+':'+target,work_id=source,target_id=target,anchor=anchor,
                source=dict(id=source,source_hash=digest(dict(id=source))),
                target=dict(id=target,source_hash=digest(dict(id=target))),
                evidence=dict(exact_title=True),accepted=accepted)


def setup(tmp_path,monkeypatch,records):
    m,b,rows,_,_,_=setup_group(tmp_path,monkeypatch)
    m.update(generation='demo',counts=dict(pairs=len(records)))
    monkeypatch.setattr(f.automatic,'allows',lambda r:r['accepted'])
    monkeypatch.setattr(f.legacy.rules,'notice',lambda r:False)
    visited=[]
    def pages(m,b,size=10000,after=''):
        for row in records:
            key=row['work_id']+':'+row['target_id']
            if key<=after:continue
            visited.append(key);yield [dict(id=key,payload=json.dumps(row))]
    monkeypatch.setattr(f,'candidate_pages',pages)
    b.details.side_effect=lambda generation,entity,keys:{k:dict(id=k) for k in keys}
    return m,b,rows,visited


def test_resume_keeps_durable_cursor_and_first_direction(tmp_path,monkeypatch):
    records=[record('W1','W2'),record('W2','W1'),record('W3','W4',accepted=False)]
    m,b,rows,visited=setup(tmp_path,monkeypatch,records)
    db,state=a.connect(m)
    def stop(m):raise InterruptedError('stop after durable commit')
    monkeypatch.setattr(f,'protect',stop)
    with pytest.raises(InterruptedError):a.assess(m,b,db,state)
    db.close()
    monkeypatch.setattr(f,'protect',lambda m:None)
    db,state=a.connect(m)
    assert state['after']=='W1:W2' and state['processed']=='1'
    a.assess(m,b,db,state)
    assert visited==['W1:W2','W2:W1','W3:W4']
    assert db.execute('SELECT count(*) FROM eligible').fetchone()[0]==1
    assert json.loads(db.execute('SELECT payload FROM eligible').fetchone()[0])==records[0]
    a.verify(m,b,db);a.write(m,b,{'release':'parent'},db)
    group=json.loads(next(iter(rows['oa_versions_new.groups'].values()))['payload'])
    assert group['members']==['W1','W2'] and group['evidence']==[records[0]['evidence']|dict(method='automatic_bibliographic',policy=f.automatic.POLICY,independent_review=False)]
    assert m['counts']['raw_verified']==2
    result=copy.deepcopy(rows)
    a.assess(m,b,db,state);a.verify(m,b,db);a.write(m,b,{'release':'parent'},db)
    assert rows==result
    checkpoint=f.path('new')/'groups-progress.json'
    checkpoint.write_text(json.dumps(dict(identity='changed',after_anchor='W999')))
    with pytest.raises(RuntimeError,match='checkpoint identity'):a.write(m,b,{'release':'parent'},db)
    db.close()


def test_conflicts_across_pages_block_all_anchors(tmp_path,monkeypatch):
    records=[record('W1','W2'),record('W1','W9',anchor='W9')]
    m,b,rows,_=setup(tmp_path,monkeypatch,records)
    db,state=a.connect(m)
    a.assess(m,b,db,state);a.verify(m,b,db);a.write(m,b,{'release':'parent'},db)
    assert not rows
    db.close()


def test_evidence_order_uses_directional_cursor_not_pair_hash(tmp_path,monkeypatch):
    records=[record('W1','W2'),record('W2','W3')]
    records[0].update(id='zzz',evidence=dict(first=True))
    records[1].update(id='aaa',evidence=dict(second=True))
    m,b,rows,_=setup(tmp_path,monkeypatch,records)
    db,state=a.connect(m);a.assess(m,b,db,state);a.verify(m,b,db);a.write(m,b,{'release':'parent'},db)
    group=json.loads(next(iter(rows['oa_versions_new.groups'].values()))['payload'])
    assert group['evidence'][0]['first'] and group['evidence'][1]['second']
    db.close()


def test_failed_review_write_does_not_advance_cursor(tmp_path,monkeypatch):
    m,b,rows,_=setup(tmp_path,monkeypatch,[record('W1','W2',accepted=False)])
    db,state=a.connect(m)
    monkeypatch.setattr(f,'insert',Mock(side_effect=RuntimeError('database failed')))
    with pytest.raises(RuntimeError,match='database failed'):a.assess(m,b,db,state)
    assert dict(db.execute('SELECT key,value FROM state'))['after']==''
    db.close()


def test_changed_identity_and_raw_hash_rejected(tmp_path,monkeypatch):
    m,b,rows,_=setup(tmp_path,monkeypatch,[record('W1','W2')])
    db,state=a.connect(m);a.assess(m,b,db,state)
    b.details.side_effect=lambda *args:{'W1':{},'W2':{}}
    with pytest.raises(RuntimeError,match='checksum'):a.verify(m,b,db)
    assert db.execute('SELECT count(*) FROM features WHERE verified IS NOT NULL').fetchone()[0]==0
    db.close();m['signature']='changed'
    with pytest.raises(RuntimeError,match='inputs changed'):a.connect(m)


def test_feature_hash_conflict_is_not_suppressed(tmp_path,monkeypatch):
    records=[record('W1','W2'),record('W1','W9',anchor='W9')]
    records[1]['source']['source_hash']='changed'
    m,b,rows,_=setup(tmp_path,monkeypatch,records)
    db,state=a.connect(m)
    with pytest.raises(RuntimeError,match='contradictory'):a.assess(m,b,db,state)
    assert dict(db.execute('SELECT key,value FROM state'))['processed']=='1'
    assert db.execute('SELECT count(*) FROM eligible').fetchone()[0]==1
    db.close()


def test_memory_budget_is_configurable_but_still_enforced(monkeypatch):
    import psutil
    monkeypatch.setattr(f.legacy,'protect',lambda m:None)
    process=SimpleNamespace(children=lambda recursive:[],memory_info=lambda:SimpleNamespace(rss=65*1024**3))
    monkeypatch.setattr(psutil,'Process',lambda:process)
    monkeypatch.setattr(f,'config',lambda:{})
    with pytest.raises(InterruptedError,match='64 GiB'):f.protect(dict(review_mode='automatic'))
    monkeypatch.setattr(f,'config',lambda:dict(version_memory_gib=128))
    f.protect(dict(review_mode='automatic'))
    process.memory_info=lambda:SimpleNamespace(rss=129*1024**3)
    with pytest.raises(InterruptedError,match='128 GiB'):f.protect(dict(review_mode='automatic'))
    monkeypatch.setattr(f,'config',lambda:dict(version_memory_gib=False))
    with pytest.raises(ValueError):f.protect(dict(review_mode='automatic'))
