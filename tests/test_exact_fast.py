import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path
import pytest
from openalex_api import exact_fast as fast, exact, ingest, proofs, resources
from openalex_api.backend import Backend, database


def seed_legacy(path, left='entities', right='audit'):
    path.mkdir(parents=True)
    with sqlite3.connect(path/'tasks.sqlite') as db:
        db.execute('CREATE TABLE ranges(lo TEXT PRIMARY KEY,hi TEXT,expected INTEGER,state TEXT,missing INTEGER)')
        db.execute("INSERT INTO ranges VALUES('','W3',3,'done',0)")
        db.execute('CREATE TABLE planning(singleton INTEGER PRIMARY KEY,last TEXT,done INTEGER)')
        db.execute("INSERT INTO planning VALUES(1,'W3',0)")
    (path/'result.json').write_text(json.dumps(dict(left=left,right=right)))
    (path/(hashlib.sha256(b'').hexdigest()[:16]+'.jsonl')).write_text('')


def test_migration_preserves_only_proven_direction(tmp_path):
    seed_legacy(tmp_path/'case')
    target, db=fast.open_tasks(tmp_path/'case','entities','audit')
    assert db.execute('SELECT state FROM ranges').fetchone()[0]=='done'
    db.close()
    _, db=fast.open_tasks(tmp_path/'case','audit','entities')
    assert db.execute('SELECT state FROM ranges').fetchone()[0]=='pending'
    assert db.execute('SELECT last FROM planning').fetchone()[0]=='W3'
    db.close()
    assert (tmp_path/'case/tasks.sqlite').exists()


def test_legacy_aliased_relation_receipt_does_not_prove_reverse(tmp_path):
    seed_legacy(tmp_path/'case','identifiers','recovery_ids_x')
    for left,right in [('identifiers','recovery_ids_x'),('recovery_ids_x','identifiers')]:
        _,db=fast.open_tasks(tmp_path/'case',left,right)
        assert db.execute('SELECT state,missing FROM ranges').fetchone()==('pending',None)
        assert db.execute('SELECT last FROM planning').fetchone()[0]=='W3'
        db.close()


def test_union_stream_keeps_right_only_keys_and_is_bounded():
    class Fake:
        def ch_stream(self,query):
            keys=['W1','W3','W3'] if '.left' in query else ['W2','W3','W4']
            yield from ({'id':k} for k in keys)
    assert list(fast.merged_keys(Fake(),'test',[('left',False),('right',False)],'id','works',''))==['W1','W2','W3','W4']


@pytest.fixture
def live(tmp_path,monkeypatch):
    if os.getenv('OA_EXACT_LIVE')!='1':pytest.skip('isolated real CH opt-in')
    g='test_exact_'+uuid.uuid4().hex[:10]
    b=Backend(writer=True)
    b.ch(f'CREATE DATABASE {database(g)}')
    path=tmp_path/g;path.mkdir()
    monkeypatch.setattr(fast,'generation_path',lambda _:path)
    monkeypatch.setattr(exact,'generation_path',lambda _:path)
    monkeypatch.setattr(proofs,'semantic',lambda _:'test-data')
    monkeypatch.setattr(resources,'checkpoint',lambda *a,**k:None)
    ledger=tmp_path/'ledger.sqlite'
    with sqlite3.connect(ledger) as db:
        db.execute('CREATE TABLE shards(relative TEXT,metadata_line INTEGER,metadata_state TEXT,selected INTEGER)')
        db.execute("INSERT INTO shards VALUES('fixture',1,'done',1)")
    monkeypatch.setattr(ingest,'connect',lambda _:sqlite3.connect(ledger))
    monkeypatch.setattr(fast,'RANGE_SIZE',2)
    monkeypatch.setattr(fast,'KEY_PAGE_SIZE',2)
    for name in ['expected_ids','identifiers']:
        b.ch(f'CREATE TABLE {database(g)}.{name} (entity String,scheme String,value String,id String,version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY(entity,scheme,value,id)')
    yield g,b,path
    b.close()  # Retain isolated tables as reproducible evidence.


def fill(b,g,table,items):
    b.insert(g,table,[dict(entity='works',scheme='doi',value=value,id=key,version=1) for key,value in items])


def test_live_both_directions_and_ready_projection_reuse(live):
    g,b,path=live
    fill(b,g,'expected_ids',[('W1','a'),('W2','b'),('W3','c'),('W7','g')])
    fill(b,g,'identifiers',[('W1','a'),('W2','b'),('W4','d'),('W8','h')])
    args=('entity,scheme,value,id','id','works')
    assert exact.compare(g,b,'expected_ids','identifiers',*args)==2
    assert exact.compare(g,b,'identifiers','expected_ids',*args)==2
    receipts=list((path/'exact-projections').glob('*.json'))
    assert len(receipts)==2
    before={f.name:f.read_bytes() for f in receipts}
    assert exact.compare(g,b,'expected_ids','identifiers',*args)==2
    assert before=={f.name:f.read_bytes() for f in receipts}
    dirs=list((path/'exact-differences').glob('*/direction-*'))
    assert len(dirs)==2
    for directory in dirs:
        with sqlite3.connect(directory/'tasks.sqlite') as db:
            assert db.execute('SELECT sum(expected) FROM ranges').fetchone()[0]==6
            assert db.execute('SELECT count(*) FROM ranges WHERE state!="done"').fetchone()[0]==0


def test_live_resume_after_sink_before_checkpoint_failure(live,monkeypatch):
    g,b,path=live
    fill(b,g,'expected_ids',[('W1','a'),('W2','b'),('W3','c'),('W9','x')])
    fill(b,g,'identifiers',[('W1','a'),('W2','b'),('W3','c')])
    original=fast.difference
    failed=[]
    def crash(*args):
        n=original(*args)
        if not failed:
            failed.append(True)
            raise RuntimeError('injected after durable difference before checkpoint')
        return n
    monkeypatch.setattr(fast,'difference',crash)
    args=('entity,scheme,value,id','id','works')
    with pytest.raises(RuntimeError,match='injected'):exact.compare(g,b,'expected_ids','identifiers',*args)
    monkeypatch.setattr(fast,'difference',original)
    assert exact.compare(g,b,'expected_ids','identifiers',*args)==1
    assert exact.compare(g,b,'identifiers','expected_ids',*args)==0


def test_live_keeps_legacy_planning_boundary(live,monkeypatch):
    g,b,path=live
    fill(b,g,'expected_ids',[(f'W{i}',str(i)) for i in range(1,7)])
    fill(b,g,'identifiers',[(f'W{i}',str(i)) for i in range(1,7)])
    captured=[]
    actual=fast.compare_ranges
    monkeypatch.setattr(fast,'compare_ranges',lambda *a:captured.append(a) or 0)
    args=('entity,scheme,value,id','id','works')
    exact.compare(g,b,'expected_ids','identifiers',*args)
    directory=path/'exact-differences'/captured[0][2]
    seed_legacy(directory,'expected_ids','identifiers')
    monkeypatch.setattr(fast,'compare_ranges',actual)
    starts=[]
    original=fast.merged_keys
    def record(*a):
        starts.append(a[-1])
        yield from original(*a)
    monkeypatch.setattr(fast,'merged_keys',record)
    assert exact.compare(g,b,'expected_ids','identifiers',*args)==0
    assert starts==['W3']
    for f in directory.glob('direction-*/tasks.sqlite'):
        with sqlite3.connect(f) as db:
            assert db.execute('SELECT hi,expected FROM ranges WHERE lo=""').fetchone()==('W3',3)
            assert db.execute('SELECT sum(expected) FROM ranges').fetchone()[0]==6


def test_reverse_reuses_full_forward_plan_without_reusing_results(tmp_path):
    seed_legacy(tmp_path/'case','expected_ids','identifiers')
    target,db=fast.open_tasks(tmp_path/'case','expected_ids','identifiers')
    with db:
        db.execute("INSERT INTO ranges VALUES('W3','W6',3,'done',0)")
        db.execute("UPDATE ranges SET state='done',missing=0")
        db.execute("UPDATE planning SET last='W6',done=1")
    db.close()
    _,db=fast.open_tasks(tmp_path/'case','identifiers','expected_ids')
    assert db.execute('SELECT last,done FROM planning').fetchone()==('W6',1)
    assert db.execute('SELECT count(*) FROM ranges').fetchone()[0]==2
    assert db.execute('SELECT count(*) FROM ranges WHERE state="done"').fetchone()[0]==0
    db.close()


def test_live_unrelated_partition_change_keeps_proof_but_target_drop_invalidates(live,monkeypatch):
    g,b,path=live
    for table in ['partition_left','partition_right']:
        b.ch(f'CREATE TABLE {database(g)}.{table} (entity String,id String,version UInt64) ENGINE=ReplacingMergeTree(version) PARTITION BY entity ORDER BY(entity,id)')
        b.insert(g,table,[dict(entity='works',id='W1',version=1),dict(entity='keywords',id='K1',version=1)])
    args=('entity,id','id','works')
    assert exact.compare(g,b,'partition_left','partition_right',*args)==0
    original=fast.difference
    def forbidden(*a):raise AssertionError('completed works proof was unnecessarily rerun')
    monkeypatch.setattr(fast,'difference',forbidden)
    b.insert(g,'partition_left',[dict(entity='keywords',id='K2',version=1)])
    assert exact.compare(g,b,'partition_left','partition_right',*args)==0
    monkeypatch.setattr(fast,'difference',original)
    b.ch(f"ALTER TABLE {database(g)}.partition_left DROP PARTITION 'works'")
    assert exact.compare(g,b,'partition_right','partition_left',*args)==1


def test_live_ordered_conflicts_cache_and_invalidation(live,monkeypatch):
    g,b,path=live
    b.ch(f'CREATE TABLE {database(g)}.audit (entity String,id String,source String,line UInt64,digest String,version UInt64) ENGINE=ReplacingMergeTree(version) PARTITION BY entity ORDER BY(entity,id,source,line)')
    rows=[dict(entity='works',id='W1',source='a',line=1,digest='a',version=1),dict(entity='works',id='W1',source='b',line=1,digest='b',version=1),dict(entity='works',id='W2',source='a',line=2,digest='c',version=1)]
    b.insert(g,'audit',rows)
    assert fast.conflict_count(g,b)==1
    original=b.ch
    def no_recheck(query,*a,**kw):
        if 'HAVING uniqExact(digest)' in query:raise AssertionError('unchanged conflict check reran')
        return original(query,*a,**kw)
    monkeypatch.setattr(b,'ch',no_recheck)
    assert fast.conflict_count(g,b)==1
    monkeypatch.setattr(b,'ch',original)
    b.insert(g,'audit',[dict(entity='works',id='W2',source='b',line=2,digest='d',version=1)])
    assert fast.conflict_count(g,b)==2


def test_paged_union_closes_responses_before_yield(monkeypatch):
    import re
    monkeypatch.setattr(fast, 'KEY_PAGE_SIZE', 2)
    class Fake:
        active = 0
        queries = []
        def ch_stream(self, query):
            self.queries.append(query)
            self.active += 1
            try:
                lower = re.search(r"id>'([^']*)'", query).group(1)
                keys = ['W1','W3','W5','W7'] if '.left' in query else ['W2','W3','W6','W8']
                for k in [k for k in keys if k > lower][:2]:
                    yield {'id': k}
            finally:
                self.active -= 1
    b = Fake()
    values = []
    for value in fast.merged_keys(b, 'test', [('left', False), ('right', False)], 'id', 'works', 'W1'):
        assert b.active == 0
        values.append(value)
    assert values == ['W2','W3','W5','W6','W7','W8']
    assert all('LIMIT 2' in q for q in b.queries)
