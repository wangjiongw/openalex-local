import json
from unittest.mock import Mock
import pytest
from openalex_api import continuous_storage as c


def test_continuous_gate_does_not_use_old_elapsed_time():
    s=dict(unique=20000000,continuous_seconds=86399,continuous_records=2028000,updates=1,active_seconds=999999)
    assert not c.workload_passed(s)
    s['continuous_seconds']=86400
    assert c.workload_passed(s)
    s['continuous_records']=2027999
    assert not c.workload_passed(s)


@pytest.mark.parametrize('bad', ['missing','version','sort','duplicate','outside'])
def test_reverse_exact_rejects_differences(bad):
    hits=[{'_id':'W2','_version':2,'sort':['W2']}]
    rows=[{'id':'W2','version':2}]
    after='W1'
    if bad=='missing':rows=[]
    if bad=='version':rows[0]['version']=3
    if bad=='sort':hits[0]['sort']=['W3']
    if bad=='duplicate':hits*=2
    if bad=='outside':after='W2'
    with pytest.raises(RuntimeError):c.compare_reverse(hits,rows,after)


def test_reverse_exact_accepts_equal_sets():
    assert c.compare_reverse([{'_id':'W2','_version':2,'sort':['W2']}],[{'id':'W2','version':'2'}],'W1')=='W2'


@pytest.mark.parametrize('failure', ['bulk','mget','log','space'])
def test_partial_batch_never_advances_durable_cursor(tmp_path,monkeypatch,failure):
    q=c.Qualification(base=tmp_path)
    q.s=dict(phase='writing',unique=6000000,updates=0,last_id='W1',initial_last='W1',cycles=0,
             continuous_seconds=0,continuous_records=0)
    monkeypatch.setattr(c,'mark',Mock())
    monkeypatch.setattr(c,'generation_path',lambda g:tmp_path/'generation')
    q.save()
    monkeypatch.setattr(c,'project_record',lambda raw:{})
    q.b=Mock()
    q.b.ch.return_value=[{'id':'W2','version':2,'raw':'{}'}]
    q.b.mget.return_value=[{'_id':'W2','_version':2,'found':True}]
    if failure=='bulk':q.b.bulk.side_effect=RuntimeError('injected after another sink')
    if failure=='mget':q.b.mget.return_value=[{'_id':'W2','_version':1,'found':True}]
    calls=0
    def guard():
        nonlocal calls
        calls+=1
        if failure=='log' and calls==2:raise RuntimeError('merge failure')
        if failure=='space':raise c.resources.Paused('disk-pause')
    q.guard=guard
    with pytest.raises(RuntimeError):q.workload()
    saved=json.loads(q.path.read_text())
    assert saved['last_id']=='W1' and saved['unique']==6000000 and saved['continuous_records']==0


def test_restart_clears_continuous_time_but_keeps_records(tmp_path,monkeypatch):
    q=c.Qualification(base=tmp_path)
    q.s=dict(phase='writing',unique=12000000,updates=0,last_id='W8',initial_last='W8',cycles=0,
             continuous_seconds=80000,continuous_records=8000000)
    monkeypatch.setattr(c,'mark',Mock())
    monkeypatch.setattr(c,'generation_path',lambda g:tmp_path/'generation')
    q.guard=Mock(side_effect=c.resources.Paused('user'))
    with pytest.raises(c.resources.Paused):q.workload()
    saved=json.loads(q.path.read_text())
    assert saved['continuous_seconds']==saved['continuous_records']==0
    assert saved['unique']==12000000 and saved['last_id']=='W8'


def test_certify_rejects_missing_reverse_without_replacing_receipt(tmp_path,monkeypatch):
    q=c.Qualification(base=tmp_path)
    q.s=dict(dependencies={},unique=20000000,continuous_seconds=86400,continuous_records=3000000,
             updates=100,crash_recovered=True,reconciliations={'post-recovery':{'forward_done':True,'forward':20000000}},physical={})
    (tmp_path/'console-test.log').write_text('MERGE_CAPTURE_TRANSFORMED')
    (tmp_path/'merge-capture.jar').write_bytes(b'test')
    q.s['capture_agent_sha256']=c.digest(tmp_path/'merge-capture.jar')
    from openalex_api import proofs
    monkeypatch.setattr(proofs,'dependencies',lambda *a:{})
    issue=Mock();monkeypatch.setattr(c.recovery,'evidence',issue)
    with pytest.raises(RuntimeError,match='incomplete'):q.certify()
    issue.assert_not_called()


def test_incomplete_source_receipt_rejected():
    prior={'attempt':'919ccc03c83a40e9a3bcbeb8afe934fe','unique':3276000,'failure':'merge'}
    extended={'state':'complete','total':2028000,'physical_passed':True,'formal_proofs_unchanged':True,'physical':{}}
    with pytest.raises(RuntimeError,match='incomplete'):c.require_source(prior,extended)


def test_internal_indexes_do_not_enter_works_manifest(tmp_path):
    for uuid in ('works-uuid','internal-index'):
        for shard in ('0','1','2','3') if uuid=='works-uuid' else ('0',):
            (tmp_path/'data/nodes/0/indices'/uuid/shard/'index').mkdir(parents=True)
    paths=c.source_shards(tmp_path,'works-uuid')
    assert len(paths)==4 and all(p.parent.parent.name=='works-uuid' for p in paths)
    with pytest.raises(RuntimeError,match='four inherited'):
        c.source_shards(tmp_path,'internal-index')


def test_downstream_search_keeps_qualified_capture_and_rejects_changed_jar(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(c,'BASE',tmp_path)
    monkeypatch.setattr(c.recovery,'require_evidence',Mock())
    p=tmp_path/'storage-verification.json'
    p.write_text(json.dumps({'passed':False}))
    assert c.qualified_capture_options()==[]
    agent=tmp_path/'merge-capture.jar';agent.write_bytes(b'qualified')
    p.write_text(json.dumps({'passed':True,'merge_capture':True,'capture_agent_sha256':c.digest(agent)}))
    assert c.qualified_capture_options()==c.expected_capture_options(tmp_path)
    agent.write_bytes(b'changed')
    with pytest.raises(RuntimeError,match='changed'):
        c.qualified_capture_options()


def test_new_qualification_directory_is_used_without_touching_old_failure(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'ROOT',tmp_path)
    monkeypatch.setattr(c,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(c.recovery,'require_evidence',Mock())
    base=tmp_path/'reports/fresh';base.mkdir(parents=True)
    agent=base/'merge-capture.jar';agent.write_bytes(b'qualified')
    receipt={'passed':True,'merge_capture':True,'capture_agent_sha256':c.digest(agent),
             'qualification_path':str(base/'progress.json')}
    p=tmp_path/'storage-verification.json';p.write_text(json.dumps(receipt))
    assert c.qualified_capture_options()==c.expected_capture_options(base)
    receipt['qualification_path']='/tmp/other/progress.json';p.write_text(json.dumps(receipt))
    with pytest.raises(RuntimeError,match='outside project'):c.qualified_capture_options()
