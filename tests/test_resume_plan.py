import json
from unittest.mock import Mock
import pytest
from openalex_api import resume, recovery, storage, manage


def test_until_boundary(tmp_path, monkeypatch):
    monkeypatch.setattr('openalex_api.build.mark',Mock())
    assert resume.boundary('x','metadata-complete','metadata-complete')
    assert not resume.boundary('x','metadata-complete','complete')


def test_failed_retirement_never_releases(tmp_path,monkeypatch):
    monkeypatch.setattr(resume,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(resume.recovery,'signature',lambda g:'sig')
    monkeypatch.setattr(resume,'read_generation',lambda g:{'manifest_sha256':'manifest'})
    p=tmp_path/'retired-search.json'
    record={'state':'closing','signature':'sig','manifest_sha256':'manifest'}
    p.write_text(json.dumps(record))
    with pytest.raises(RuntimeError):resume.require_search_disposition('test')
    record['state']='retired';p.write_text(json.dumps(record))
    assert resume.require_search_disposition('test')['state']=='retired'
    record['signature']='old';p.write_text(json.dumps(record))
    with pytest.raises(RuntimeError):resume.require_search_disposition('test')


def test_retirement_does_not_skip_metadata_audit(tmp_path,monkeypatch):
    calls=[]
    def require(g,name):
        calls.append(name)
        if name=='metadata-audit':raise RuntimeError('bad metadata')
    monkeypatch.setattr(recovery,'generation_path',lambda g:tmp_path)
    monkeypatch.setattr(recovery,'require_evidence',require)
    monkeypatch.setattr(resume,'require_search_disposition',lambda g: {'state':'retired'})
    with pytest.raises(RuntimeError,match='bad metadata'):recovery.release('test','metadata')
    assert 'metadata-audit' in calls


def test_admission_ramp_and_stale_sample():
    a=resume.Admission(4)
    def sample(t):return {'time':t,'aggregate_rss':1024,'processes':[{'fds':10,'nofile':[100,100]}]}
    assert a.update(sample(100),100)==2
    assert a.update(sample(3700),3700)==4
    assert a.update(sample(3700),3800)==2
    s=sample(3900);s['processes'][0]['fds']=80
    assert a.update(s,3900)==2


def test_committed_fingerprint_detects_same_length_change(tmp_path):
    (tmp_path/'segments_1').write_bytes(b'aaa')
    first=storage.committed_fingerprint(tmp_path)
    (tmp_path/'segments_1').write_bytes(b'bbb')
    assert storage.committed_fingerprint(tmp_path)!=first


def test_background_stop_stage_forwarded():
    command,_=manage.command('recover-full_20260908',['--until','metadata-complete'])
    assert command[-2:]==['--until','metadata-complete']


def test_observation_downtime_not_counted():
    state=resume.observation_tick({},100)
    state=resume.observation_tick(state,130)
    assert state['seconds']==30
    state=resume.observation_tick(state,1000)
    assert state['seconds']==0


def test_derived_rename_gap_resumes(tmp_path,monkeypatch):
    from openalex_api.backend import database
    monkeypatch.setattr(resume,'ROOT',tmp_path)
    monkeypatch.setattr(recovery,'signature',lambda g:'sig')
    tables={'old':'old-uuid','staging':'new-uuid'}
    b=Mock()
    def ch(query):
        if query.startswith('SELECT'):
            name=query.split("name='")[1].split("'")[0]
            return [{'uuid':tables[name]}] if name in tables else []
        if query.startswith('RENAME'):
            left,right=query.split('RENAME TABLE ')[1].split(' TO ')
            tables[right.split('.')[-1]]=tables.pop(left.split('.')[-1])
    b.ch.side_effect=ch
    state={'signature':'sig','tables':{'references_in':{'old_uuid':'old-uuid','new_uuid':'new-uuid','retained_table':'old','staging_table':'staging'}}}
    resume.finish_derived_switches('test',b,state,tmp_path/'repair.json')
    assert tables['references_in']=='new-uuid' and tables['old']=='old-uuid'
    assert state['tables']['references_in']['switched']
    resume.finish_derived_switches('test',b,state,tmp_path/'repair.json')


def test_supervisor_cli_forwards_stage(monkeypatch):
    from openalex_api import cli
    import sys
    supervisor = Mock()
    monkeypatch.setattr(manage, 'supervise', supervisor)
    monkeypatch.setattr(sys, 'argv', ['oa', '_supervise', 'recover-full_20260908', '--until', 'complete'])
    cli.main()
    supervisor.assert_called_once_with('recover-full_20260908', ['--until', 'complete'])


def test_already_closed_retirement_needs_no_close_ack():
    b=Mock()
    b.os.return_value=[{'index':'old','status':'close'}]
    resume.ensure_retired_index_closed(b,'old')
    assert all(call.args[0]=='GET' for call in b.os.call_args_list)


def test_open_retirement_requires_shard_ack():
    b=Mock()
    b.os.side_effect=[[{'index':'old','status':'open'}],{'acknowledged':True,'shards_acknowledged':False}]
    with pytest.raises(RuntimeError,match='not acknowledged'):
        resume.ensure_retired_index_closed(b,'old')


def test_closed_retirement_rechecks_actual_state():
    b=Mock()
    b.os.side_effect=[[{'index':'old','status':'close'}],[{'index':'old','status':'open'}]]
    with pytest.raises(RuntimeError,match='not closed'):
        resume.ensure_retired_index_closed(b,'old')
