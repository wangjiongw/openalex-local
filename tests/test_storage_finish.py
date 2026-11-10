import json
import time
from unittest.mock import Mock
import pytest
from openalex_api import storage_finish as finish, storage_repair as repair


@pytest.fixture
def physical(tmp_path, monkeypatch):
    generation = tmp_path / 'generation'
    generation.mkdir()
    monkeypatch.setattr(finish, 'ROOT', tmp_path)
    monkeypatch.setattr(finish, 'generation_path', lambda g: generation)
    monkeypatch.setattr(finish.resources, 'checkpoint', Mock())
    monkeypatch.setattr(finish.manage, 'identity', lambda *a, **k: None)
    monkeypatch.setattr(finish.storage, 'assert_storage_logs_clean', Mock())
    monkeypatch.setattr(finish.storage, 'runtime_options', lambda: [])
    proof = Mock(side_effect=lambda g, name, **data: data)
    monkeypatch.setattr(finish.recovery, 'evidence', proof)
    for i in range(4):
        shard=tmp_path/'var/storage-canary/data/nodes/0/indices/uuid'/str(i)/'index'
        shard.mkdir(parents=True)
        (shard/'segments_1').write_text('commit'+str(i))
    (tmp_path/'reports').mkdir()
    state=dict(validation_phase='physical',unique=20000000,active_seconds=86400,
               index_uuid='uuid',dependencies={'runtime':[]},started=time.time()-86500,
               recovery_checked=20000000,crash_recovered=True,attempt='new')
    return generation,state,proof


def test_formal_physical_resume_only_checks_unfinished_shards(physical, monkeypatch):
    generation,state,proof=physical
    checked=[]
    def check(g,shard,log,flags):
        if len(checked)==2 and not getattr(check,'interrupted',False):
            check.interrupted=True
            raise finish.resources.Paused('checker-timeout')
        checked.append(shard.parent.name)
    monkeypatch.setattr(repair,'check_one',check)
    with pytest.raises(finish.resources.Paused):
        finish.finish('test',state,Mock(),'index')
    proof.assert_not_called()
    state=json.loads((generation/'storage-progress.json').read_text())
    assert len(state['physical_checks'])==2
    result=finish.finish('test',state,Mock(),'index')
    assert checked==['0','1','2','3']
    assert result['passed'] and result['physical_check']
    assert json.loads((generation/'storage-progress.json').read_text())['validation_phase']=='passed'


def test_changed_physical_commit_cannot_reuse_recovery_evidence(physical, monkeypatch):
    generation,state,proof=physical
    checked=[]
    monkeypatch.setattr(repair,'check_one',lambda g,s,l,f:checked.append(s.parent.name))
    finish.finish('test',state,Mock(),'index')
    target=finish.ROOT/'var/storage-canary/data/nodes/0/indices/uuid/2/index/segments_1'
    target.write_text('new commit')
    with pytest.raises(RuntimeError,match='immutable canary commit changed'):
        finish.finish('test',state,Mock(),'index')
    assert checked==['0','1','2','3']


def test_missing_physical_shard_never_issues_proof(physical):
    _,state,proof=physical
    shard=finish.ROOT/'var/storage-canary/data/nodes/0/indices/uuid/3/index'
    (shard/'segments_1').unlink();shard.rmdir()
    with pytest.raises(RuntimeError,match='all four'):
        finish.finish('test',state,Mock(),'index')
    proof.assert_not_called()


def test_physical_failure_never_issues_proof(physical,monkeypatch):
    _,state,proof=physical
    monkeypatch.setattr(repair,'check_one',Mock(side_effect=RuntimeError('checksum failed')))
    with pytest.raises(RuntimeError,match='checksum'):
        finish.finish('test',state,Mock(),'index')
    proof.assert_not_called()


def test_crash_intent_alone_is_not_recovery_evidence(physical):
    _,state,proof=physical
    state.update(validation_phase='crash-issued',crash_target={'pid':123})
    with pytest.raises(RuntimeError,match='execution evidence'):
        finish.finish('test',state,Mock(),'index')
    proof.assert_not_called()
