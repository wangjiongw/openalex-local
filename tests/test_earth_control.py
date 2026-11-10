import errno
import json
from pathlib import Path

import pytest

from openalex_api import earth_control as ctl
from openalex_api.config import atomic_json
from openalex_api.resources import Paused


@pytest.fixture
def controlled(tmp_path, monkeypatch):
    monkeypatch.setattr(ctl.ec,'collection_path',lambda *a:tmp_path)
    monkeypatch.setattr(ctl,'generation_path',lambda g:tmp_path/'generation')
    monkeypatch.setattr(ctl.time,'sleep',lambda delay:None)
    atomic_json(tmp_path/'manifest.json',dict(blocks=0,state='running'))
    return tmp_path


def missing(path):
    return FileNotFoundError(errno.ENOENT,'transient',str(path))


def test_resume_preserves_budget_and_logs_failure(controlled,monkeypatch,caplog):
    calls=[]
    def run(g,collection,follow,remaining):
        calls.append(remaining)
        if len(calls)==1:
            atomic_json(controlled/'manifest.json',dict(blocks=2,state='failed'))
            raise missing(controlled/'generation/search-catalog.json')
        return dict(blocks=5,state='partial')
    monkeypatch.setattr(ctl.ec,'run',run)
    result=ctl.run('g','v1',max_blocks=5)
    assert result['blocks']==5 and calls==[5,3]
    recovery=json.loads((controlled/'export-recovery.json').read_text())
    assert recovery['retries']==1 and recovery['last_successful_blocks']==5
    assert 'earth_export_retry' in caplog.text


def test_retries_are_bounded_without_progress(controlled,monkeypatch):
    calls=[]
    def run(*a):
        calls.append(1);raise missing(controlled/'generation/search-catalog.json')
    monkeypatch.setattr(ctl.ec,'run',run)
    with pytest.raises(FileNotFoundError):ctl.run('g')
    assert len(calls)==6
    assert json.loads((controlled/'export-recovery.json').read_text())['state']=='failed'


def test_progress_resets_consecutive_retry_budget(controlled,monkeypatch):
    calls=[]
    def run(*a):
        calls.append(1);n=len(calls)
        if n==8:return dict(state='complete',blocks=7)
        atomic_json(controlled/'manifest.json',dict(state='failed',blocks=n))
        raise missing(controlled/'generation/search-catalog.json')
    monkeypatch.setattr(ctl.ec,'run',run)
    assert ctl.run('g')['blocks']==7 and len(calls)==8


@pytest.mark.parametrize('error',[RuntimeError('identity changed'),ValueError('corrupt JSON'),PermissionError('denied'),Paused('signal-stop')])
def test_correctness_failures_and_pauses_do_not_retry(controlled,monkeypatch,error):
    calls=[]
    def run(*a):calls.append(1);raise error
    monkeypatch.setattr(ctl.ec,'run',run)
    with pytest.raises(type(error)):ctl.run('g')
    assert len(calls)==1
    assert json.loads((controlled/'export-recovery.json').read_text())['state']==('paused' if isinstance(error,Paused) else 'failed')


def test_missing_committed_file_is_not_retried(controlled,monkeypatch):
    monkeypatch.setattr(ctl.ec,'run',lambda *a:(_ for _ in ()).throw(missing(controlled/'blocks/00000000.json')))
    with pytest.raises(FileNotFoundError):ctl.run('g')
    assert json.loads((controlled/'export-recovery.json').read_text())['retries']==0


def test_status_keeps_manifest_when_catalog_is_missing(controlled,monkeypatch):
    monkeypatch.setattr(ctl.ec,'status',lambda *a:(_ for _ in ()).throw(missing(controlled/'generation/search-catalog.json')))
    result=ctl.status('g','v1')
    assert result['blocks']==0 and result['current_catalog']['state']=='unavailable'


def test_read_retries_only_missing_files(controlled,monkeypatch):
    original=Path.read_text;calls=[]
    def read(path,*a,**kw):
        if path.name=='manifest.json':
            calls.append(1)
            if len(calls)<3:raise missing(path)
        return original(path,*a,**kw)
    monkeypatch.setattr(Path,'read_text',read)
    assert ctl.read_json(controlled/'manifest.json')['blocks']==0 and len(calls)==3
    (controlled/'bad.json').write_text('{bad')
    with pytest.raises(json.JSONDecodeError):ctl.read_json(controlled/'bad.json')
