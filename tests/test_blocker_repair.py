import contextlib,io,json,signal
from types import SimpleNamespace as NS
from unittest.mock import Mock
import httpx,pytest
from openalex_api import backend,diagnostics,manage,storage_finish


def test_long_clickhouse_queries_use_body_and_inserts_keep_compressed_data():
 import gzip
 b=backend.Backend.__new__(backend.Backend)
 b.cfg={'ch_url':'http://ch:8123','ch_reader_password':'test','query_timeout':15}
 b.writer=False
 requests=[]
 def handle(request):
  requests.append(request)
  return httpx.Response(200,text='{"n":1}\n')
 with httpx.Client(transport=httpx.MockTransport(handle)) as client:
  b.http=client
  query="SELECT length('"+('a'*70000)+"') n FORMAT JSONEachRow"
  assert b.ch(query)==[{'n':1}]
  assert requests[-1].content==query.encode() and 'query' not in requests[-1].url.params
  b.ch('INSERT INTO example FORMAT JSONEachRow',b'{"n":1}\n')
  assert requests[-1].url.params['query']=='INSERT INTO example FORMAT JSONEachRow'
  assert gzip.decompress(requests[-1].content)==b'{"n":1}\n'


def test_batch_identifier_resolution_is_one_bounded_escaped_query():
 b=backend.Backend.__new__(backend.Backend)
 b.ch=Mock(return_value=[{'value':"10.1000/a'b",'id':'W1'},{'value':"10.1000/a'b",'id':'W2'}])
 assert b.resolve_many('full_test','works','doi',[])=={}
 b.ch.assert_not_called()
 assert b.resolve_many('full_test','works','doi',["10.1000/a'b"]*2)=={"10.1000/a'b":['W1','W2']}
 b.ch.assert_called_once()
 query=b.ch.call_args.args[0]
 assert "value IN ('10.1000/a\\'b')" in query
 assert 'ORDER BY value,id LIMIT 101 BY value' in query
 assert "entity='works' AND scheme='doi'" in query


@pytest.mark.parametrize('timed_out,failed',[(True,0),(False,1),(True,1)])
def test_search_partial_preserves_failure_details_without_retry(timed_out,failed,monkeypatch):
 b=backend.Backend.__new__(backend.Backend);b.cfg={'os_url':'http://search:9200','query_timeout':15};b.writer=False
 response={'timed_out':timed_out,'took':15000,'_shards':{'total':4,'failed':failed,'failures':[
     {'index':'example','shard':2,'reason':{'type':'corrupt_index_exception','reason':'checksum failed'}}] if failed else []}}
 requests=[]
 def handle(request):
  requests.append(request)
  return httpx.Response(200,json=response)
 with httpx.Client(transport=httpx.MockTransport(handle)) as client:
  b.http=client
  monkeypatch.setattr(backend.LOG,'error',Mock(side_effect=OSError(28,'full')))
  with pytest.raises(backend.BackendError) as caught:b.os('POST','/_search',{'query':{'match_all':{}}})
 assert caught.value.backend_context['timed_out']==timed_out
 assert caught.value.backend_context['shards']==response['_shards']
 assert caught.value.backend_context['took']==15000
 assert len(requests)==1


@pytest.mark.parametrize('docs,category',[([], 'response_count'),([{'_id':'W2'}],'response_id'),([{'_id':'W1','error':{'type':'rejected_execution_exception','reason':'queue full'}}],'item_error')])
def test_mget_classification(docs,category):
 b=backend.Backend.__new__(backend.Backend);b.os=Mock(return_value={'docs':docs})
 with pytest.raises(backend.BackendError) as caught:b.mget('g','works',['W1'])
 ctx=caught.value.backend_context
 assert ctx['category']==category
 assert b.os.call_args.kwargs['headers']['X-Opaque-Id']==ctx['request_id']
 assert ctx['first_id']==ctx['last_id']=='W1'


def test_mget_retries_transient_shard_error(monkeypatch):
 b=backend.Backend.__new__(backend.Backend)
 b.os=Mock(side_effect=[
     {'docs':[{'_id':'W1','error':{'type':'no_shard_available_action_exception','reason':'recovering'}}]},
     {'docs':[{'_id':'W1','found':True,'_version':2}]},
 ])
 monkeypatch.setattr(backend.time, 'sleep', lambda _: None)
 assert b.mget('g','works',['W1'])[0]['found']
 assert b.os.call_count == 2

def test_mget_does_not_retry_merge_failure(monkeypatch):
 b=backend.Backend.__new__(backend.Backend)
 b.os=Mock(return_value={'docs':[{'_id':'W1','error':{
     'type':'no_shard_available_action_exception','reason':'closed',
     'caused_by':{'type':'already_closed_exception','caused_by':{
         'type':'merge_exception','reason':'writer hit an unrecoverable error'}}}}]})
 monkeypatch.setattr(backend.time, 'sleep', lambda _: None)
 with pytest.raises(backend.BackendError): b.mget('g','works',['W1'])
 assert b.os.call_count == 1

@pytest.mark.parametrize('doc,category',[({'_id':'W1','found':False},'missing_record'),({'_id':'W1','found':True,'_version':3},'version_mismatch')])
def test_ack_classification(doc,category):
 with pytest.raises(RuntimeError) as caught:storage_finish.require_records([{'id':'W1','version':4}],[doc])
 assert caught.value.backend_context['category']==category

def test_unwritable_log_preserves_error(monkeypatch):
 b=backend.Backend.__new__(backend.Backend);b.os=Mock(return_value={'docs':[{'_id':'W2'}]})
 monkeypatch.setattr(backend.LOG,'error',Mock(side_effect=OSError(28,'full')))
 with pytest.raises(backend.BackendError) as caught:b.mget('g','works',['W1'])
 assert caught.value.backend_context['category']=='response_id'

def test_canary_snapshot_when_formal_offline(monkeypatch):
 from openalex_api import config
 monkeypatch.setattr(config,'config',lambda:dict(os_url='http://formal:9200',ch_url='http://ch:8123',ch_reader_password='secret'))
 urls=[]
 def handle(r):
  urls.append(str(r.url))
  if r.url.host=='formal':raise httpx.ConnectError('offline')
  return httpx.Response(200,json={'sample':'ok'})
 client=httpx.Client(transport=httpx.MockTransport(handle));monkeypatch.setattr(httpx,'Client',lambda **kw:client)
 result=diagnostics.backend_snapshot(canary=True)
 assert result['opensearch']['unavailable']=='ConnectError'
 assert result['canary']['status']==result['canary_shards']['status']==200
 assert any(':19201/_nodes/stats/' in r for r in urls)
 assert 'secret' not in json.dumps(result)

@pytest.mark.parametrize('emergency',[False,True])
def test_slow_shutdown_policy(tmp_path,monkeypatch,emergency):
 (tmp_path/'var/log').mkdir(parents=True);(tmp_path/'var/run').mkdir()
 monkeypatch.setattr(manage,'ROOT',tmp_path);monkeypatch.setattr(manage,'lock',lambda p:contextlib.nullcontext())
 monkeypatch.setattr(manage,'config',lambda:dict(cpus=1,memory_gib=1024))
 own=NS(cpu_affinity=lambda *a:[0],create_time=lambda:1)
 monkeypatch.setattr(manage,'psutil',NS(Process=lambda *a:own))
 child=NS(pid=9876,stdout=io.BytesIO(b''),returncode=None);child.poll=lambda:child.returncode
 monkeypatch.setattr(manage.subprocess,'Popen',lambda *a,**k:child)
 monkeypatch.setattr(manage,'command',lambda *a:(['fake'],{}))
 handlers={};monkeypatch.setattr(manage.signal,'signal',lambda sig,fn:handlers.update({sig:fn}))
 kills=[];monkeypatch.setattr(manage.os,'killpg',lambda pid,sig:kills.append(sig))
 clock=[1];snapshots=[]
 def sleep(_):
  clock[0]+=31;snapshots.append(json.loads((tmp_path/'var/run/canary.json').read_text()))
  if clock[0]>=94:child.returncode=143
 monkeypatch.setattr(manage,'time',NS(time=lambda:clock[0],monotonic=lambda:clock[0],sleep=sleep))
 def resources():
  if clock[0]==1 and not emergency:handlers[signal.SIGTERM]()
  return {'total_rss_bytes':2048*1024**3 if emergency else 0}
 monkeypatch.setattr(manage,'resource_status',resources);monkeypatch.setattr(manage,'memory_victim',lambda r:'canary')
 monkeypatch.setattr(manage.diagnostics,'incident',lambda *a,**k:'/evidence')
 manage.supervise('canary')
 assert signal.SIGTERM in kills
 assert (signal.SIGKILL in kills)==emergency
 assert any(s.get('stop_pending') for s in snapshots)
 if emergency:assert any(s.get('unclean_stop') for s in snapshots)
 final=json.loads((tmp_path/'var/run/canary.json').read_text());assert final['state']=='stopped' and not final['stop_pending']

def test_explicit_force_receipt(tmp_path,monkeypatch):
 monkeypatch.setattr(manage,'ROOT',tmp_path);monkeypatch.setattr(manage,'lock',lambda p:contextlib.nullcontext())
 child_proc=NS(pid=7654,create_time=lambda:123,wait=lambda **k:None);parent=NS(send_signal=Mock(),wait=Mock())
 monkeypatch.setattr(manage,'identity',lambda p,child=False:child_proc if child else parent)
 kills=[];monkeypatch.setattr(manage.os,'killpg',lambda pid,sig:kills.append((pid,sig)))
 manage.stop('canary',force=True)
 saved=json.loads((tmp_path/'var/run/stop-receipts/canary.json').read_text())
 assert saved['unclean_stop'] and saved['pid']==7654
 assert kills==[(7654,signal.SIGKILL)]


def test_pending_stop_blocks_start(tmp_path,monkeypatch):
 monkeypatch.setattr(manage,'ROOT',tmp_path);monkeypatch.setattr(manage,'lock',lambda p:contextlib.nullcontext())
 (tmp_path/'var/run').mkdir(parents=True)
 (tmp_path/'var/run/canary.json').write_text(json.dumps({'stop_pending':True}))
 monkeypatch.setattr(manage,'identity',lambda p:object())
 launch=Mock();monkeypatch.setattr(manage.subprocess,'Popen',launch)
 with pytest.raises(RuntimeError,match='still stopping'):manage.start('canary')
 launch.assert_not_called()
