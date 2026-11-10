"""Measure native-layout batches on a freshly owned isolated engine; retain all correctness checks."""
import argparse,itertools,time,multiprocessing,tempfile,subprocess,json,os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from openalex_api.config import ROOT,atomic_json
from openalex_api.earth_control import read_json
from openalex_api.manticore_formats import create,engine_env
from openalex_api.manticore_engine import Client
from openalex_api.manticore_build import members,write_batch,seal
from openalex_api.backend import Backend,sql_string

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--rows',type=int,default=20000);args=parser.parse_args()
 if not 20000<=args.rows<=100000:parser.error('rows must be 20,000 through 100,000')
 out=ROOT/'reports/manticore-throughput-20261108'/('parallel-'+str(time.time_ns()));out.mkdir(parents=True)
 atomic_json(out.parent/'latest-probe.json',dict(path=str(out)))
 root=Path(tempfile.mkdtemp(prefix='oa-native-speed-'))
 for d in ['data','binlog']:(root/d).mkdir()
 conf=root/'searchd.conf';conf.write_text(f'searchd {{\nlisten = 127.0.0.1:19338:http\nlog = {root}/searchd.log\npid_file = {root}/pid\ndata_dir = {root}/data\nbinlog_path = {root}/binlog\nbinlog_flush = 1\nthreads = 16\nmax_packet_size = 64M\nbuddy_path =\n}}\n')
 log=(root/'stdout.log').open('w');proc=subprocess.Popen([str(ROOT/'var/engines/manticore-29.9.0/usr/bin/searchd'),'--config',str(conf),'--nodetach'],env=engine_env(),stdout=log,stderr=log)
 atomic_json(out/'probe-engine.json',dict(pid=proc.pid,path=str(root)))
 c=Client('http://127.0.0.1:19338',timeout=300)
 try:
  for _ in range(100):
   if proc.poll() is not None:raise RuntimeError('owned test engine failed to start: '+str(root))
   try:c.rows('SHOW TABLES');break
   except Exception:time.sleep(.1)
  collection=ROOT/'exports/earth_collections/full_20260908/earth_scope_v1'
  selected=list(itertools.islice(members(collection,read_json(collection/'manifest.json'),'02_broad_interdisciplinary',100000),args.rows))
  b=Backend(writer=True);rows=[];fetch=[]
  for offset in range(0,len(selected),2000):
   batch=selected[offset:offset+2000];ids=','.join(sql_string(r['id']) for r in batch);t=time.monotonic()
   rows.extend(b.ch("SELECT id,raw,version,digest FROM oa_full_20260908.entities WHERE entity='works' AND id IN ("+ids+") ORDER BY id FORMAT JSONEachRow"));fetch.append(time.monotonic()-t)
  b.close();print('source_read_seconds',sum(fetch),flush=True)
  assert len(rows)==len(selected)==len({r['id'] for r in rows})
  membership={r['id']:r for r in selected};results=[]
  for trial,(workers,batch_size,bulk_workers,memory) in enumerate([(4,2000,1,'256M'),(8,5000,4,'256M'),(8,5000,4,'2G'),(8,5000,2,'2G'),(16,5000,4,'2G'),(4,2000,1,'256M')]):
   name=f'oa_speed_{trial}_w{workers}_b{batch_size}';create(c,name);c.sql(f"ALTER TABLE {name} rt_mem_limit='{memory}'");path=out/name
   segment=dict(index=name,entity='works',group='02_broad_interdisciplinary',physical_layout='native_v2',count=0,batches=[])
   with ProcessPoolExecutor(workers,mp_context=multiprocessing.get_context('spawn')) as pool:
    t=time.monotonic();timings=[]
    for offset in range(0,len(rows),batch_size):
     batch=rows[offset:offset+batch_size]
     timings.append(write_batch(c,None,path,segment,batch,[membership[r['id']] for r in batch],pool,bulk_workers))
    seconds=time.monotonic()-t
   result=dict(workers=workers,batch_rows=batch_size,bulk_workers=bulk_workers,rt_mem_limit=memory,rows=len(rows),seconds=seconds,rows_per_second=len(rows)/seconds,
      stages={k:sum(t[k] for t in timings) for k in timings[0]},source_seconds=sum(fetch))
   assert int(c.rows('SELECT count(*) n FROM '+name)[0]['n'])==len(rows)
   if trial==2:
    t=time.monotonic();seal(c,path,segment);result['seal_seconds']=time.monotonic()-t;result['restored']=segment['restore_verified']
   results.append(result);atomic_json(out/'probe-results.json',results);print(json.dumps(result),flush=True)
   c.sql('DROP TABLE '+name)
 finally:
  c.close();proc.terminate();proc.wait(timeout=30);log.close()
if __name__=='__main__':main()
