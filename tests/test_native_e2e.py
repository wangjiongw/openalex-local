"""Opt-in native engine check; CI runs this against both supported architectures."""
import json
import os
import socket
import subprocess
import sys
import time

import httpx
import pytest

from openalex_api.demo import make_snapshot


IDENTITY_SQL_CHECK = r"""
import json,copy
from openalex_api.config import ROOT
from openalex_api.manticore_api import search_request
from openalex_api.manticore_engine import Client
from openalex_api.identity_filters import search
from openalex_api.query import query
m=json.loads((ROOT/'var/generations/demo/searches/demo_catalogs/build.json').read_text())
c=Client()
for filters in ('author:A1,institution:I1','author:A1,institution:!I2','author:A2,institution:I1','year:2022,topic:T1'):
 n=search_request('works',query('works','Climate','title',filters),m['indexes']['works'].split(','),2,layout=m.get('physical_layout','legacy'))
 n['query'].pop('match_all',None);n['query'].setdefault('bool',{}).setdefault('must',[]).append({'in':{'id':list(range(1,13))}})
 expected=c.search(n);actual=search(c,n,True)
 assert actual['hits']['total']==expected['hits']['total'],(actual,expected)
 assert [(h['_id'],h['_score']) for h in actual['hits']['hits']]==[(h['_id'],h['_score']) for h in expected['hits']['hits']],(actual,expected)
 if actual['hits']['total']>2:
  left=copy.deepcopy(n);right=copy.deepcopy(n);left['options']['scroll']=expected['scroll'];right['options']['scroll']=actual['scroll']
  a=c.search(left);b=search(c,right,True)
  assert [h['_id'] for h in a['hits']['hits']]==[h['_id'] for h in b['hits']['hits']],(a,b)
c.close()

import copy,json
from openalex_api.manticore_engine import Client
from openalex_api.manticore_formats import create,project_native
from openalex_api.manticore_api import search_request
from openalex_api.identity_filters import search
from openalex_api.query import query
c=Client();table='oa_sql_parity_fixture'
a=[dict(author=dict(id='A1',display_name='John Smith'),institutions=[dict(id='I1')]),dict(author=dict(id='A2',display_name='Alice'),institutions=[dict(id='I2')])]
records=[dict(id='W1',title='Climate change',publication_year=2020,topics=[dict(id='T1'),dict(id='T2')],authorships=a,open_access=dict(is_oa=False)),dict(id='W2',title='Climate ocean',publication_year=2021,topics=[dict(id='T2')]),dict(id='W3',title='Ocean'),dict(id='W4',title='Climate')]
create(c,table);c.bulk(table,[project_native('works',dict(raw=json.dumps(r),version=1),'',[]) for r in records]);c.sql('FLUSH RAMCHUNK '+table)
try:
 bodies=[query('works',filters=f) for f in ['topic:T1','topic:T1|T2','topic:!T1','topic:null','topic:!null','id:!W1','year:2020..2021','year:null','is_oa:!false','author:A1,institution:I2','author:A1,institution:I1','author:A1,institution:!I2']]
 bodies += [query('works','Climate','title'),query('works','Climate','title','topic:T2',sort='year:desc')]
 for body in bodies:
  n=search_request('works',body,[table],2,layout='native_v2');n['query'].pop('match_all',None);n['query'].setdefault('bool',{}).setdefault('must',[]).append({'in':{'id':[1,2,3,4]}})
  left=c.search(n);right=search(c,n,True)
  expected=[(r['_id'],r['_score']) for r in left['hits']['hits']];actual=[(r['_id'],r['_score']) for r in right['hits']['hits']]
  assert (left['hits']['total'],expected)==(right['hits']['total'],actual),(body,left,right)
  if left['hits']['total']>2:
   one=copy.deepcopy(n);two=copy.deepcopy(n);one['options']['scroll']=left['scroll'];two['options']['scroll']=right['scroll']
   l=c.search(one);r=search(c,two,True)
   assert [h['_id'] for h in l['hits']['hits']]==[h['_id'] for h in r['hits']['hits']],(body,l,r)
 print('SQL/native parity passed for',len(bodies),'filter/ranking/scroll cases')
finally:c.sql('DROP TABLE '+table);c.close()
"""


@pytest.mark.skipif(os.environ.get('OPENALEX_NATIVE_E2E') != '1', reason='requires native engine downloads')
@pytest.mark.parametrize("profile", ["metadata", "earth_sciences"])
def test_native_pipeline_publish_restore_and_resume(tmp_path, profile):
    snapshot = tmp_path / 'snapshot'
    make_snapshot(snapshot)
    workspace = tmp_path / 'workspace.v1'
    command = [sys.executable, '-m', 'openalex_api.cli', '--workspace', str(workspace)]
    def run(*args):
        result = subprocess.run(command + list(args), timeout=900)
        if result.returncode:
            for log in (workspace / 'var/log').glob('*.log'):
                errors = [v for v in log.read_text(errors='replace').splitlines() if '<Error>' in v]
                print(log.name, '\n'.join(errors[-6:]), flush=True)
        result.check_returncode()
    run('init', '--snapshot', str(snapshot))
    sockets = [socket.socket() for _ in range(4)]
    try:
        for sock in sockets:
            sock.bind(('127.0.0.1', 0))
        api, ch, tcp, search = [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()
    path = workspace / 'config/local.json'
    cfg = json.loads(path.read_text())
    cfg.update(host='127.0.0.1', port=api, ch_url=f'http://127.0.0.1:{ch}',
               ch_tcp_port=tcp, manticore_url=f'http://127.0.0.1:{search}',
               cpus=2, workers=2, memory_gib=8, clickhouse_memory_gib=4, export_shard_rows=2, export_batch_rows=2)
    path.write_text(json.dumps(cfg))
    options = ['--profile', os.environ.get('OPENALEX_ENGINE_PROFILE', 'standard')]
    if os.environ.get('OPENALEX_ENGINE_CACHE'):
        options += ['--offline', '--cache', os.environ['OPENALEX_ENGINE_CACHE']]
    run('engines', 'ensure', *options, *(['--search'] if profile == 'earth_sciences' else []))
    try:
        run('pipeline', 'run', '--generation', 'demo', '--profile', profile, '--until', 'published')
        state = workspace / 'var/generations/demo/pipeline.json'
        before = state.read_bytes()
        run('pipeline', 'resume', '--generation', 'demo', '--profile', profile, '--until', 'published')
        assert state.read_bytes() == before
        if profile == 'earth_sciences':
            report = json.loads((workspace / 'var/generations/demo/searches/demo_earth/qualification/acceptance.json').read_text())
            assert report['passed'] and report['restored'] and report['performance_passed']
            assert report['validation_profile'] == 'partial-snapshot-smoke'
        else:
            assert not (workspace / 'var/engines/manticore-29.9.0').exists()
            assert not (workspace / 'exports/earth_collections').exists()
            assert json.loads(state.read_text())['completed'] == ['metadata', 'published']
        with httpx.Client(base_url=f'http://127.0.0.1:{api}', trust_env=False, headers={'X-API-Key': cfg['api_keys'][0]}) as client:
            assert client.get('/readyz').status_code == 200
            assert client.get('/').status_code == 200
            lookup = client.get('/v1/works/lookup', params={'identifier': '10.99999/demo.2'})
            lookup.raise_for_status()
            assert lookup.json()['doi'] == 'https://doi.org/10.99999/demo.2'
            assert client.get('/v1/works/W2/references').status_code == 200
            assert client.get('/v1/works/W1/citations').status_code == 200
            if profile == 'earth_sciences':
                result = client.get('/v1/works', params={'search': 'syntheticpaper2', 'search_field': 'title'})
                result.raise_for_status()
                assert any(v['doi'] == 'https://doi.org/10.99999/demo.2' for v in result.json()['results'])
            run('start', 'export-worker')
            requests = [dict(query=dict(route='/works',params={'filter':'publication_year:2022'}),formats=['jsonl.gz','csv','ids.txt','doi.txt'])]
            if profile == 'earth_sciences':
                requests.append(dict(query=dict(route='/v1/works',params={})))
                requests.append(dict(query=dict(route='/v1/works',params={'search':'Climate','search_field':'title','filter':'language:en,author:A1,institution:I1'})))
            for payload in requests:
                reply=client.post('/v1/exports',json=payload)
                reply.raise_for_status()
                job=reply.json()
                for _ in range(90):
                    job=client.get('/v1/exports/'+job['id']).json()
                    if job['state'] in ('completed','failed'):break
                    time.sleep(.3)
                assert job['state']=='completed',job
                assert job['exported']==(12 if payload['query']['route']=='/v1/works' else 3)
                assert len(job['receipts'])>=2
                for file in job['files']:
                    downloaded=client.get('/v1/exports/'+job['id']+'/files/'+file['name'])
                    downloaded.raise_for_status()
                    import hashlib
                    assert hashlib.sha256(downloaded.content).hexdigest()==file['sha256']
            detail = client.get('/works/W2').json()
            assert detail['unknown_future_field'] == {'preserved': 'yes'}
            if profile == 'earth_sciences':
                arxiv_input=workspace/'arxiv-fixture.json'
                arxiv_rows=[dict(id=f'1001.000{i}',title=f'Synthetic climate arxiv {i}',abstract='Climate forecasting research',authors='Demo Scientist',categories='physics.ao-ph cs.AI',doi='10.99999/demo.2' if i==1 else None,versions=[dict(version='v1',created='Fri, 1 Jan 2010 00:00:00 GMT')],unknown_source_field={'keep':True}) for i in range(1,4)]
                arxiv_input.write_text('\n'.join(json.dumps(r) for r in arxiv_rows))
                pointer_before=(workspace/'var/active.json').read_bytes()
                search_before=(workspace/'var/generations/demo/search-active.json').read_bytes()
                run('arxiv','plan','--release','arxiv_fixture','--input',str(arxiv_input),'--generation','demo')
                run('arxiv','run','--release','arxiv_fixture','--until','ingest')
                run('arxiv','resume','--release','arxiv_fixture')
                run('arxiv','publish','--release','arxiv_fixture')
                assert (workspace/'var/active.json').read_bytes()==pointer_before
                assert (workspace/'var/generations/demo/search-active.json').read_bytes()==search_before
                reply=client.get('/v1/arxiv/works',params={'search':'climate','year':'2010..2011','category':'cs.AI','per_page':1})
                reply.raise_for_status();data=reply.json();assert data['meta']['count']==3,data
                keys=[r['id'] for r in data['results']]
                while data['meta']['next_cursor']:
                    reply=client.get('/v1/arxiv/works',params={'search':'climate','year':'2010..2011','category':'cs.AI','per_page':1,'cursor':data['meta']['next_cursor']})
                    reply.raise_for_status();data=reply.json();keys += [r['id'] for r in data['results']]
                assert len(keys)==len(set(keys))==3,keys
                reply=client.get('/v1/arxiv/lookup',params={'identifier':'1001.0001v2'});reply.raise_for_status()
                ar=reply.json();assert ar['version_links']['preferred_openalex_id']=='W2' and ar['raw_source']['unknown_source_field']=={'keep':True},ar
                reply=client.get('/v1/works/W2/versions');reply.raise_for_status();assert reply.json()['results'][0]['id']=='arxiv:1001.0001'
                created=client.post('/v1/exports',json=dict(source='arxiv',query=dict(route='/v1/arxiv/works',params=dict(search='climate')),formats=['jsonl.gz','csv','doi.txt','ids.txt']))
                created.raise_for_status();job=created.json()['id']
                for _ in range(120):
                    state=client.get('/v1/exports/'+job).json()
                    if state['state'] in ('completed','failed'):break
                    time.sleep(.2)
                assert state['state']=='completed' and state['exported']==3,state
                manifest=client.get(f'/v1/exports/{job}/files/manifest.json');manifest.raise_for_status()
                assert manifest.json()['plan']['source_release']=='arxiv_fixture'
                run('stop','manticore');run('start','manticore')
                for _ in range(60):
                    reply=client.get('/v1/arxiv/works',params={'search':'climate'})
                    if reply.status_code==200:break
                    time.sleep(.2)
                reply.raise_for_status();assert reply.json()['meta']['count']==3
                assert client.get('/v1/works',params={'search':'climate'}).status_code==200
        if profile=='earth_sciences':
            run('catalogs','build','--generation','demo','--build','demo_catalogs','--parent','demo_earth')
            manifest=json.loads((workspace/'var/generations/demo/searches/demo_catalogs/build.json').read_text())
            assert manifest['state']=='sealed' and manifest['entities']['authors']['count']==1 and manifest['entities']['institutions']['count']==1
            run('catalogs','build','--generation','demo','--build','demo_catalogs','--parent','demo_earth')
            subprocess.run([sys.executable,'-c',IDENTITY_SQL_CHECK],env=os.environ|dict(OPENALEX_HOME=str(workspace)),check=True,timeout=60)
            user_jobs_before=len(list((workspace/'exports/query').glob('*/state.json')))
            run('catalogs','validate','--generation','demo','--build','demo_catalogs')
            assert len(list((workspace/'exports/query').glob('*/state.json')))==user_jobs_before
            assert any((workspace/'var/generations/demo/searches/demo_catalogs/qualification/export-workspace/exports/query').glob('*/state.json'))
            active_before=(workspace/'var/active.json').read_bytes()
            run('catalogs','publish','--generation','demo','--build','demo_catalogs')
            assert (workspace/'var/active.json').read_bytes()==active_before
            # A separate API can pin the qualified catalogs explicitly.
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0));candidate_port=sock.getsockname()[1]
            env=os.environ|dict(OPENALEX_HOME=str(workspace),OPENALEX_GENERATION='demo',OPENALEX_SEARCH_BUILD='demo_catalogs',OPENALEX_SEARCH_VALIDATION='1')
            proc=subprocess.Popen([sys.executable,'-m','uvicorn','openalex_api.api:app','--port',str(candidate_port)],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                with httpx.Client(base_url=f'http://127.0.0.1:{candidate_port}',trust_env=False,headers={'X-API-Key':cfg['api_keys'][0]}) as candidate:
                    for _ in range(60):
                        try:
                            if candidate.get('/healthz').status_code==200:break
                        except httpx.HTTPError:pass
                        time.sleep(.2)
                    for entity,name,key in [('authors','Demo Scientist','A1'),('institutions','Demo University','I1')]:
                        for route,params in [(f'/v1/{entity}',{'search':name}), (f'/{entity}',{'search':name})]:
                            reply=candidate.get(route,params=params)
                            reply.raise_for_status()
                            assert reply.json()['results'][0]['id'].endswith('/'+key),reply.json()
            finally:
                proc.terminate();proc.wait(timeout=15)
    finally:
        for service in ('export-worker', 'api', 'manticore', 'clickhouse'):
            run('stop', service)
