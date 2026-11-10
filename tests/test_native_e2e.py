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
        if profile=='earth_sciences':
            run('catalogs','build','--generation','demo','--build','demo_catalogs','--parent','demo_earth')
            manifest=json.loads((workspace/'var/generations/demo/searches/demo_catalogs/build.json').read_text())
            assert manifest['state']=='sealed' and manifest['entities']['authors']['count']==1 and manifest['entities']['institutions']['count']==1
            run('catalogs','build','--generation','demo','--build','demo_catalogs','--parent','demo_earth')
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
