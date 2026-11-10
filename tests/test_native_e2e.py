"""Opt-in native engine check; CI runs this against both supported architectures."""
import json
import os
import socket
import subprocess
import sys

import httpx
import pytest

from openalex_api.demo import make_snapshot


@pytest.mark.skipif(os.environ.get('OPENALEX_NATIVE_E2E') != '1', reason='requires native engine downloads')
def test_native_pipeline_publish_restore_and_resume(tmp_path):
    snapshot = tmp_path / 'snapshot'
    make_snapshot(snapshot)
    workspace = tmp_path / 'workspace'
    command = [sys.executable, '-m', 'openalex_api.cli', '--workspace', str(workspace)]
    def run(*args):
        subprocess.run(command + list(args), check=True, timeout=900)
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
               cpus=2, workers=2, memory_gib=8, clickhouse_memory_gib=4)
    path.write_text(json.dumps(cfg))
    options = ['--profile', os.environ.get('OPENALEX_ENGINE_PROFILE', 'standard')]
    if os.environ.get('OPENALEX_ENGINE_CACHE'):
        options += ['--offline', '--cache', os.environ['OPENALEX_ENGINE_CACHE']]
    run('engines', 'ensure', *options)
    try:
        run('pipeline', 'run', '--generation', 'demo', '--until', 'published')
        state = workspace / 'var/generations/demo/pipeline.json'
        before = state.read_bytes()
        run('pipeline', 'resume', '--generation', 'demo', '--until', 'published')
        assert state.read_bytes() == before
        report = json.loads((workspace / 'var/generations/demo/searches/demo_earth/qualification/acceptance.json').read_text())
        assert report['passed'] and report['restored'] and report['performance_passed']
        assert report['validation_profile'] == 'partial-snapshot-smoke'
        with httpx.Client(base_url=f'http://127.0.0.1:{api}', trust_env=False, headers={'X-API-Key': cfg['api_keys'][0]}) as client:
            assert client.get('/readyz').status_code == 200
            assert client.get('/').status_code == 200
            result = client.get('/v1/works', params={'search': 'syntheticpaper2', 'search_field': 'title'})
            result.raise_for_status()
            assert any(v['doi'] == 'https://doi.org/10.99999/demo.2' for v in result.json()['results'])
            detail = client.get('/works/W2').json()
            assert detail['unknown_future_field'] == {'preserved': 'yes'}
    finally:
        for service in ('api', 'manticore', 'clickhouse'):
            run('stop', service)
