"""Source-derived acceptance with an explicit partial/demo versus full load policy."""
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psutil

from .backend import Backend, database, sql_string
from .config import ROOT, CODE_ROOT, config, read_generation, atomic_json
from .earth_delivery import SELECT, check, load_cases
from .manticore_build import members, file_hash, verify_segment
from .model import canonical_id, doi
from .search_versions import build_path, read


def fixtures(g, manifest, output):
    collection = ROOT / 'exports/earth_collections' / g / manifest['collection']
    state = json.loads((collection / 'manifest.json').read_text())
    keys = []
    for group in manifest['groups']:
        for item in __import__('itertools').islice(members(collection, state, group), 4):
            keys.append(canonical_id(item['id']))
    if not keys:
        raise ValueError('no indexed papers available for acceptance')
    b = Backend(writer=True)
    cases = []
    def add(name, phase, path, params, **expected):
        cases.append(dict(name=name, phase=phase, path=path, params=params, expected=expected))
    try:
        records = b.details(g, 'works', keys)
        targets = set()
        for key in keys:
            work = records[key]
            add('detail:' + key, 'metadata', '/works/' + key, {'select': SELECT}, shape='record', ids=[key])
            identifier = doi(work.get('doi'))
            if identifier:
                ids = b.resolve(g, 'works', 'doi', identifier)
                targets.update(ids)
                add('doi:' + key, 'metadata', '/v1/works/lookup', {'identifier': identifier, 'select': SELECT},
                    shape='ambiguous' if len(ids) > 1 else 'record', ids=ids[:100], status=409 if len(ids) > 1 else 200)
            refs = sorted({canonical_id(v) for v in work.get('referenced_works') or []}, key=lambda v: int(v[1:]))
            targets.update(refs[:100])
            add('references:' + key, 'metadata', '/v1/works/' + key + '/references', {'per_page': 100, 'select': SELECT}, shape='graph', ids=refs[:100])
            incoming = b.ch(f"SELECT citing FROM {database(g)}.references_in FINAL WHERE cited={int(key[1:])} ORDER BY citing LIMIT 3 FORMAT JSONEachRow")
            ids = ['W' + str(v['citing']) for v in incoming]
            targets.update(ids)
            add('citations:' + key, 'metadata', '/v1/works/' + key + '/citations', {'per_page': 3, 'select': SELECT}, shape='graph', ids=ids)
            related = [canonical_id(v) for v in work.get('related_works') or []]
            targets.update(related)
            add('related:' + key, 'metadata', '/v1/works/' + key + '/related-works', {'select': SELECT}, shape='graph', ids=related)
            title = work.get('title') or work.get('display_name') or ''
            if title:
                params = dict(search=title, search_field='title', search_scope='earth_sciences', per_page=25, select=SELECT)
                add('title:' + key, 'search', '/v1/works', params, shape='search', contains=key)
                author = next(((v.get('author') or {}).get('display_name') for v in work.get('authorships') or [] if (v.get('author') or {}).get('display_name')), None)
                if author:
                    add('author:' + key, 'search', '/v1/works', params | {'author_search': author}, shape='search', contains=key, author=author)
                year, oa = work.get('publication_year'), (work.get('open_access') or {}).get('is_oa')
                if isinstance(year, int) and isinstance(oa, bool):
                    add('year-oa:' + key, 'search', '/v1/works', params | {'filter': f'year:{year},is_oa:{str(oa).lower()}'}, shape='search', contains=key, year=year, oa=oa)
            for field, value in [('topic', (work.get('topics') or [{}])[0].get('id')), ('source', ((work.get('primary_location') or {}).get('source') or {}).get('id'))]:
                if value:
                    add(field + ':' + key, 'search', '/v1/works', {'filter': f'id:{key},{field}:{canonical_id(value)}', 'select': SELECT}, shape='search', ids=[key])
        for start in range(0, len(targets), 100):
            records.update(b.details(g, 'works', sorted(targets)[start:start+100]))
    finally:
        b.close()
    if read_generation(g).get('snapshot_complete', True):
        from .search_acceptance import EARTH_GOLD
        for gold in EARTH_GOLD:
            bibliography = {field: gold[field] for field in ('doi','title','year')}
            for label, params in [
                ('complete-title',dict(search=gold['title'],search_field='title',per_page=10)),
                ('title-fragment',dict(search=' '.join(gold['title'].split()[:6]),search_field='title',per_page=25)),
                ('author-year',dict(search=gold['title'],search_field='title',author_search=gold['author'],filter='year:'+str(gold['year']),per_page=10)),
            ]:
                add('gold:'+label+':'+gold['doi'],'search','/v1/works',params|{'select':SELECT},shape='search',bibliography=bibliography)
        add('negative:unmatched-title','search','/v1/works',dict(search='openalexlocalnonexistentgold00000001',search_field='title',select=SELECT),shape='search',ids=[])
    fixture = dict(schema=1, generation=g, search_build=manifest['build'], cases=cases,
                   records={k: {field: work.get(field) for field in SELECT.split(',')} for k, work in records.items()},
                   snapshot_complete=read_generation(g).get('snapshot_complete', False),
                   collection_signature=manifest['collection_signature'],
                   limitation='Source consistency checks do not establish retrieval precision/recall.')
    atomic_json(output / 'cases.json', fixture)
    return fixture


@contextmanager
def candidate_api(g, build, output):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = os.environ | dict(OPENALEX_HOME=str(ROOT), PYTHONPATH=str(CODE_ROOT),
                             OPENALEX_GENERATION=g, OPENALEX_SEARCH_BUILD=build, OPENALEX_SEARCH_VALIDATION='1')
    output.mkdir(parents=True, exist_ok=True)
    with (output / 'candidate-api.log').open('ab') as log:
        process = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'openalex_api.api:app', '--host', '127.0.0.1', '--port', str(port), '--no-access-log'], env=env, stdout=log, stderr=log)
        try:
            url = 'http://127.0.0.1:' + str(port)
            for _ in range(120):
                if process.poll() is not None:
                    raise RuntimeError('candidate API exited; inspect its log')
                try:
                    r = httpx.get(url + '/healthz', timeout=1, trust_env=False)
                    r.raise_for_status()
                    break
                except httpx.HTTPError:
                    time.sleep(.25)
            else:
                raise RuntimeError('candidate API startup timed out')
            yield url
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


def qualify(g, build):
    from .operations import restore
    from .manticore_build import save
    manifest = read(g, build)
    if manifest['state'] not in ('sealed', 'validated'):
        raise ValueError('qualification requires a sealed index')
    output = build_path(g, build) / 'qualification'
    output.mkdir(parents=True, exist_ok=True)
    fixture = fixtures(g, manifest, output)
    complete = read_generation(g).get('snapshot_complete', True)
    with candidate_api(g, build, output) as url:
        report = check(output / 'cases.json', output, url, 'all', 1800 if complete else 0)
        if not report['passed']:
            raise RuntimeError('functional or full load acceptance failed: ' + str(output / 'acceptance.json'))
        if not complete:
            seconds = max(5, int(config().get('validation_smoke_seconds', 10)))
            matrix = [asyncio.run(load_cases(url, fixture['cases'], fixture['records'], n, seconds,
                       {'X-API-Key': config()['api_keys'][0]})) for n in (1, 8)]
            passed = all(v['requests'] > 0 and not v['errors'] and v['p95_seconds'] <= 2 and v['p99_seconds'] <= 5 for v in matrix)
            report.update(matrix=matrix, performance_passed=passed, performance_tested=True,
                          validation_profile='partial-snapshot-smoke',
                          limitations=report['limitations'] + ['Partial snapshot load smoke is not full production qualification.'])
            if not passed:
                atomic_json(output / 'acceptance.json', report)
                raise RuntimeError('partial-snapshot concurrent load smoke failed')
    report.setdefault('validation_profile', 'full-snapshot-30-minute-load')
    restore_output = output / ('restore-' + str(time.time_ns()))
    restored = restore(g, build, restore_output, allow_sealed=True)
    if not restored['passed']:
        raise RuntimeError('fresh-engine recovery failed')
    # Bind the successful API, load and fresh-engine checks to the current implementation.
    report.update(passed=True, restored=True, restore_result=str(restore_output / 'result.json'))
    atomic_json(output / 'acceptance.json', report)
    atomic_json(build_path(g, build) / 'observation.json', dict(state='complete', required_seconds=0,
                long_observation_skipped=True, elapsed_seconds=0, errors=0,
                projection_signature=manifest['projection_signature'], validation_profile=report['validation_profile'],
                restore_verified=True, mixed_load_required=False))
    manifest.update(state='validated', validated_at=time.time())
    save(manifest)
    return report


def verify_remote(g, build, output, base_url=None, remote=False):
    url = base_url or 'http://127.0.0.1:' + str(config()['port'])
    host = urlparse(url).hostname
    addresses = {v.address for rows in psutil.net_if_addrs().values() for v in rows if v.family == socket.AF_INET}
    if remote and (not host or set(socket.gethostbyname_ex(host)[2]) & (addresses | {'127.0.0.1'})):
        raise ValueError('remote-client verification must run on a different client host')
    result = check(build_path(g, build) / 'qualification/cases.json', output, url, 'all')
    result.update(cross_pod_tested=bool(remote), client_hostname=socket.gethostname())
    atomic_json(output / 'acceptance.json', result)
    if not result['passed']:
        raise RuntimeError('remote verification failed: ' + str(output))
    return result
