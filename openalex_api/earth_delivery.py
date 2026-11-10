from .config import CODE_ROOT
"""Reproducible samples and HTTP checks for the versioned Earth collections."""
import asyncio
import bisect
import collections
import csv
import gzip
import itertools
import json
import random
import shlex
import socket
import time
from pathlib import Path

import httpx

from . import earth_collections as ec
from .backend import Backend, database, sql_list, sql_string
from .config import atomic_json, config, generation_path
from .earth_control import read_json
from .model import doi, normalize_title
from .proofs import digest, file_hash
from .search_acceptance import percentile

SEED = 20261105
SELECT = 'id,doi,title,publication_year,authorships,open_access'


def context(g, collection):
    path = ec.collection_path(g, collection)
    state = read_json(path / 'manifest.json')
    b = Backend(writer=True)
    try:
        ec.initialize(b, g, collection)
    finally:
        b.close()
    return path, state, ec.load_catalog(g), read_json(path / 'journals.json')['journals']


def sample_exports(path, state, per_group, seed=SEED):
    """Draw uniform row ordinals using receipt counts, not an ID-ordered prefix."""
    files = {g: [] for g in ec.GROUPS}
    counts = collections.Counter()
    cursors = {'direct': '', 'author': ''}
    scanned = collections.Counter()
    for seq in range(state['blocks']):
        r = read_json(path / 'blocks' / f'{seq:08d}.json')
        phase = r['phase']
        if r['sequence'] != seq or r['previous_after'] != cursors[phase]:
            raise RuntimeError('invalid export receipt chain')
        cursors[phase] = r['after']; scanned[phase] += r['source_count']; counts.update(r['counts'])
        for f in r['files']:
            target = path / f['path']
            if not target.resolve().is_relative_to(path.resolve()):
                raise ValueError('export file escapes collection')
            if f['path'].endswith('.jsonl.gz'):
                files[f['group']].append(f)
    if (dict(counts) != state['counts'] or scanned['direct'] != state['scanned']
            or scanned['author'] != state['author_scanned']):
        raise RuntimeError('manifest/receipt counts differ')
    result = []
    for i, group in enumerate(ec.GROUPS):
        ends = list(itertools.accumulate(f['count'] for f in files[group]))
        n = ends[-1] if ends else 0
        ordinals = sorted(random.Random(seed + i).sample(range(n), min(per_group, n)))
        selected = collections.defaultdict(set)
        for ordinal in ordinals:
            fi = bisect.bisect_right(ends, ordinal)
            selected[fi].add(ordinal - (ends[fi - 1] if fi else 0))
        for fi, positions in selected.items():
            item = files[group][fi]; target = path / item['path']
            if target.stat().st_size != item['bytes'] or file_hash(target) != item['sha256']:
                raise RuntimeError('sample shard hash differs: ' + item['path'])
            found = 0
            with gzip.open(target, 'rt', encoding='utf-8') as stream:
                for pos, line in enumerate(itertools.islice(stream, max(positions) + 1)):
                    if pos in positions:
                        record = json.loads(line)
                        if record['group'] != group:
                            raise RuntimeError('sample group differs from receipt')
                        result.append(dict(record=record, file=item['path'], row=pos)); found += 1
            if found != len(positions):
                raise RuntimeError('sample shard is truncated')
    return result


def source_rows(b, g, keys):
    rows = {}
    for offset in range(0, len(keys), 100):
        batch = b.ch(f"SELECT id,raw,version,digest FROM {database(g)}.entities FINAL "
                     f"WHERE entity='works' AND id IN {sql_list(keys[offset:offset+100])} FORMAT JSONEachRow")
        rows.update({r['id']: r for r in batch})
    if set(rows) != set(keys):
        raise RuntimeError('sample metadata records are missing')
    return rows


def prepare(g, collection, output):
    path, state, catalog, registry = context(g, collection)
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    samples = sample_exports(path, state, 20)
    if any(sum(s['record']['group'] == group for s in samples) < 20 for group in ec.GROUPS):
        raise RuntimeError('at least 20 committed records per group are required')
    keys = [s['record']['openalex_id'].rsplit('/', 1)[-1] for s in samples]
    b = Backend(writer=True)
    try:
        raw = source_rows(b, g, keys)
        membership = {r['id']: r for r in b.ch(f"SELECT id,owner FROM {catalog['table']} FINAL WHERE id IN {sql_list(keys)} FORMAT JSONEachRow")}
        records = {k: json.loads(row['raw']) for k, row in raw.items()}
        aliases = b.resolve_many(g, 'works', 'doi', [doi(r.get('doi')) for r in records.values() if doi(r.get('doi'))])
        cases = []
        targets = set()
        def add(name, phase, route, params, **expected):
            cases.append(dict(name=name, phase=phase, path=route, params=params, expected=expected))
        for key in keys:
            r = records[key]; title = r.get('title') or r.get('display_name') or ''
            add('detail:' + key, 'metadata', '/works/' + key, {'select': SELECT}, shape='record', ids=[key])
            normalized = doi(r.get('doi'))
            if normalized:
                ids = aliases.get(normalized, [])
                add('doi:' + key, 'metadata', '/v1/works/lookup', {'identifier': normalized, 'select': SELECT},
                    shape='ambiguous' if len(ids) > 1 else 'record', ids=ids[:100], status=409 if len(ids) > 1 else 200)
                targets.update(ids[:100])
            year = r.get('publication_year'); oa = (r.get('open_access') or {}).get('is_oa')
            if isinstance(year, int):
                filters = 'id:' + key + ',publication_year:' + str(year)
                if isinstance(oa, bool): filters += ',open_access.is_oa:' + str(oa).lower()
                add('metadata-filter:' + key, 'metadata', '/works', {'filter': filters, 'select': SELECT}, shape='list', ids=[key], year=year, oa=oa)
                add('negative-year:' + key, 'metadata', '/works', {'filter': 'id:' + key + ',publication_year:' + str(year + 1), 'select': SELECT}, shape='list', ids=[])
            for kind, fixed, other in [('references', 'citing', 'cited'), ('citations', 'cited', 'citing')]:
                table = 'references_out' if kind == 'references' else 'references_in'
                edges = b.ch(f"SELECT {other} target FROM {database(g)}.{table} FINAL WHERE {fixed}={int(key[1:])} ORDER BY {other} LIMIT 7 FORMAT JSONEachRow")
                ids = ['W' + str(x['target']) for x in edges]
                add(kind + ':' + key, 'metadata', '/v1/works/' + key + '/' + kind, {'per_page': 3, 'select': SELECT},
                    shape='graph', ids=ids[:3], more=len(ids) > 3, next_ids=ids[3:6] if len(ids) > 3 else None)
                targets.update(ids[:6])
            ids = [v.rsplit('/', 1)[-1] for v in r.get('related_works') or []]
            add('related:' + key, 'metadata', '/v1/works/' + key + '/related-works', {'select': SELECT}, shape='graph', ids=ids)
            targets.update(ids)
            if membership.get(key, {}).get('owner') == 'earth_sciences' and 30 <= len(title) <= 2000:
                params = dict(search=title, search_field='title', search_scope='earth_sciences', per_page=25, select=SELECT)
                add('title:' + key, 'search', '/v1/works', params, shape='search', contains=key)
                author = next((a.get('author', {}).get('display_name') for a in r.get('authorships') or [] if a.get('author', {}).get('display_name')), None)
                if author and len(author) <= 500:
                    add('author:' + key, 'search', '/v1/works', dict(search=author, search_field='author', search_scope='earth_sciences', per_page=25, select=SELECT), shape='search', author=author)
                    combined = params | dict(author_search=author)
                    if isinstance(year, int): combined['filter'] = 'year:' + str(year)
                    add('title-author-year:' + key, 'search', '/v1/works', combined, shape='search', contains=key, year=year)
                if isinstance(year, int) and isinstance(oa, bool):
                    add('title-year-oa:' + key, 'search', '/v1/works', params | {'filter': f'year:{year},is_oa:{str(oa).lower()}'}, shape='search', contains=key, year=year, oa=oa)
        for offset in range(0, len(targets), 100):
            records.update(b.details(g, 'works', sorted(targets)[offset:offset+100]))
        from .search_acceptance import EARTH_GOLD
        for gold in EARTH_GOLD:
            ids=b.resolve(g,'works','doi',gold['doi'])
            known=b.details(g,'works',ids);records.update(known)
            key=next((k for k,r in known.items() if normalize_title(r.get('title'))==normalize_title(gold['title']) and r.get('publication_year')==gold['year']),None)
            if key is None:raise RuntimeError('independent bibliography differs: '+gold['doi'])
            add('gold-doi:'+key,'metadata','/v1/works/lookup',{'identifier':gold['doi'],'select':SELECT},
                shape='ambiguous' if len(ids)>1 else 'record',ids=ids[:100],status=409 if len(ids)>1 else 200,bibliography=gold)
            add('gold-title-author:'+key,'search','/v1/works',dict(search=gold['title'],search_field='title',
                author_search=gold['author'],filter='year:'+str(gold['year']),search_scope='earth_sciences',per_page=25,select=SELECT),
                shape='search',contains=key,year=gold['year'])
    finally:
        b.close()
    fixture = dict(schema=1, generation=g, collection=collection, catalog_signature=catalog['signature'],
                   collection_signature=state['signature'], snapshot_blocks=state['blocks'], snapshot_complete=state['state'] == 'complete',
                   seed=SEED, human_reviewed=False, papers=[dict(id=k, group=s['record']['group'], file=s['file'], row=s['row']) for k,s in zip(keys,samples)],
                   records={k:{field:r.get(field) for field in SELECT.split(',')} for k,r in records.items()}, cases=cases,
                   independent_bibliography=__import__('openalex_api.search_acceptance', fromlist=['EARTH_GOLD']).EARTH_GOLD)
    atomic_json(output / 'cases.json', fixture)
    write_curl(output, fixture)
    return dict(path=str(output), papers=len(keys), cases=len(cases), signature=digest(fixture))


def write_curl(output, fixture):
    lines = ['#!/usr/bin/env bash', 'set -euo pipefail', ': "${OPENALEX_API_KEY:?Set OPENALEX_API_KEY in this client terminal}"',
             'BASE_URL="${OPENALEX_BASE_URL:-http://127.0.0.1:8088}"',
             '# Run from the other Pod (CLIENT_IP); results are not claimed as tested until returned.',
             '# Set OPENALEX_CHECK_SEARCH=1 only after /status reports published Earth search.',
             'curl --fail-with-body --silent --show-error "$BASE_URL/healthz"',
             'curl --fail-with-body --silent --show-error -H "X-API-Key: $OPENALEX_API_KEY" "$BASE_URL/status"']
    for prefix in ('doi:', 'references:', 'related:', 'title-author-year:', 'title-year-oa:'):
        case = next((c for c in fixture['cases'] if c['name'].startswith(prefix) and c['expected'].get('status',200) == 200), None)
        if case:
            if case['phase'] == 'search': lines.append('if [ "${OPENALEX_CHECK_SEARCH:-0}" = 1 ]; then')
            lines.append('# ' + case['name'] + (' (requires published Earth search)' if case['phase'] == 'search' else ''))
            args = ' '.join('--data-urlencode ' + shlex.quote(k + '=' + str(v)) for k,v in case['params'].items())
            lines.append('curl --fail-with-body --silent --show-error --get -H "X-API-Key: $OPENALEX_API_KEY" "$BASE_URL' + case['path'] + '" ' + args)
            if case['phase'] == 'search': lines.append('fi')
    (output / 'cross-pod-curl.sh').write_text('\n'.join(lines) + '\n')
    atomic_json(output / 'curl-expectations.json', dict(client_ip='CLIENT_IP', tested=False,
                generation=fixture['generation'], expected_metadata_ready=True, expected_search_ready='required for search examples',
                limitations=['Run in the client Pod; a server-local request does not prove cross-Pod connectivity.']))


def validate_response(case, response, records):
    expected = case['expected']
    if response.status_code != expected.get('status', 200):
        raise AssertionError('HTTP ' + str(response.status_code))
    data = response.json(); shape = expected['shape']
    if shape == 'record': values = [data]
    elif shape == 'ambiguous': values = data['candidates']
    elif shape == 'graph':
        values = []
        for item in data['results']:
            key = item['id'].rsplit('/', 1)[-1]
            if item['resolved'] != (key in records): raise AssertionError('graph resolution differs')
            values.append(item['work'] if item['resolved'] else {'id': item['id']})
    else: values = data['results']
    ids = [v['id'].rsplit('/',1)[-1] for v in values]
    if len(values) < expected.get('min_results', 0): raise AssertionError('too few results')
    if 'ids' in expected and ids != expected['ids']: raise AssertionError('result identities/order differ')
    if expected.get('contains') and expected['contains'] not in ids: raise AssertionError('expected work absent from top 25')
    if expected.get('bibliography'):
        gold=expected['bibliography']
        if not any(doi(v.get('doi'))==gold['doi'] and normalize_title(v.get('title'))==normalize_title(gold['title']) and v.get('publication_year')==gold['year'] for v in values):
            raise AssertionError('independent bibliography differs')
    if shape == 'search' and expected.get('author'):
        names = [normalize_title(a.get('author',{}).get('display_name')) for r in values for a in r.get('authorships') or []]
        if normalize_title(expected['author']) not in names: raise AssertionError('expected author name absent')
    for value in values:
        key = value['id'].rsplit('/',1)[-1]
        for field in ('author_id', 'institution_id'):
            if field in expected:
                candidates = [str(item.get('id', '')).rsplit('/', 1)[-1] for authorship in value.get('authorships') or []
                              for item in ([authorship.get('author') or {}] if field == 'author_id' else authorship.get('institutions') or [])]
                if expected[field] not in candidates:
                    raise AssertionError(field + ' filter differs')
        known = records.get(key)
        if known and (shape != 'graph' or value.get('title') is not None):
            for field in SELECT.split(','):
                if value.get(field) != known.get(field): raise AssertionError('source field differs: ' + field)
        if isinstance(expected.get('year'), int) and value.get('publication_year') != expected['year']: raise AssertionError('year filter differs')
        if isinstance(expected.get('oa'), bool) and (value.get('open_access') or {}).get('is_oa') != expected['oa']: raise AssertionError('OA filter differs')
        for field in ('topic', 'primary_topic', 'source', 'primary_source'):
            if field not in expected:
                continue
            if field == 'topic': candidates = value.get('topics') or []
            elif field == 'primary_topic': candidates = [value.get('primary_topic') or {}]
            elif field == 'primary_source': candidates = [(value.get('primary_location') or {}).get('source') or {}]
            else: candidates = [x.get('source') or {} for x in (value.get('locations') or []) + [value.get('primary_location') or {}]]
            if expected[field] not in [str(x.get('id', '')).rsplit('/', 1)[-1] for x in candidates]:
                raise AssertionError(field + ' filter differs')
    if 'count' in expected and (data['meta']['count'] != expected['count'] or data['meta']['count_relation'] != 'eq'):
        raise AssertionError('exact count differs')
    if 'more' in expected and bool(data['meta'].get('next_cursor')) != expected['more']: raise AssertionError('pagination differs')
    return data


async def load_cases(url, cases, records, concurrency, seconds, headers, min_per_kind=0):
    started=time.monotonic(); latencies=[]; errors=[]; by_kind=collections.defaultdict(list); requests=0
    groups = collections.defaultdict(list)
    for case in cases: groups[case['name'].split(':')[0]].append(case)
    kinds = list(groups); scheduled = 0; completed = collections.Counter(); failures = 0
    async with httpx.AsyncClient(base_url=url,headers=headers,timeout=config()['query_timeout']+5,trust_env=False) as client:
        async def worker(offset):
            nonlocal requests, scheduled, failures
            while (time.monotonic()-started < seconds or any(completed[k] < min_per_kind for k in kinds)) and time.monotonic()-started < max(seconds*4, 60):
                n=scheduled;scheduled+=1;kind=kinds[n % len(kinds)]
                case=groups[kind][(n // len(kinds)) % len(groups[kind])];t=time.monotonic()
                try:
                    r=await client.get(case['path'],params=case['params']);validate_response(case,r,records)
                except (httpx.HTTPError,ValueError,KeyError,AssertionError,TypeError) as ex:
                    if len(errors)<10:errors.append(dict(case=case['name'],error=type(ex).__name__ + ': ' + str(ex)[:160]))
                    failures+=1
                elapsed=time.monotonic()-t;latencies.append(elapsed);by_kind[kind].append(elapsed);completed[kind]+=1
                requests+=1
        await asyncio.gather(*(worker(n) for n in range(concurrency)))
    elapsed=time.monotonic()-started
    return dict(concurrency=concurrency,requested_seconds=seconds,elapsed_seconds=elapsed,requests=requests,
                throughput=requests/elapsed,p95_seconds=percentile(latencies,.95),p99_seconds=percentile(latencies,.99),
                errors=failures,error_examples=errors,minimum_samples_passed=all(completed[k]>=min_per_kind for k in kinds),
                by_kind={k:dict(requests=len(v),p95_seconds=percentile(v,.95),p99_seconds=percentile(v,.99)) for k,v in by_kind.items()})


def check(cases_file, output, base_url=None, phase='auto', load_seconds=0):
    from .config import ROOT
    code={p.name:file_hash(p) for p in (CODE_ROOT / 'openalex_api').glob('*.py')}
    fixture=read_json(Path(cases_file));output=Path(output);output.mkdir(parents=True,exist_ok=True)
    if fixture.get('schema') != 1 or not fixture.get('cases'):raise ValueError('invalid acceptance fixture')
    for case in fixture['cases']:
        if (not case['path'].startswith('/') or case['path'].startswith('//') or '\\' in case['path']
                or case['phase'] not in ('metadata','search') or 'api_key' in case['params']):
            raise ValueError('unsafe acceptance case')
    cfg=config();url=base_url or 'http://127.0.0.1:'+str(cfg['port']);headers={'X-API-Key':cfg['api_keys'][0]}
    results=[];pending=[];records=fixture['records']
    with httpx.Client(base_url=url,headers=headers,timeout=cfg['query_timeout']+5,trust_env=False) as client:
        state=client.get('/status');state.raise_for_status();before=state.json()
        if before['generation'] != fixture['generation']:raise RuntimeError('API generation differs from fixture')
        if fixture.get('search_build') and before.get('search_build') != fixture['search_build']:
            raise RuntimeError('API search build differs from fixture')
        available=before.get('search_ready') and 'earth_sciences' in before.get('search_coverage',{}).get('published_scopes',[])
        selected=[]
        for case in fixture['cases']:
            if phase=='metadata' and case['phase']!='metadata':continue
            if phase=='search' and case['phase']!='search':continue
            if case['phase']=='search' and not available:
                pending.append(case['name']);continue
            selected.append(case);t=time.monotonic()
            try:
                response=client.get(case['path'],params=case['params']);data=validate_response(case,response,records)
                following=case['expected'].get('next_ids')
                if following is not None:
                    follow=dict(case,params=case['params'] | {'cursor':data['meta']['next_cursor']},expected=dict(shape='graph',ids=following))
                    validate_response(follow,client.get(follow['path'],params=follow['params']),records)
                results.append(dict(case=case['name'],passed=True,seconds=time.monotonic()-t))
            except (httpx.HTTPError,ValueError,KeyError,AssertionError,TypeError) as ex:
                results.append(dict(case=case['name'],passed=False,error=type(ex).__name__+': '+str(ex)[:200]))
        unauthorized=httpx.get(url+'/status',timeout=cfg['query_timeout']+5,trust_env=False)
        results.append(dict(case='missing-api-key',passed=unauthorized.status_code in (401,403)))
    matrix=[];sustained=None
    if load_seconds and selected and all(r['passed'] for r in results):
        for n in (1,4,8,16,32):
            matrix.append(asyncio.run(load_cases(url,selected,records,n,60,headers,20 if fixture.get('strict_performance') else 0)))
            atomic_json(output/'load-progress.json',dict(matrix=matrix,state='matrix'))
        sustained=asyncio.run(load_cases(url,selected,records,10,load_seconds,headers,100 if fixture.get('strict_performance') else 0))
    with httpx.Client(base_url=url,headers=headers,timeout=cfg['query_timeout']+5,trust_env=False) as client:
        after=client.get('/status');after.raise_for_status();after=after.json()
    unchanged=code=={p.name:file_hash(p) for p in (CODE_ROOT / 'openalex_api').glob('*.py')}
    stable=unchanged and all(before.get(k)==after.get(k) for k in ('generation','search_build'))
    functional=bool(results) and all(r['passed'] for r in results) and stable
    performance=(not load_seconds or bool(load_seconds>=1800 and sustained and not sustained['errors'] and sustained['requests']>=1000
                 and sustained['p95_seconds'] is not None and sustained['p95_seconds']<=2 and sustained['p99_seconds']<=5
                 and all(not row['errors'] and row.get('minimum_samples_passed', True) for row in matrix)))
    if fixture.get('strict_performance') and load_seconds:
        performance = bool(performance and sustained.get('minimum_samples_passed') and all(
            value['requests'] >= 100 and value['p95_seconds'] <= 2 and value['p99_seconds'] <= 5
            for key,value in sustained['by_kind'].items() if key.startswith('real-')))
    status='failed' if not functional or not performance else 'pending' if pending else 'passed'
    report=dict(status=status,passed=status=='passed',functional_passed=functional,
                performance_tested=bool(load_seconds),performance_passed=performance if load_seconds else None,
                generation=fixture['generation'],fixture_signature=digest(fixture),phase=phase,base_url=url,client_hostname=socket.gethostname(),
                cross_pod_tested=False,stable_generation_and_build=stable,results=results,pending=pending,matrix=matrix,sustained=sustained,
                code=code,code_unchanged=unchanged,search_ready=bool(available),
                unexecuted_search_cases=sum(c['phase']=='search' for c in fixture['cases']) if phase=='metadata' else len(pending),
                filesystem_cache_flushed=False,limitations=['Source-derived checks establish consistency, not independent retrieval precision/recall.','Cross-Pod curl execution is recorded separately; this run does not infer a remote client from the URL.'])
    atomic_json(output/'acceptance.json',report)
    return report
