"""Pinned, receipt-driven result exports; API processes never own long jobs."""
import contextlib
import csv
import gzip
import json
import logging
import os
import re
import shutil
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .config import ROOT, atomic_json, config, read_generation
from .earth_control import read_json
from .ingest import lock
from .manticore_build import file_hash

FORMATS = ('jsonl.gz', 'csv', 'doi.txt', 'ids.txt')
CSV_FIELDS = ('id', 'doi', 'title', 'publication_year', 'authors', 'abstract', 'abstract_status', 'source',
              'is_oa', 'oa_status', 'oa_url', 'cited_by_count')
STOP = threading.Event()
SHARD_BYTES = 256 * 1024**2


def hash_file(file):
    for attempt, delay in enumerate((0, .1, .5, 1)):
        if delay:
            time.sleep(delay)
        try:
            return file_hash(file)
        except FileNotFoundError:
            if attempt == 3:
                raise
            logging.getLogger(__name__).warning('Export file temporarily unavailable: %s; retry %d/3',file,attempt+1)


def path(job=None):
    root = ROOT / 'exports/query'
    if job is None:
        return root
    if not re.fullmatch('[a-f0-9]{32}', job):
        raise ValueError('invalid export ID')
    return root / job


def get(job):
    return read_json(path(job) / 'state.json')


def status():
    from .manage import identity, state_path
    return dict(formats=list(FORMATS), worker_ready=bool(identity(state_path('export-worker'))),
                default_concurrency=config().get('export_workers', 2), order='stable ID; independent of display ranking')


def listing(offset=0, limit=25):
    jobs = sorted(path().glob('*/state.json'), key=lambda p: p.parent.name, reverse=True)
    return dict(count=len(jobs), results=[read_json(p) for p in jobs[offset:offset + limit]])


def prepare(payload, meta):
    from .query import query
    from . import official, official_text
    from .api import require_search, selected
    if not isinstance(payload, dict) or set(payload) - {'query', 'identifiers', 'formats', 'select', 'csv_fields'}:
        raise ValueError('unsupported export request fields')
    if ('query' in payload) == ('identifiers' in payload):
        raise ValueError('provide query or identifiers, exclusively')
    formats = payload.get('formats', ['jsonl.gz'])
    if not isinstance(formats, list) or not formats or any(not isinstance(f,str) or f not in FORMATS for f in formats) or len(set(formats)) != len(formats):
        raise ValueError('unsupported or duplicate export formats')
    select = payload.get('select')
    if select is not None and not isinstance(select, str):
        raise ValueError('select must be a comma-separated string')
    selected({}, select)
    columns = payload.get('csv_fields', list(CSV_FIELDS))
    if not isinstance(columns, list) or not columns or any(not isinstance(f, str) or not f or not f.isidentifier() for f in columns) or len(set(columns)) != len(columns):
        raise ValueError('csv_fields must be unique top-level field names')
    plan = dict(schema=1, generation=meta['generation'], search_build=None,
                formats=formats, select=select, csv_fields=columns)
    if 'identifiers' in payload:
        values = payload['identifiers']
        if not isinstance(values, list) or not values or any(not isinstance(v, str) or not v.strip() or len(v) > 2000 for v in values):
            raise ValueError('identifiers must be a nonempty list of DOI/work ID strings')
        plan.update(kind='identifiers', identifiers=values)
        return plan
    spec = payload['query']
    if not isinstance(spec, dict) or set(spec) != {'route', 'params'} or spec['route'] not in ('/works', '/v1/works') or not isinstance(spec['params'], dict):
        raise ValueError('query requires route /works or /v1/works and params')
    params = spec['params']
    if any(not isinstance(k, str) or not isinstance(v, (str, int)) or isinstance(v, bool) for k, v in params.items()):
        raise ValueError('query params must be scalar strings or integers')
    params = {k: str(v) for k, v in params.items()}
    if set(params) & {'cursor', 'page', 'per_page', 'per-page', 'group_by', 'group-by', 'sample', 'seed', 'api_key', 'mailto'}:
        raise ValueError('export query excludes pagination, grouping, sampling and credentials')
    if 'select' in params:
        if select is not None and select != params['select']:
            raise ValueError('conflicting select fields')
        plan['select'] = params.pop('select')
        selected({}, plan['select'])
    plan['query'] = dict(route=spec['route'],params=params)
    scope = params.get('search_scope', 'available')
    if spec['route'] == '/v1/works':
        if set(params) - {'search', 'search_field', 'author_search', 'filter', 'corpus', 'sort', 'search_scope', 'title_equal', 'count'}:
            raise ValueError('unsupported local export query parameters')
        if len(params.get('search','')) > 2000 or len(params.get('filter','')) > 8000 or len(params.get('author_search','')) > 500 or len(params.get('title_equal','')) > 8191 or params.get('count','bounded') not in ('exact','bounded'):
            raise ValueError('local export query exceeds supported bounds')
        body = query('works', params.get('search', ''), params.get('search_field', 'all'), params.get('filter', ''), params.get('corpus', 'all'), params.get('sort'), None, params.get('author_search', ''))
        if 'title_equal' in params:
            from .model import normalize_title
            body['query'] = {'bool': {'must': [body['query']], 'filter': [{'term': {'name_exact': normalize_title(params['title_equal'])}}]}}
        engine = 'manticore'
    else:
        if set(params) - official.PARAMS - set(official_text.SCOPES) - {'search_scope'}:
            raise ValueError('unsupported official export query parameters')
        if len(params.get('filter','')) > 64000:
            raise ValueError('official export filter exceeds supported bounds')
        filters = official.split_filters(params.get('filter', ''))
        engine = 'manticore' if 'search_scope' in params or any(k in official_text.SCOPES for k in params) or any(k in official_text.FILTER_SEARCH for k, _ in filters) else 'metadata'
        if engine == 'metadata':
            # Validate sort even though bulk output has its own stable order.
            official.sort_fields('works', params.get('sort'))
            plan.update(kind=engine, where=official.where_sql(meta['generation'], 'works', filters))
            return plan
        body = official_text.build('works', params, filters)
    require_search(meta, 'works')
    manifest = meta.get('_search_build') or {}
    if manifest.get('engine') != 'manticore':
        raise ValueError('query exports require the published Manticore search engine')
    from .manticore_api import scope as segments, search_request
    tables = [s['index'] for s in segments(meta, 'works', scope)]
    # Validate the complete query before creating a persistent task.
    search_request('works', body, tables, 1, layout=manifest.get('physical_layout', 'legacy'))
    plan.update(kind=engine, search_build=manifest['build'], body=body, search_scope=scope)
    return plan


def create(payload, meta):
    if not meta.get('metadata_frozen'):
        raise ValueError('exports require frozen metadata')
    plan = prepare(payload, meta)
    job = f'{time.time_ns():016x}' + uuid.uuid4().hex[16:]
    directory = path(job)
    directory.mkdir(parents=True, mode=0o700)
    atomic_json(directory / 'plan.json', plan)
    state = dict(id=job, state='queued', created=time.time(), updated=time.time(),
                 generation=plan['generation'], search_build=plan['search_build'],
                 exported=0, missing_doi=0, after=None, receipts=[], files=[], order='stable ID', error=None)
    atomic_json(directory / 'state.json', state)
    return state


def control(job, action):
    directory = path(job)
    if action == 'cancel':
        state = get(job)
        if state['state'] != 'completed':
            atomic_json(directory / 'control.json', {'cancel': True})
        return get(job)
    if action != 'resume':
        raise ValueError('invalid export operation')
    if get(job)['state'] == 'running':
        raise ValueError('cancel the running task before resuming')
    with lock(directory / 'worker.lock'):
        state = get(job)
        if state['state'] == 'completed':
            return state
        (directory / 'control.json').unlink(missing_ok=True)
        state.update(state='queued', error=None, updated=time.time())
        atomic_json(directory / 'state.json', state)
        return state


def download(job, name):
    state = get(job)
    if name not in {f['name'] for f in state['files']} | ({'manifest.json'} if state['state'] == 'completed' else set()):
        raise FileNotFoundError('file is not committed')
    if Path(name).name != name:
        raise ValueError('invalid export filename')
    return path(job) / name


def csv_record(record, columns):
    from .model import abstract
    source = (record.get('primary_location') or {}).get('source') or {}
    oa = record.get('open_access') or {}
    text, abstract_status = None, 'missing'
    if {'abstract','abstract_status'} & set(columns):
        try:
            text = abstract(record.get('abstract_inverted_index'))
            abstract_status = 'present' if text else 'missing'
        except ValueError:
            abstract_status = 'invalid'
    derived = dict(title=record.get('title') or record.get('display_name'), abstract_status=abstract_status, authors=[a.get('author') or {} for a in record.get('authorships') or []], abstract=text,
                   source=source, is_oa=oa.get('is_oa'), oa_status=oa.get('oa_status'), oa_url=oa.get('oa_url'))
    values = []
    for key in columns:
        value = derived[key] if key in derived else record.get(key)
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@')):
            value = "'" + value
        values.append(value)
    return values


def rows(plan, after, backend):
    from . import official, search_versions
    from .backend import sql_string
    n = int(config().get('export_batch_rows', 1000))
    if plan['kind'] == 'metadata':
        meta = read_generation(plan['generation'])
        table = official.source_table(backend, meta, 'works')
        records = backend.ch(f"SELECT id,raw FROM {table} WHERE ({plan['where']}) AND id>{sql_string(after or '')} ORDER BY id LIMIT {n} FORMAT JSONEachRow")
        return [(r['id'], json.loads(r['raw'])) for r in records]
    if plan['kind'] == 'identifiers':
        directory = path(plan['job'])
        resolved = directory / 'resolved.json'
        if not resolved.exists():
            from .model import canonical_id, doi
            found, unresolved, entries = set(), [], []
            dois = list(dict.fromkeys(doi(value) for value in plan['identifiers'] if doi(value)))
            mapping = {}
            for offset in range(0,len(dois),100):
                mapping.update(backend.resolve_many(plan['generation'],'works','doi',dois[offset:offset+100]))
            for value in plan['identifiers']:
                normalized = doi(value)
                try:
                    keys = mapping.get(normalized,[]) if normalized else [canonical_id(value)]
                    if any(not re.fullmatch(r'W[1-9]\d*', k) for k in keys):
                        raise ValueError('expected DOI or work ID')
                    if len(keys) == 1:
                        entries.append((value,keys[0]))
                    else:
                        unresolved.append(dict(input=value,status='not_found' if not keys else 'ambiguous',candidates=keys))
                except ValueError:
                    unresolved.append(dict(input=value,status='invalid',candidates=[]))
            keys = sorted({key for _,key in entries})
            for offset in range(0,len(keys),1000):
                found.update(backend.details(plan['generation'],'works',keys[offset:offset+1000]))
            unresolved.extend(dict(input=value,status='not_found',candidates=[]) for value,key in entries if key not in found)
            atomic_json(resolved, {'ids': sorted(found), 'unresolved': unresolved})
        ids = read_json(resolved)['ids']
        selected = [key for key in ids if key > (after or '')][:n]
        data = backend.details(plan['generation'], 'works', selected) if selected else {}
        if len(data) != len(selected):
            raise RuntimeError('identifier/detail consistency failure')
        return [(key, data[key]) for key in selected]
    meta = read_generation(plan['generation'])
    manifest = search_versions.read(plan['generation'], plan['search_build'])
    from .manticore_api import search_request, scope
    from .manticore_engine import Client
    body = plan['body'] | {'sort': [{'id': {'order': 'asc'}}]}
    request = search_request('works', body, [s['index'] for s in scope(meta | {'_search_build': manifest}, 'works', plan['search_scope'])], n, layout=manifest.get('physical_layout', 'legacy'))
    request['sort'] = [{'id': 'asc'}]
    request['query'].pop('match_all', None)
    request['options'].pop('scroll', None)
    request['query'].setdefault('bool', {}).setdefault('must', []).append({'range': {'id': {'gt': int(after or 0)}}})
    from .identity_filters import apply as identity_prefilter, search as identity_search
    lookup = identity_prefilter(request, body, manifest, backend)
    client = Client()
    try:
        result = identity_search(client, request, lookup)
    finally:
        client.close()
    hits = result['hits']['hits']
    ids = [h['_source']['original_id'] for h in hits]
    data = backend.details(plan['generation'], 'works', ids) if ids else {}
    if len(data) != len(ids) or len(set(ids)) != len(ids):
        raise RuntimeError('search/detail consistency failure')
    return [(str(h['_id']), data[key]) for h, key in zip(hits, ids)]


def verify(directory, state):
    previous, count, files = None, 0, []
    for reference in state['receipts']:
        receipt = directory / reference['name']
        if hash_file(receipt) != reference['sha256']:
            raise RuntimeError('export receipt checksum mismatch')
        value = read_json(receipt)
        if value['previous_after'] != previous:
            raise RuntimeError('export receipt chain mismatch')
        for f in value['files']:
            if hash_file(directory / f['name']) != f['sha256']:
                raise RuntimeError('export file checksum mismatch')
        count += value['count']; previous = value['after']; files.extend(value['files'])
    if count != state['exported'] or previous != state['after'] or files != state['files']:
        raise RuntimeError('export checkpoint mismatch')


def run(job):
    from .backend import Backend
    directory = path(job)
    with lock(directory / 'worker.lock'):
        state = get(job)
        if state['state'] not in ('queued', 'running'):
            return state
        backend = Backend()
        try:
            plan = read_json(directory / 'plan.json') | {'job': job}
            if plan.get('schema') != 1:
                raise ValueError('unsupported export schema')
            verify(directory, state)
            if state.get('host') and state['host'] != socket.gethostname():
                raise RuntimeError('export belongs to another host; controlled transfer required')
            state.update(state='running', host=socket.gethostname(), error=None)
            atomic_json(directory / 'state.json', state)
            while True:
                if STOP.is_set() or (directory / 'control.json').exists():
                    state['state'] = 'paused' if STOP.is_set() else 'cancelled'
                    break
                if shutil.disk_usage(directory).free < config().get('min_free_gib', 1) * 1024**3:
                    state.update(state='paused', error='disk-reserve')
                    break
                batch = rows(plan, state['after'], backend)
                if not batch:
                    state.update(state='completed', updated=time.time())
                    atomic_json(directory / 'manifest.json', state | {'plan': plan, 'unresolved': read_json(directory / 'resolved.json').get('unresolved', []) if (directory / 'resolved.json').exists() else []})
                    break
                part = len(state['receipts'])
                files, missing, count, size, after = [], 0, 0, 0, state['after']
                with contextlib.ExitStack() as stack:
                    streams = {}
                    for fmt in plan['formats'] + ['missing-doi.jsonl.gz']:
                        name = f'part-{part:06d}.' + fmt
                        target = directory / name
                        stream = stack.enter_context(gzip.open(str(target) + '.part', 'wt', encoding='utf-8', newline='') if fmt.endswith('.gz') else open(str(target) + '.part', 'w', encoding='utf-8', newline=''))
                        streams[fmt] = (stream, name)
                    writer = csv.writer(streams['csv'][0]) if 'csv' in streams else None
                    if writer:
                        writer.writerow(plan['csv_fields'])
                    while batch:
                        for cursor, record in batch:
                            from .model import doi
                            if 'jsonl.gz' in streams:
                                from .api import selected
                                streams['jsonl.gz'][0].write(json.dumps(selected(record, plan['select']), ensure_ascii=False, separators=(',', ':')) + '\n')
                            if writer:
                                writer.writerow(csv_record(record, plan['csv_fields']))
                            identifier = record['id']
                            if 'ids.txt' in streams:
                                streams['ids.txt'][0].write(identifier + '\n')
                            value = doi(record.get('doi'))
                            if 'doi.txt' in streams and value:
                                streams['doi.txt'][0].write(value + '\n')
                            if not value:
                                streams['missing-doi.jsonl.gz'][0].write(json.dumps({'id': identifier}) + '\n')
                                missing += 1
                            count += 1; after = cursor
                        size = max(stream.tell() for stream, _ in streams.values())
                        if count >= config().get('export_shard_rows', 100000) or size >= SHARD_BYTES or STOP.is_set() or (directory / 'control.json').exists():
                            break
                        batch = rows(plan, after, backend)
                for _, name in streams.values():
                    temporary = directory / (name + '.part')
                    with temporary.open('rb') as stream:
                        os.fsync(stream.fileno())
                    os.replace(temporary, directory / name)
                    files.append(dict(name=name, bytes=(directory / name).stat().st_size, sha256=hash_file(directory / name)))
                receipt_name = f'receipt-{part:06d}.json'
                atomic_json(directory / receipt_name, dict(previous_after=state['after'], after=after, count=count, missing_doi=missing, files=files))
                state['receipts'].append(dict(name=receipt_name, sha256=hash_file(directory / receipt_name)))
                state['files'].extend(files)
                state.update(after=after, exported=state['exported'] + count, missing_doi=state['missing_doi'] + missing, updated=time.time())
                atomic_json(directory / 'state.json', state)
        except Exception as error:
            state.update(state='failed', error=type(error).__name__ + ': ' + str(error))
        finally:
            backend.close()
            state['updated'] = time.time()
            atomic_json(directory / 'state.json', state)
        return state


def main():
    from .workspace import claim
    claim()
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    with lock(ROOT / 'var/run/export-worker.lock'):
        with ThreadPoolExecutor(max_workers=max(1, int(config().get('export_workers', 2)))) as pool:
            pending, completed = {}, set()
            while not STOP.is_set():
                for job, future in list(pending.items()):
                    if future.done():
                        try:
                            if future.result()['state'] == 'completed':
                                completed.add(job)
                        except Exception:
                            logging.getLogger(__name__).exception('Export task %s could not start',job)
                        del pending[job]
                for item in sorted(path().glob('*/state.json')):
                    if item.parent.name in completed:
                        continue
                    try:
                        state = read_json(item)
                    except (OSError,ValueError):
                        logging.getLogger(__name__).exception('Export checkpoint unavailable: %s',item)
                        continue
                    if len(pending) >= max(1, int(config().get('export_workers', 2))):
                        break
                    if state['state'] in ('queued', 'running') and state['id'] not in pending:
                        pending[state['id']] = pool.submit(run, state['id'])
                STOP.wait(1)


if __name__ == '__main__':
    main()
