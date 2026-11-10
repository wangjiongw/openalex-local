"""Recoverable identity-to-work candidates; original authorship predicates stay authoritative."""
import hashlib
import json
import signal
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED

import orjson

from .backend import Backend, database
from .config import atomic_json, config
from .earth_control import read_json
from .manticore_build import save
from .manticore_engine import Client, checked_name
from .manticore_formats import numeric
from .search_versions import build_path

MAX_CANDIDATES = 250000


def pairs(rows):
    result = set()
    for row in rows:
        authors = row['authorships']
        authors = json.loads(authors) if isinstance(authors, str) else authors
        for author in authors or []:
            if author.get('author_id'):
                result.add((0, numeric(author['author_id'], 'A'), int(row['id'])))
            for key in author.get('institution_id') or []:
                result.add((1, numeric(key, 'I'), int(row['id'])))
    return [dict(kind=kind, entity_id=key, work_id=work) for kind, key, work in sorted(result)]


def initialize():
    global BACKEND, SEARCH
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    BACKEND, SEARCH = Backend(writer=True), Client(timeout=600)


def batch(g, name, index, after, size):
    rows = SEARCH.rows(f'SELECT id,attrs.authorships AS authorships FROM {checked_name(index)} WHERE id>{int(after)} ORDER BY id ASC LIMIT {int(size)} OPTION max_matches={int(size)},threads=1')
    if not rows:
        raise RuntimeError('identity source membership ended early')
    edges = pairs(rows)
    if edges:
        BACKEND.insert(g, name, edges)
    return dict(after=int(rows[-1]['id']), count=len(rows), pairs=len(edges), digest=hashlib.sha256(orjson.dumps(edges)).hexdigest())


def build(b, manifest, stop):
    target = build_path(manifest['generation'], manifest['build'])
    segments = [s for s in manifest['segments'] if s['entity'] == 'works']
    signature = hashlib.sha256(orjson.dumps(dict(metadata=manifest['metadata_signature'], segments=[(s['index'], s['count'], s['snapshot']) for s in segments]), option=orjson.OPT_SORT_KEYS)).hexdigest()
    name = 'identity_' + hashlib.sha256((manifest['generation'] + manifest['build']).encode()).hexdigest()[:12]
    table = database(manifest['generation']) + '.' + name
    checkpoint = target / 'identity-progress.json'
    progress = read_json(checkpoint) if checkpoint.exists() else dict(signature=signature, segments={s['index']: dict(after=0, count=0, pairs=0, digest='') for s in segments})
    if progress['signature'] != signature:
        raise RuntimeError('identity prefilter inputs changed')
    exists = int(b.ch(f"EXISTS TABLE {table} FORMAT JSONEachRow")[0]['result'])
    if not exists and any(s['count'] for s in progress['segments'].values()):
        raise RuntimeError('identity table missing after checkpoint; restore it or rebuild into a new catalog build')
    b.ch(f'CREATE TABLE IF NOT EXISTS {table} (kind UInt8, entity_id UInt64, work_id UInt64) ENGINE=ReplacingMergeTree ORDER BY (kind,entity_id,work_id)')
    if manifest.get('identity_index', {}).get('complete'):
        return
    manifest.update(state='building', identity_index=dict(table=table, signature=signature, complete=False))
    save(manifest)
    initial = sum(s['count'] for s in progress['segments'].values())
    started = time.monotonic()
    workers = max(1, min(len(segments), int(config()['cpus']), int(config().get('catalog_workers', 8))))
    size = max(1, min(int(config().get('catalog_batch_rows', 10000)), 10000))
    with ProcessPoolExecutor(workers, initializer=initialize) as pool:
        pending = {}
        def submit(segment):
            state = progress['segments'][segment['index']]
            if state['count'] < segment['count']:
                future = pool.submit(batch, manifest['generation'], name, segment['index'], state['after'], min(size, segment['count'] - state['count']))
                pending[future] = segment
        for segment in segments:
            submit(segment)
        while pending:
            if shutil.disk_usage(target).free < config().get('min_free_gib', 1) * 1024**3:
                raise RuntimeError('identity index disk reserve reached; resume after freeing space')
            done, _ = wait(pending, timeout=5, return_when=FIRST_COMPLETED)
            for future in done:
                segment = pending.pop(future)
                receipt = future.result()
                state = progress['segments'][segment['index']]
                if receipt['after'] <= state['after'] or receipt['count'] > segment['count'] - state['count']:
                    raise RuntimeError('identity checkpoint overflow or order mismatch')
                state.update(after=receipt['after'], count=state['count'] + receipt['count'], pairs=state['pairs'] + receipt['pairs'],
                             digest=hashlib.sha256((state['digest'] + receipt['digest']).encode()).hexdigest())
                # A replay after an interrupted checkpoint is idempotent through DISTINCT reads.
                atomic_json(checkpoint, progress)
                if not stop.is_set():
                    submit(segment)
            scanned = sum(s['count'] for s in progress['segments'].values())
            speed = (scanned - initial) / max(.001, time.monotonic() - started)
            atomic_json(target / 'progress.json', dict(state='building_identity_prefilter', scanned_works=scanned,
                expected_works=sum(s['count'] for s in segments), pairs=sum(s['pairs'] for s in progress['segments'].values()),
                workers=workers, records_per_second=speed, eta_seconds=(sum(s['count'] for s in segments) - scanned) / max(.001, speed), updated=time.time()))
            if stop.is_set():
                for future in pending:
                    future.cancel()
                raise RuntimeError('identity prefilter paused; resume the same catalog build')
    if any(progress['segments'][s['index']]['count'] != s['count'] for s in segments):
        raise RuntimeError('identity prefilter coverage differs')
    manifest['identity_index'].update(complete=True, scanned_works=sum(s['count'] for s in progress['segments'].values()), pairs=sum(s['pairs'] for s in progress['segments'].values()), checkpoint_sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest())
    save(manifest)


def mandatory(node):
    if 'term' in node:
        for field, value in node['term'].items():
            if field in ('authorships.author_id', 'authorships.institution_id'):
                kind, prefix = (0, 'A') if field.endswith('author_id') else (1, 'I')
                value = value.get('value') if isinstance(value, dict) else value
                try:
                    yield kind, numeric(value, prefix)
                except ValueError:
                    pass
    if 'nested' in node:
        if node['nested']['path'] == 'authorships':
            yield from mandatory(node['nested']['query'])
    for kind in ('must', 'filter'):
        children = node.get('bool', {}).get(kind, [])
        for child in children if isinstance(children, list) else [children]:
            yield from mandatory(child)


def apply(native, body, manifest, backend):
    index = manifest.get('identity_index') or {}
    if not index.get('complete'):
        return
    table = index['table']
    if not table.startswith(database(manifest['generation']) + '.identity_') or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_.' for c in table):
        raise ValueError('invalid identity index table')
    for kind, key in dict.fromkeys(mandatory(body['query'])):
        rows = backend.ch(f'SELECT DISTINCT work_id FROM {table} WHERE kind={kind} AND entity_id={key} LIMIT {MAX_CANDIDATES + 1} FORMAT JSONEachRow')
        # shortcut: very large identities retain exact scanning; use a join index above this bound.
        if len(rows) > MAX_CANDIDATES:
            continue
        ids = [int(row['work_id']) for row in rows]
        native['query'].pop('match_all', None)
        native['query'].setdefault('bool', {}).setdefault('must', []).append({'in': {'id': ids}} if ids else {'equals': {'id': 0}})
        native['options'].pop('threads', None)
        return


def validate(backend, manifest):
    index = manifest.get('identity_index') or {}
    if not index.get('complete'):
        raise RuntimeError('catalog identity prefilter is incomplete')
    checkpoint = build_path(manifest['generation'], manifest['build']) / 'identity-progress.json'
    if hashlib.sha256(checkpoint.read_bytes()).hexdigest() != index['checkpoint_sha256']:
        raise RuntimeError('identity checkpoint checksum differs')
    progress = read_json(checkpoint)
    segments = [s for s in manifest['segments'] if s['entity'] == 'works']
    if progress['signature'] != index['signature'] or any(progress['segments'][s['index']]['count'] != s['count'] for s in segments) or index['scanned_works'] != sum(s['count'] for s in segments):
        raise RuntimeError('identity prefilter source coverage differs')
    if not int(backend.ch(f"EXISTS TABLE {index['table']} FORMAT JSONEachRow")[0]['result']):
        raise RuntimeError('identity prefilter table is missing')
