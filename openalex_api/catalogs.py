"""Add scoped author and full institution catalogs to an immutable search build."""
import copy
import gzip
import shutil
import signal
import threading
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED

STOP = threading.Event()
import hashlib
import json
import time

from .backend import Backend, database, sql_string, sql_list
from .config import atomic_json, config, read_generation
from .earth_control import read_json
from .ingest import lock
from .manticore_build import MAX_BLOCK, file_hash, save, write_batch, seal, restore, tables
from .manticore_engine import Client
from .search_versions import build_path, read, require_metadata


def status(g, build):
    return read_json(build_path(g, build) / 'build.json')


def author_ids(rows):
    result = set()
    for row in rows:
        values = row['authorships']
        values = json.loads(values) if isinstance(values, str) else values
        for author in values or []:
            value = author.get('author_id')
            if value:
                from .manticore_formats import numeric
                numeric(value, 'A')
                result.add(value)
    return sorted(result)


def include_aliases(node):
    if isinstance(node, dict):
        if 'multi_match' in node:
            fields = node['multi_match']['fields']
            if any(f.split('^')[0].split('.')[0] == 'name' for f in fields) and 'aliases' not in fields:
                fields.append('aliases')
        for value in node.values():
            include_aliases(value)
    elif isinstance(node, list):
        for value in node:
            include_aliases(value)


def prepare(g, build, parent):
    proof = require_metadata(g)
    parent_manifest = read(g, parent)
    if parent_manifest.get('state') != 'validated' or not parent_manifest.get('earth_complete'):
        raise ValueError('catalog expansion requires a validated complete Earth parent')
    target = build_path(g, build)
    original = build_path(g, parent)
    signature = file_hash(original / 'build.json')
    if (target / 'build.json').exists():
        m = status(g, build)
        if m.get('parent_manifest_sha256') != signature or m['metadata_signature'] != proof['signature']:
            raise RuntimeError('catalog inputs changed; use another build ID')
        return m
    if parent_manifest.get('indexes', {}).get('authors') or parent_manifest.get('indexes', {}).get('institutions'):
        raise ValueError('parent already contains catalogs')
    m = copy.deepcopy(parent_manifest)
    for segment in m['segments']:
        for batch in segment['batches']:
            batch['path'] = str((original / batch['path']).resolve())
        segment['snapshot']['path'] = str((original / segment['snapshot']['path']).resolve())
    for key in ('parent_artifact_signature', 'rebuild_source', 'source_artifact_signature', 'source_reconciliation'):
        m.pop(key, None)
    m.update(build=build, parent=parent, parent_manifest_sha256=signature, state='building',
             created=time.time(), catalog_progress={}, catalog_membership={}, catalog_expansion=True)
    m['catalog_scopes'] = dict(authors='authors linked to frozen published Earth works', institutions='all imported institutions',
                               works=parent_manifest['entities']['works']['count'])
    save(m)
    return m


def membership_worker():
    global MEMBER_BACKEND, MEMBER_CLIENT
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    MEMBER_BACKEND, MEMBER_CLIENT = Backend(writer=True), Client(timeout=600)


def membership_batch(g, name, index, after, size):
    rows = MEMBER_CLIENT.rows(f"SELECT id,attrs.authorships AS authorships FROM {index} WHERE id>{after} ORDER BY id ASC LIMIT {size} OPTION max_matches={size},threads=1")
    if not rows:
        raise RuntimeError('published work membership ended early')
    MEMBER_BACKEND.insert(g, name, [dict(id=i) for i in author_ids(rows)])
    return int(rows[-1]['id']), len(rows)


def membership(b, c, m):
    g, target = m['generation'], build_path(m['generation'], m['build'])
    name = 'catalog_authors_' + hashlib.sha256((g + m['build']).encode()).hexdigest()[:12]
    table = database(g) + '.' + name
    b.ch(f'CREATE TABLE IF NOT EXISTS {table} (id String) ENGINE=ReplacingMergeTree ORDER BY id')
    progress = m['catalog_membership']
    segments = [s for s in m['segments'] if s['entity'] == 'works']
    for segment in segments:
        progress.setdefault(segment['index'], dict(after=0, count=0))
    initial = sum(v['count'] for v in progress.values())
    started = time.monotonic()
    workers = max(1,min(int(config().get('catalog_workers',8)),int(config()['cpus'])))
    size = max(1,min(int(config().get('catalog_batch_rows',10000)),10000))
    with ProcessPoolExecutor(max_workers=workers,initializer=membership_worker) as pool:
        pending = {}
        def submit(segment):
            cursor = progress[segment['index']]
            if cursor['count'] < segment['count']:
                future = pool.submit(membership_batch,g,name,segment['index'],cursor['after'],min(size,segment['count']-cursor['count']))
                pending[future] = segment
        for segment in segments:
            submit(segment)
        while pending:
            done, _ = wait(pending,return_when=FIRST_COMPLETED)
            for future in done:
                segment = pending.pop(future)
                after, count = future.result()
                cursor = progress[segment['index']]
                cursor.update(after=after,count=cursor['count']+count)
                if cursor['count'] > segment['count']:
                    raise RuntimeError('published work membership count changed')
                save(m)
                total = sum(v['count'] for v in progress.values())
                speed = (total-initial)/max(time.monotonic()-started,.001)
                atomic_json(target/'progress.json',dict(state='extracting_authors',scanned_works=total,
                    expected_works=m['entities']['works']['count'],workers=workers,records_per_second=speed,
                    eta_seconds=(m['entities']['works']['count']-total)/max(speed,.001),updated=time.time()))
                if STOP.is_set():
                    for remaining in pending:
                        remaining.cancel()
                    raise RuntimeError('catalog paused by signal; resume with the same build')
                submit(segment)
    count = int(b.ch(f'SELECT count() n FROM {table} FINAL FORMAT JSONEachRow')[0]['n'])
    m['author_membership_table'] = table
    m['expected_authors'] = count
    save(m)
    return table


def replay(b, c, manifest, segment):
    from .manticore_formats import create as native_create
    from .manticore_engine import create as legacy_create
    from .manticore_build import prepare_document, verify_rows
    from .official import source_table
    target = build_path(manifest['generation'], manifest['build'])
    (native_create if segment['physical_layout'] == 'native_v2' else legacy_create)(c, segment['index'])
    source = source_table(b, read_generation(manifest['generation']), segment['entity'])
    for batch in segment['batches']:
        receipt = target / batch['path']
        if file_hash(receipt) != batch['sha256']:
            raise RuntimeError('catalog replay receipt checksum mismatch')
        expected = json.loads(gzip.decompress(receipt.read_bytes()))
        rows = b.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity={sql_string(segment['entity'])} AND id IN {sql_list([r['original_id'] for r in expected])} ORDER BY id FORMAT JSONEachRow")
        docs = [prepare_document((segment['entity'], '', r, {}, segment['physical_layout'])) for r in rows]
        c.bulk(segment['index'], docs)
        verify_rows(c, segment['index'], expected)


def build(g, build, parent=None):
    from .official import source_table
    if not parent:
        from .search_versions import active
        parent = active(g)['build']
    if parent == build:
        raise ValueError('catalog build must differ from parent')
    target = build_path(g, build)
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda *_: STOP.set())
        signal.signal(signal.SIGINT, lambda *_: STOP.set())
    with lock(target / 'catalogs.lock'):
        m = prepare(g, build, parent)
        if m['state'] in ('sealed', 'validated'):
            return m
        b, c = Backend(writer=True), Client(timeout=600)
        try:
            meta = read_generation(g)
            for entity in ('institutions', 'authors'):
                if entity == 'authors':
                    membership(b, c, m)
                source = source_table(b, meta, entity)
                progress = m['catalog_progress'].setdefault(entity, dict(after='', selected=0, missing=0, missing_sample=[]))
                while not progress.get('done'):
                    if STOP.is_set():
                        raise RuntimeError('catalog paused by signal; resume with the same build')
                    batch_rows = max(1,min(int(config().get('catalog_batch_rows',10000)),10000))
                    if shutil.disk_usage(target).free < config().get('min_free_gib', 1) * 1024**3:
                        raise RuntimeError('catalog disk reserve reached; resume after freeing space')
                    if entity == 'authors':
                        ids = [r['id'] for r in b.ch(f"SELECT id FROM {m['author_membership_table']} FINAL WHERE id>{sql_string(progress['after'])} ORDER BY id LIMIT {batch_rows} FORMAT JSONEachRow")]
                        rows = b.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity='authors' AND id IN {sql_list(ids)} ORDER BY id FORMAT JSONEachRow") if ids else []
                    else:
                        rows = b.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity='institutions' AND id>{sql_string(progress['after'])} ORDER BY id LIMIT {batch_rows} FORMAT JSONEachRow")
                        ids = [r['id'] for r in rows]
                    selected = [s for s in m['segments'] if s['entity'] == entity]
                    segment = selected[-1] if selected and not selected[-1].get('sealed') else None
                    if not ids:
                        if segment:
                            seal(c, target, segment)
                        progress['done'] = True
                        m['entities'][entity] = dict(count=sum(s['count'] for s in m['segments'] if s['entity'] == entity))
                        if entity == 'institutions' and m['entities'][entity]['count'] != meta['expected'][entity]['records']:
                            raise RuntimeError('institution catalog does not cover the frozen source')
                        save(m)
                        break
                    if rows:
                        if segment is None:
                            index = 'oa_mc_' + hashlib.sha256((g + build).encode()).hexdigest()[:12] + f"_{len(m['segments']):03d}"
                            segment = dict(index=index, entity=entity, group='', count=0, after='', batches=[], physical_layout=m.get('physical_layout', 'legacy'), sealed=False)
                            from .manticore_formats import create as native_create
                            from .manticore_engine import create as legacy_create
                            (native_create if segment['physical_layout'] == 'native_v2' else legacy_create)(c, index)
                            m['segments'].append(segment)
                            save(m)
                        if segment['index'] not in tables(c):
                            replay(b, c, m, segment)
                        write_batch(c, b, target, segment, rows, bulk_workers=2 if config()['cpus'] >= 2 else 1)
                        if segment['count'] >= MAX_BLOCK:
                            seal(c, target, segment)
                    found = {r['id'] for r in rows}
                    missing = [i for i in ids if i not in found]
                    progress.update(after=ids[-1], selected=progress['selected'] + len(ids), missing=progress['missing'] + len(missing))
                    progress['missing_sample'] = (progress['missing_sample'] + missing)[:100]
                    m['entities'][entity] = dict(count=sum(s['count'] for s in m['segments'] if s['entity'] == entity))
                    save(m)
                    atomic_json(target / 'progress.json', dict(state='building_catalogs', entity=entity, progress=progress, updated=time.time()))
            m['indexes'] = {entity: ','.join(s['index'] for s in m['segments'] if s['entity'] == entity and s.get('sealed')) for entity in m['entities'] if any(s['entity'] == entity and s.get('sealed') for s in m['segments'])}
            m.update(state='sealed', sealed_at=time.time())
            save(m)
            return m
        except Exception as error:
            m.update(state='failed', error=type(error).__name__ + ': ' + str(error))
            save(m)
            raise
        finally:
            b.close(); c.close()


def qualify(g, build):
    """Qualify added catalogs while retaining the parent's full restore evidence."""
    import concurrent.futures
    import threading
    from .acceptance import fixtures, candidate_api
    from .earth_delivery import check, load_cases
    from .manticore_build import verify_segment, physical_check, signature
    from .config import CODE_ROOT
    import asyncio
    m = read(g, build)
    target = build_path(g, build)
    if not m.get('catalog_expansion') or m['state'] not in ('sealed', 'validated'):
        raise ValueError('a sealed catalog expansion is required')
    for key in ('parent_artifact_signature', 'rebuild_source', 'source_artifact_signature', 'source_reconciliation'):
        m.pop(key, None)
    parent_path = build_path(g, m['parent'])
    if file_hash(parent_path / 'build.json') != m['parent_manifest_sha256']:
        raise RuntimeError('parent manifest changed')
    parent = read(g, m['parent'])
    proof = read_json(parent_path / 'qualification/acceptance.json')
    if not proof.get('passed') or not proof.get('performance_passed'):
        raise RuntimeError('parent qualification is not passed')
    inherited = {s['index'] for s in parent['segments']}
    c = Client(timeout=600)
    try:
        for segment in m['segments']:
            if not segment.get('restore_verified') or not segment.get('physical_verified'):
                raise RuntimeError('a segment lacks restore/physical evidence')
            if segment['index'] not in inherited:
                verify_segment(c, target, segment)
                physical_check(target, segment)
    finally:
        c.close()
    output = target / 'qualification'
    output.mkdir(exist_ok=True)
    fixture = fixtures(g, m, output)
    b = Backend()
    try:
        for entity in ('authors', 'institutions'):
            segment = next((s for s in m['segments'] if s['entity'] == entity), None)
            if not segment:
                continue
            receipt = read_json_bytes(target / segment['batches'][0]['path'])
            keys = [r['original_id'] for r in receipt[:10]]
            for key, record in b.details(g, entity, keys).items():
                names = [record.get('display_name')] + (record.get('display_name_alternatives') or [])[:1]
                for index, name in enumerate(names):
                    if not name:
                        continue
                    for route in (f'/v1/{entity}', f'/{entity}'):
                        params = {'search': name, 'filter': 'id:' + key, 'select': 'id,display_name'}
                        fixture['cases'].append(dict(name=f'{entity}:{key}:{index}:{route}', phase='search', path=route, params=params,
                                                     expected=dict(shape='search', ids=[key])))
    finally:
        b.close()
    atomic_json(output / 'cases.json', fixture)
    stop = threading.Event()
    def exports_during_load():
        from .exports import create, run
        result = dict(jobs=0, records=0, errors=[])
        identifiers = list(fixture['records'])[:100]
        while not stop.is_set():
            job = create(dict(identifiers=identifiers, formats=['jsonl.gz']), read_generation(g))
            exported = run(job['id'])
            result['jobs'] += 1
            result['records'] += exported['exported']
            if exported['state'] != 'completed':
                result['errors'].append(exported.get('error') or exported['state'])
                break
        return result
    with candidate_api(g, build, output) as url:
        complete = read_generation(g).get('snapshot_complete', True)
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            producer = pool.submit(exports_during_load)
            try:
                report = check(output / 'cases.json', output, url, 'all', 1800 if complete else 0)
                if not complete:
                    matrix = [asyncio.run(load_cases(url,fixture['cases'],fixture['records'],n,5,{'X-API-Key':config()['api_keys'][0]})) for n in (1,8)]
                    report['performance_passed'] = all(v['requests'] and not v['errors'] and v['p95_seconds'] <= 2 and v['p99_seconds'] <= 5 for v in matrix)
                    report['matrix'] = matrix
            finally:
                stop.set()
            mixed = producer.result()
    passed = report.get('passed') and report.get('performance_passed') and mixed['jobs'] > 0 and not mixed['errors']
    report.update(passed=bool(passed), mixed_exports=mixed, parent_restore_evidence=str(parent_path / 'qualification/acceptance.json'),
                  reused_parent_segments=len(inherited), added_segments_restored=True,
                  limitations=report.get('limitations', []) + ['Parent full restore evidence reused; no repeated full restore or ClickHouse disaster recovery.'])
    atomic_json(output / 'acceptance.json', report)
    if not passed:
        raise RuntimeError('catalog/query/export qualification failed; inspect qualification/acceptance.json')
    if file_hash(parent_path / 'build.json') != m['parent_manifest_sha256']:
        raise RuntimeError('parent changed during qualification')
    atomic_json(target / 'observation.json', dict(state='complete', errors=0, long_observation_skipped=True, required_seconds=0,
                elapsed_seconds=0, projection_signature=signature(m.get('physical_layout', 'legacy')), mixed_load_required=False))
    m.update(state='validated', validated_at=time.time())
    save(m)
    return report


def read_json_bytes(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def publish(g, build):
    m = read(g, build)
    if not m.get('catalog_expansion') or file_hash(build_path(g,m['parent'])/'build.json') != m['parent_manifest_sha256']:
        raise RuntimeError('catalog parent identity mismatch')
    from .community_pipeline import publish as publish_candidate
    return publish_candidate(g, build)
