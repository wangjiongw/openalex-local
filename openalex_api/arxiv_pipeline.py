"""Independent resumable source releases: input → records → links → index → validate."""
import json
import sqlite3
import time
from pathlib import Path

import orjson

from .config import ROOT, active, atomic_json, config, generation_name
from .earth_control import read_json
from .ingest import lock
from .backend import Backend, sql_string, sql_list
from .semantics import digest
from .manticore_build import file_hash
from . import arxiv_source, arxiv_index

STAGES = ('ingest', 'link', 'store', 'index', 'validate')


def path(release):
    return ROOT / 'var/sources/arxiv' / generation_name(release)


def signature():
    return digest({name:file_hash(Path(__file__).with_name(name)) for name in
                   ('arxiv_source.py', 'arxiv_index.py', 'arxiv_pipeline.py')})


def read(release):
    m = read_json(path(release) / 'release.json')
    if m['release'] != release or m['signature'] != signature():
        raise RuntimeError('arXiv processing code changed; use a new source release')
    return m


def save(m):
    m['updated'] = time.time()
    atomic_json(path(m['release']) / 'release.json', m)


def plan(release, input_path, input_format='jsonl', year=2010, generation=None):
    if config().get('adopted_from'):
        raise ValueError('adopted workspace cannot build source releases')
    if not 1000 <= year <= 3000:
        raise ValueError('invalid minimum submission year')
    source = Path(input_path).expanduser().resolve()
    if not source.is_file() or input_format not in ('jsonl', 'duckdb'):
        raise ValueError('provide an existing JSONL/gzip or DuckDB file')
    from .search_versions import require_metadata
    generation = generation or active()['generation']
    proof = require_metadata(generation)
    spec = dict(release=release, input_path=str(source), input_format=input_format, min_year=year,
                generation=generation, metadata_proof=proof['signature'], input_sha256=file_hash(source),
                input_bytes=source.stat().st_size, signature=signature(), schema=1)
    directory = path(release)
    directory.mkdir(parents=True, exist_ok=True)
    with lock(directory / 'worker.lock'):
        if (directory / 'release.json').exists():
            m = read(release)
            if any(m[k] != v for k,v in spec.items()):
                raise ValueError('source release identity changed; choose a new release')
            return m
        m = spec | dict(state='planned', completed=[], processed=0, selected=0, duplicates=0,
                        linked=0, stored=0, indexed=0, created=time.time(), segment=None, error=None)
        save(m)
        return m


def connect(release):
    c = sqlite3.connect(path(release) / 'staging.sqlite', timeout=30)
    c.execute('PRAGMA journal_mode=DELETE')
    c.execute('PRAGMA synchronous=FULL')
    c.execute('PRAGMA cache_size=-131072')
    c.execute('CREATE TABLE IF NOT EXISTS records (number INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, hash TEXT NOT NULL, record TEXT NOT NULL, links TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS progress (stage TEXT PRIMARY KEY, value INTEGER NOT NULL)')
    return c


def checkpoint(c, stage):
    row = c.execute('SELECT value FROM progress WHERE stage=?', (stage,)).fetchone()
    return row[0] if row else 0


def commit(c, stage, value):
    c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)', (stage,value))
    c.commit()


def protect(m):
    import shutil
    if shutil.disk_usage(path(m['release'])).free < config().get('min_free_gib',1)*1024**3:
        raise InterruptedError('disk reserve reached; free space and resume')
    if (path(m['release']) / 'stop.json').exists():
        raise InterruptedError('source job stopped; resume to continue')


def ingest(m):
    with connect(m['release']) as c:
        after = checkpoint(c, 'ingest')
        duplicates = checkpoint(c, 'duplicates')
        last = after
        for line, raw in arxiv_source.records(m['input_path'], m['input_format']):
            if line <= after:
                continue
            first = arxiv_source.first_submission(raw,m['input_format'])
            if int(first[:4]) >= m['min_year']:
                record = arxiv_source.normalize(raw, m['input_format'])
                previous = c.execute('SELECT hash FROM records WHERE id=?', (record['id'],)).fetchone()
                h = digest(record)
                if previous:
                    if previous[0] != h:
                        raise ValueError('conflicting duplicate arXiv record: ' + record['id'])
                    duplicates += 1
                else:
                    c.execute('INSERT INTO records(id,hash,record) VALUES (?,?,?)',
                              (record['id'], h, json.dumps(record, ensure_ascii=False)))
            last = line
            if line % 5000 == 0:
                protect(m)
                c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)', ('duplicates',duplicates))
                commit(c, 'ingest', line)
                m.update(processed=line, selected=c.execute('SELECT count(*) FROM records').fetchone()[0], duplicates=duplicates)
                save(m)
        c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)', ('duplicates',duplicates))
        commit(c, 'ingest', last)
        m.update(processed=last, selected=c.execute('SELECT count(*) FROM records').fetchone()[0], duplicates=duplicates)
        dates = c.execute("SELECT min(json_extract(record,'$.first_submitted')),max(json_extract(record,'$.first_submitted')),max(json_extract(record,'$.update_date')) FROM records").fetchone()
        m['data_dates'] = dict(first_submission_min=dates[0],first_submission_max=dates[1],latest_source_update=dates[2])
    if 'input_sha256' in m and file_hash(m['input_path']) != m['input_sha256']:
        raise ValueError('arXiv input changed during ingestion; create a new release')


def exact_links(records, backend, generation):
    from .model import doi, external_ids
    values = set()
    for record in records:
        values.add('10.48550/arxiv.' + record['arxiv_id'])
        if record['doi']:
            values.add(record['doi'])
    mapping = backend.resolve_many(generation, 'works', 'doi', sorted(values))
    keys = sorted({ids[0] for ids in mapping.values() if len(ids) == 1})
    details = backend.details(generation, 'works', keys)
    result = []
    for record in records:
        candidates, confirmed = [], []
        for value, kind in (('10.48550/arxiv.'+record['arxiv_id'], 'preprint'), (record['doi'], 'publication')):
            if not value:
                continue
            ids = mapping.get(value, [])
            if len(ids) > 1:
                candidates.append(dict(doi=value, status='ambiguous', openalex_ids=ids, truncated=len(ids)>=101))
            elif len(ids) == 1:
                raw = details.get(ids[0])
                if raw is None or ('doi',value) not in set(external_ids(raw)):
                    raise RuntimeError('exact DOI hydration mismatch')
                if kind == 'publication' and (raw.get('type') == 'preprint' or value.startswith('10.48550/arxiv.')):
                    kind = 'preprint'
                confirmed.append(dict(openalex_id=ids[0], kind=kind, evidence='unique_exact_doi', doi=value,
                                      title=raw.get('title') or raw.get('display_name')))
        confirmed = list({v['openalex_id']:v for v in confirmed}.values())
        publications = [v['openalex_id'] for v in confirmed if v['kind']=='publication']
        result.append(dict(status='confirmed' if confirmed else 'ambiguous' if candidates else 'unmatched',
                           confirmed=confirmed, ambiguous=candidates,
                           preferred_openalex_id=publications[0] if len(publications)==1 else None,
                           generation=generation))
    return result


class LinkEvidence:
    """Release-local exact-lookup evidence; the matching rule stays in exact_links."""
    def __init__(self, connection):
        self.c = connection

    def resolve_many(self, generation, entity, scheme, values):
        if not values:
            return {}
        rows = self.c.execute('SELECT value,candidates FROM link_values WHERE value IN ('+','.join('?' for _ in values)+')',values).fetchall()
        if len(rows) != len(set(values)):
            raise RuntimeError('incomplete arXiv identifier evidence')
        return {v:json.loads(ids) for v,ids in rows}

    def details(self, generation, entity, keys):
        if not keys:
            return {}
        rows = self.c.execute('SELECT id,raw FROM link_works WHERE id IN ('+','.join('?' for _ in keys)+')',keys).fetchall()
        if len(rows) != len(set(keys)) or any(raw is None for _,raw in rows):
            raise RuntimeError('incomplete arXiv work evidence')
        return {key:json.loads(raw) for key,raw in rows}


def link(m, backend):
    from concurrent.futures import ThreadPoolExecutor
    workers = config().get('arxiv_link_workers', 8)
    batch_rows = config().get('arxiv_link_batch_rows', 2000)
    if type(workers) is not int or not 1 <= workers <= 32 or type(batch_rows) is not int or not 1 <= batch_rows <= 5000:
        raise ValueError('invalid arXiv link worker/batch configuration')
    def fetch(args):
        kind,values = args
        reader = Backend(writer=True)
        try:
            return (reader.resolve_many(m['generation'],'works','doi',values) if kind == 'identifiers'
                    else reader.details(m['generation'],'works',values))
        finally:
            reader.close()
    with connect(m['release']) as c, ThreadPoolExecutor(workers) as pool:
        c.execute('CREATE TABLE IF NOT EXISTS link_values(value TEXT PRIMARY KEY,candidates TEXT)')
        c.execute('CREATE TABLE IF NOT EXISTS link_works(id TEXT PRIMARY KEY,raw TEXT)')
        if not checkpoint(c,'link-input'):
            protect(m)
            c.execute("INSERT OR IGNORE INTO link_values(value) SELECT '10.48550/arxiv.'||json_extract(record,'$.arxiv_id') FROM records")
            c.execute("INSERT OR IGNORE INTO link_values(value) SELECT json_extract(record,'$.doi') FROM records WHERE json_extract(record,'$.doi') IS NOT NULL")
            commit(c,'link-input',1)
        # Globally ordered keys avoid repeatedly hydrating random granules of the full metadata store.
        for kind,table_name,key_column,result_column in [('identifiers','link_values','value','candidates'),('works','link_works','id','raw')]:
            if kind == 'works' and not checkpoint(c,'link-work-input'):
                c.execute("INSERT OR IGNORE INTO link_works(id) SELECT j.value FROM link_values,json_each(link_values.candidates) AS j WHERE json_array_length(candidates)=1")
                commit(c,'link-work-input',1)
            stage = 'link-'+kind
            after = checkpoint(c,stage) or ''
            while window := c.execute(f'SELECT {key_column} FROM {table_name} WHERE {key_column}>? ORDER BY {key_column} LIMIT ?', (after,batch_rows*workers)).fetchall():
                protect(m)
                batches = [[r[0] for r in window[i:i+batch_rows]] for i in range(0,len(window),batch_rows)]
                for keys,result in zip(batches,pool.map(fetch,[(kind,keys) for keys in batches])):
                    protect(m)
                    if kind == 'identifiers':
                        values = [(json.dumps(result.get(key,[])),key) for key in keys]
                    else:
                        if set(result) != set(keys):
                            raise RuntimeError('arXiv DOI evidence points to missing OpenAlex records')
                        values = [(json.dumps({f:result[key].get(f) for f in ('id','doi','ids','type','title','display_name')}),key) for key in keys]
                    c.executemany(f'UPDATE {table_name} SET {result_column}=? WHERE {key_column}=?',values)
                    count = checkpoint(c,stage+'-count')+len(keys)
                    c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)',(stage+'-count',count))
                    after=keys[-1];commit(c,stage,after)
                    m.update(link_phase=kind,**{'resolved_identifiers' if kind=='identifiers' else 'hydrated_works':count})
                    save(m)
        evidence = LinkEvidence(c)
        after = checkpoint(c,'link')
        while batch := c.execute('SELECT number,record FROM records WHERE number>? ORDER BY number LIMIT ?', (after,batch_rows)).fetchall():
            protect(m)
            links = exact_links([json.loads(r) for _,r in batch],evidence,m['generation'])
            c.executemany('UPDATE records SET links=? WHERE number=?',[(json.dumps(v),row[0]) for row,v in zip(batch,links)])
            after=batch[-1][0];commit(c,'link',after)
            m.update(link_phase='records',linked=after);save(m)


def table(m):
    return 'oa_arxiv_' + generation_name(m['release'])


def store(m, backend):
    db = table(m)
    backend.ch(f'CREATE DATABASE IF NOT EXISTS {db}')
    backend.ch(f'CREATE TABLE IF NOT EXISTS {db}.records (number UInt64, id String, hash String, record String, links String, INDEX id_bloom id TYPE bloom_filter GRANULARITY 1) ENGINE=ReplacingMergeTree ORDER BY number')
    backend.ch(f'CREATE TABLE IF NOT EXISTS {db}.versions (openalex_id String, arxiv_id String) ENGINE=ReplacingMergeTree ORDER BY (openalex_id,arxiv_id)')
    with connect(m['release']) as c:
        after = checkpoint(c, 'store')
        while batch := c.execute('SELECT number,id,hash,record,links FROM records WHERE number>? ORDER BY number LIMIT 2000', (after,)).fetchall():
            protect(m)
            rows = [dict(zip(('number','id','hash','record','links'), r)) for r in batch]
            backend.ch(f'INSERT INTO {db}.records FORMAT JSONEachRow', b''.join(orjson.dumps(r)+b'\n' for r in rows))
            versions = [dict(openalex_id=v['openalex_id'],arxiv_id=r['id']) for r in rows for v in json.loads(r['links'])['confirmed']]
            if versions:
                backend.ch(f'INSERT INTO {db}.versions FORMAT JSONEachRow', b''.join(orjson.dumps(r)+b'\n' for r in versions))
            # A replay may leave identical physical duplicates; FINAL is the release contract.
            found = backend.ch(f"SELECT number,hash FROM {db}.records FINAL WHERE number IN ({','.join(str(r['number']) for r in rows)}) FORMAT JSONEachRow")
            if {r['number']:r['hash'] for r in found} != {r['number']:r['hash'] for r in rows}:
                raise RuntimeError('arXiv metadata write verification failed')
            after = batch[-1][0]
            commit(c, 'store', after)
            m['stored'] = after
            save(m)


def index(m, client):
    from .manticore_build import tables, write_documents, seal, restore, verify_segment
    if not m['segment']:
        m['segment'] = dict(index='arxiv_'+m['release'], count=0, after=0, batches=[], entity='arxiv')
        save(m)
    s = m['segment']
    if s.get('sealed'):
        restore(client, path(m['release']), s)
        return
    if s['index'] not in tables(client):
        arxiv_index.create(client, s['index'])
        if s['count']:
            # Recreate acknowledged documents from durable staging without rewriting receipts.
            with connect(m['release']) as c:
                after = 0
                while batch := c.execute('SELECT number,record FROM records WHERE number>? AND number<=? ORDER BY number LIMIT 2000', (after,s['after'])).fetchall():
                    protect(m)
                    client.bulk(s['index'],[arxiv_index.project(n,json.loads(r)) for n,r in batch])
                    after = batch[-1][0]
            verify_segment(client,path(m['release']),s)
    with connect(m['release']) as c:
        while batch := c.execute('SELECT number,record FROM records WHERE number>? ORDER BY number LIMIT 2000', (s['after'],)).fetchall():
            protect(m)
            documents = [arxiv_index.project(n,json.loads(r)) for n,r in batch]
            write_documents(client, path(m['release']), s, documents, batch[-1][0])
            m['indexed'] = s['count']
            save(m)
    seal(client, path(m['release']), s, lambda:protect(m))
    save(m)


def hydrate(m, keys, backend):
    if not keys:
        return {}
    rows = backend.ch(f'SELECT id,record,links FROM {table(m)}.records FINAL WHERE id IN {sql_list(keys)} FORMAT JSONEachRow')
    return {r['id']: json.loads(r['record']) | dict(version_links=json.loads(r['links']), source_release=m['release']) for r in rows}


def validate(m, backend, client):
    from .manticore_build import verify_segment
    s = m['segment']
    if not s or not all(s.get(k) for k in ('sealed','restore_verified','physical_verified')):
        raise RuntimeError('arXiv snapshot is not sealed and restore-verified')
    verify_segment(client, path(m['release']), s)
    count = backend.ch(f'SELECT count() AS n FROM {table(m)}.records FINAL FORMAT JSONEachRow')[0]['n']
    if count != m['selected'] or count != s['count'] or not count:
        raise RuntimeError('arXiv source/index cardinality mismatch')
    # Verify normalized source, hashes and exact links against the complete durable staging stream.
    with connect(m['release']) as c:
        after = 0
        version_count = 0
        while batch := c.execute('SELECT number,id,hash,record,links FROM records WHERE number>? ORDER BY number LIMIT 2000', (after,)).fetchall():
            protect(m)
            found = backend.ch(f"SELECT number,id,hash,record,links FROM {table(m)}.records FINAL WHERE number IN ({','.join(str(r[0]) for r in batch)}) FORMAT JSONEachRow")
            expected = {r[0]:tuple(r[1:]) for r in batch}
            if {r['number']:(r['id'],r['hash'],r['record'],r['links']) for r in found} != expected:
                raise RuntimeError('arXiv full source reconciliation failed')
            version_count += sum(len(json.loads(r[4])['confirmed']) for r in batch)
            after = batch[-1][0]
    actual_versions = backend.ch(f'SELECT count() AS n FROM {table(m)}.versions FINAL FORMAT JSONEachRow')[0]['n']
    if actual_versions != version_count:
        raise RuntimeError('arXiv version-link count mismatch')
    hits,_ = arxiv_index.search(client,m,{},per_page=1)
    if hits['total'] != count or not hits['hits']:
        raise RuntimeError('arXiv API query count mismatch')
    m['validation'] = dict(passed=True, count=count, signature=m['signature'], at=time.time())
    atomic_json(path(m['release'])/'validation.json', m['validation'])


def run(release, until='validate'):
    if until not in STAGES:
        raise ValueError('unknown arXiv stage')
    from .manticore_engine import Client
    from .workspace import claim
    claim()
    directory = path(release)
    with lock(directory / 'worker.lock'):
        m = read(release)
        if m['state'] == 'published':
            return m
        source = Path(m['input_path'])
        if source.stat().st_size != m['input_bytes'] or file_hash(source) != m['input_sha256']:
            raise ValueError('arXiv input changed; use a new source release')
        from .search_versions import require_metadata
        if require_metadata(m['generation'])['signature'] != m['metadata_proof']:
            raise RuntimeError('OpenAlex metadata proof changed')
        backend, client = Backend(writer=True), Client(timeout=600)
        try:
            for stage in STAGES[:STAGES.index(until)+1]:
                if stage in m['completed']:
                    continue
                protect(m)
                m.update(state='running', stage=stage, error=None)
                save(m)
                if stage == 'ingest': ingest(m)
                elif stage == 'link': link(m,backend)
                elif stage == 'store': store(m,backend)
                elif stage == 'index': index(m,client)
                else: validate(m,backend,client)
                m['completed'].append(stage)
                m.update(state='validated' if stage=='validate' else 'paused')
                save(m)
            return m
        except Exception as error:
            m.update(state='paused' if isinstance(error,InterruptedError) else 'failed', error=type(error).__name__+': '+str(error))
            save(m)
            raise
        finally:
            backend.close(); client.close()


def publish(release):
    with lock(ROOT/'var/run/arxiv-publish.lock'), lock(path(release)/'worker.lock'):
        m = read(release)
        if m['state'] not in ('validated','published') or not m.get('validation',{}).get('passed'):
            raise ValueError('validate the complete source release before publishing')
        if active()['generation'] != m['generation']:
            raise ValueError('arXiv mappings target another OpenAlex generation')
        from .manticore_engine import Client
        backend,client = Backend(),Client(timeout=600)
        try:
            validate(m,backend,client)
        finally:
            backend.close(); client.close()
        pointer = ROOT/'var/arxiv-active.json'
        previous = read_json(pointer) if pointer.exists() else None
        m.update(state='published',published=time.time())
        save(m)
        if previous is None or previous['release'] != release:
            atomic_json(pointer, dict(release=release, previous=previous, signature=m['signature']))
        return m


def published(release=None):
    if release is None:
        release = read_json(ROOT/'var/arxiv-active.json')['release']
    m = read(release)
    if m['state'] != 'published' or not m.get('validation',{}).get('passed'):
        raise RuntimeError('arXiv source is not published')
    if active()['generation'] != m['generation']:
        raise RuntimeError('arXiv source mappings require their pinned OpenAlex generation')
    return m


def status():
    try:
        if not (ROOT/'var/arxiv-active.json').exists():
            return dict(ready=False,error='not published')
        m = published()
        recovery = ROOT/'var/run/arxiv-recovery.json'
        if recovery.exists():
            r = read_json(recovery)
            if r.get('release') == m['release'] and r['state'] != 'complete':
                return dict(ready=False, release=m['release'], error=r.get('error') or r['state'])
        return dict(ready=True,release=m['release'],count=m['selected'],min_year=m['min_year'],
                    input_format=m['input_format'], generation=m['generation'],
                    references='not provided by source; use confirmed publication links',
                    raw_available=m['input_format']=='jsonl',data_dates=m.get('data_dates'))
    except (OSError,RuntimeError,ValueError,KeyError,TypeError) as error:
        return dict(ready=False,error='not published' if isinstance(error,FileNotFoundError) else str(error))


def recover():
    from .manticore_engine import Client
    from .manticore_build import restore
    pointer = ROOT/'var/arxiv-active.json'
    if not pointer.exists():
        return
    state = dict(state='restoring',release=read_json(pointer)['release'], started=time.time())
    target = ROOT/'var/run/arxiv-recovery.json'
    atomic_json(target,state)
    client = Client(timeout=600)
    try:
        m = published()
        restore(client,path(m['release']),m['segment'])
        state.update(state='complete')
    except Exception as error:
        state.update(state='failed',error=str(error))
    finally:
        client.close()
        atomic_json(target,state | dict(updated=time.time()))
