"""Independent resumable version relationships; no work or search-index mutation."""
import gzip
import json
import random
import sqlite3
import time
from pathlib import Path

import orjson

from .config import ROOT, active, atomic_json, config, generation_name
from .earth_control import read_json
from .ingest import lock
from .backend import Backend, sql_string, sql_list
from .manticore_build import file_hash, GROUPS
from .semantics import digest
from . import arxiv_pipeline as arxiv, version_groups as rules

STAGES = ('seeds', 'sample', 'targets', 'pilot', 'groups', 'store', 'validate')
QUOTAS = dict(zip(GROUPS, (30000, 35000, 35000)))
BATCH = 2000


def path(release):
    return ROOT/'var/versions'/generation_name(release)


def signature():
    return digest({n:file_hash(Path(__file__).with_name(n)) for n in ('version_groups.py','versions_pipeline.py')})


def read(release):
    m = read_json(path(release)/'release.json')
    if m['release'] != release or m['signature'] != signature():
        raise RuntimeError('version rules changed; create a new release')
    return m


def save(m):
    m['updated'] = time.time()
    atomic_json(path(m['release'])/'release.json', m)


def inputs(m):
    from .search_versions import require_metadata, build_path
    if require_metadata(m['generation'])['signature'] != m['metadata_proof']:
        raise RuntimeError('version metadata proof changed')
    a = arxiv.published(m['arxiv_release'])
    if digest(a) != m['arxiv_identity']:
        raise RuntimeError('version arXiv input changed')
    for name, expected in m['files'].items():
        if file_hash(Path(name)) != expected:
            raise RuntimeError('version input changed: '+name)
    return a, read_json(build_path(m['generation'],m['search_build'])/'build.json')


def plan(release, generation=None, source_release=None, collection='earth_scope_v1', seed=20261113):
    from .search_versions import require_metadata, active as search_active, build_path
    from .earth_collections import collection_path
    if config().get('adopted_from'):
        raise ValueError('adopted workspace cannot build version releases')
    generation = generation or active()['generation']
    a = arxiv.published(source_release)
    if a['generation'] != generation:
        raise ValueError('arXiv and metadata generations differ')
    search = search_active(generation)
    if not search:
        raise ValueError('published Earth search receipts required for the stratified pilot')
    root = collection_path(generation,collection)
    frozen,verified = read_json(root/'manifest.json'),read_json(root/'verification.json')
    if not verified.get('passed') or not verified.get('complete') or frozen['signature'] != search['collection_signature'] or verified['signature'] != frozen['signature']:
        raise ValueError('complete verified Earth collection required')
    spec = dict(schema=1,release=release,generation=generation,metadata_proof=require_metadata(generation)['signature'],
                arxiv_release=a['release'],arxiv_identity=digest(a),collection=collection,collection_signature=frozen['signature'],
                search_build=search['build'],seed=seed,quotas=QUOTAS,signature=signature(),
                files={str(p):file_hash(p) for p in [root/'manifest.json',root/'verification.json',
                       build_path(generation,search['build'])/'build.json',arxiv.path(a['release'])/'staging.sqlite']})
    directory=path(release); directory.mkdir(parents=True,exist_ok=True)
    with lock(directory/'worker.lock'):
        if (directory/'release.json').exists():
            m=read(release)
            if any(m.get(k)!=v for k,v in spec.items()):
                raise ValueError('version input identity changed; choose another release')
            return m
        m=spec|dict(state='planned',completed=[],created=time.time(),error=None,counts={})
        save(m); return m


def connect(release):
    c=sqlite3.connect(path(release)/'staging.sqlite',timeout=30)
    c.execute('PRAGMA journal_mode=DELETE');c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA cache_size=-131072')
    c.executescript('''CREATE TABLE IF NOT EXISTS progress(stage TEXT PRIMARY KEY,value TEXT);
    CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS works(id TEXT PRIMARY KEY,expected TEXT,raw TEXT);
    CREATE TABLE IF NOT EXISTS sample(id TEXT PRIMARY KEY,stratum TEXT,source_hash TEXT,payload TEXT);
    CREATE TABLE IF NOT EXISTS groups(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS members(id TEXT PRIMARY KEY,group_id TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS members_group ON members(group_id);
    CREATE TABLE IF NOT EXISTS reviews(id TEXT PRIMARY KEY,payload TEXT NOT NULL);''')
    return c


def checkpoint(c,stage):
    row=c.execute('SELECT value FROM progress WHERE stage=?',(stage,)).fetchone()
    return json.loads(row[0]) if row else None


def commit(c,stage,value):
    c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)',(stage,json.dumps(value)));c.commit()


def protect(m):
    import shutil
    if (path(m['release'])/'stop.json').exists():
        raise InterruptedError('version job stopped; resume to continue')
    if shutil.disk_usage(path(m['release'])).free < config().get('min_free_gib',1)*1024**3:
        raise InterruptedError('version job reached disk reserve')


def compact(raw):
    return {k:raw.get(k) for k in ('id','doi','ids','type','title','display_name','is_retracted','publication_year','authorships','locations','primary_location')}


def seeds(m,backend):
    import shutil
    import tempfile
    source=arxiv.path(m['arxiv_release'])/'staging.sqlite'
    # Random SQLite evidence reads stay local; only committed outputs are authoritative.
    with tempfile.TemporaryDirectory(prefix='oa-version-evidence-') as directory:
        cached=Path(directory)/'source.sqlite'
        shutil.copyfile(source,cached)
        if file_hash(cached)!=m['files'][str(source)]:
            raise RuntimeError('version source cache checksum mismatch')
        seed_batches(m,cached)


def seed_batches(m,source):
    with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as src, connect(m['release']) as c:
        src.execute('PRAGMA cache_size=-1048576')
        evidence=arxiv.LinkEvidence(src)
        after=checkpoint(c,'seeds') or 0
        while batch:=src.execute('SELECT number,id,record,links FROM records WHERE number>? ORDER BY number LIMIT ?', (after,BATCH)).fetchall():
            protect(m)
            links=arxiv.exact_links([json.loads(r[2]) for r in batch],evidence,m['generation'])
            keys=sorted({v['openalex_id'] for link in links for v in link['confirmed']})
            expected=evidence.details(m['generation'],'works',keys)
            previous=dict(c.execute('SELECT id,expected FROM works WHERE id IN ('+','.join('?' for _ in keys)+')',keys)) if keys else {}
            values=[(key,json.dumps(raw,sort_keys=True)) for key,raw in expected.items()]
            if any(previous.get(key,value)!=value for key,value in values):
                raise RuntimeError('contradictory source evidence')
            c.executemany('INSERT OR IGNORE INTO works(id,expected) VALUES (?,?)',values)
            decisions=[]
            for row,link in zip(batch,links):
                if link != json.loads(row[3]):
                    raise RuntimeError('arXiv seed evidence replay mismatch: '+row[1])
                if len(link['confirmed'])>1 or link['ambiguous']:
                    decisions.append((row[1],json.dumps(dict(source_id=row[1],links=link))))
            c.executemany('INSERT OR REPLACE INTO decisions VALUES (?,?)',decisions)
            after=batch[-1][0];commit(c,'seeds',after)
            m['counts']['seed_records']=after;save(m)
        if c.execute('SELECT count(*) FROM works').fetchone()[0] == 0:
            raise RuntimeError('no seed work evidence')


def sample(m,backend):
    from .search_versions import build_path
    _,build=inputs(m)
    root=build_path(m['generation'],m['search_build'])
    with connect(m['release']) as c:
        for group,quota in m['quotas'].items():
            if checkpoint(c,'sample-'+group):
                continue
            segments=[s for s in build['segments'] if s['entity']=='works' and s['group']==group]
            total=sum(s['count'] for s in segments)
            if total<quota:
                raise RuntimeError('not enough frozen members for '+group)
            picks=sorted(random.Random(str(m['seed'])+group).sample(range(total),quota))
            offset=0; position=0; selected=[]
            for segment in segments:
                for receipt in segment['batches']:
                    protect(m)
                    stop=offset+receipt['count']; indices=[]
                    while position<len(picks) and picks[position]<stop:
                        indices.append(picks[position]-offset);position+=1
                    if indices:
                        p=root/receipt['path']
                        if file_hash(p)!=receipt['sha256']:
                            raise RuntimeError('sample receipt checksum mismatch')
                        rows=orjson.loads(gzip.decompress(p.read_bytes()))
                        if len(rows)!=receipt['count']:
                            raise RuntimeError('sample receipt cardinality mismatch')
                        selected.extend((rows[i]['original_id'],group,rows[i]['source_hash']) for i in indices)
                    offset=stop
            if len(selected)!=quota or len({r[0] for r in selected})!=quota:
                raise RuntimeError('sample selection mismatch')
            c.executemany('INSERT INTO sample(id,stratum,source_hash) VALUES (?,?,?)',selected)
            c.executemany('INSERT OR IGNORE INTO works(id) VALUES (?)',[(r[0],) for r in selected])
            commit(c,'sample-'+group,quota)
            m['counts']['sample_'+group]=quota;save(m)
        rows=c.execute('SELECT id,stratum,source_hash FROM sample ORDER BY stratum,id').fetchall()
        atomic_json(path(m['release'])/'sample.json',dict(seed=m['seed'],rows=rows,sha256=digest(rows)))
        m['sample_sha256']=digest(rows);save(m)


def targets(m,backend):
    from concurrent.futures import ThreadPoolExecutor
    def fetch(keys):
        b=Backend(writer=True)
        try:return b.details(m['generation'],'works',keys)
        finally:b.close()
    with connect(m['release']) as c, ThreadPoolExecutor(8) as pool:
        after=checkpoint(c,'targets') or ''
        sample_hashes=dict(c.execute('SELECT id,source_hash FROM sample'))
        while rows:=c.execute('SELECT id,expected FROM works WHERE id>? ORDER BY id LIMIT ?', (after,BATCH*8)).fetchall():
            protect(m)
            batches=[rows[i:i+BATCH] for i in range(0,len(rows),BATCH)]
            for batch,found in zip(batches,pool.map(fetch,[[r[0] for r in b] for b in batches])):
                protect(m)
                if set(found)!={r[0] for r in batch}:
                    raise RuntimeError('version target missing in frozen metadata')
                values=[]
                for key,expected in batch:
                    raw=found[key]
                    if expected and any(raw.get(k)!=v for k,v in json.loads(expected).items()):
                        raise RuntimeError('version target changed: '+key)
                    if key in sample_hashes and digest(raw)!=sample_hashes[key]:
                        raise RuntimeError('pilot raw metadata digest mismatch: '+key)
                    selected=compact(raw) if key in sample_hashes else {k:raw.get(k) for k in ('id','doi','ids','type','title','display_name','is_retracted')}
                    values.append((json.dumps(selected),key))
                c.executemany('UPDATE works SET raw=? WHERE id=?',values)
                m['counts']['verified_targets']=(checkpoint(c,'target-count') or 0)+len(batch)
                c.execute('INSERT OR REPLACE INTO progress VALUES (?,?)',('target-count',json.dumps(m['counts']['verified_targets'])))
                after=batch[-1][0];commit(c,'targets',after);save(m)


def pilot(m,backend):
    from .model import external_ids
    import re
    with connect(m['release']) as c:
        after=checkpoint(c,'pilot') or ''
        while rows:=c.execute('SELECT s.id,w.raw FROM sample s JOIN works w USING(id) WHERE s.id>? ORDER BY s.id LIMIT ?', (after,BATCH)).fetchall():
            protect(m)
            raws={key:json.loads(raw) for key,raw in rows}
            values=sorted({v for raw in raws.values() for scheme,v in external_ids(raw) if scheme=='doi'})
            resolved=backend.resolve_many(m['generation'],'works','doi',values)
            for key,raw in raws.items():
                dois=[v for scheme,v in external_ids(raw) if scheme=='doi']
                candidates=sorted({v for doi in dois for v in resolved.get(doi,[]) if v!=key})
                urls=[str(loc.get(field) or '') for loc in [raw.get('primary_location') or {}]+(raw.get('locations') or []) for field in ('landing_page_url','pdf_url')]
                arxiv_ids=sorted({match.lower() for url in urls for match in re.findall(r'arxiv\.org/(?:abs|pdf)/((?:[a-z.-]+/)?\d{4}\.\d{4,5}|[a-z.-]+/\d{7})',url,re.I)})
                result=dict(work_id=key,state='candidate' if candidates or arxiv_ids else 'unmatched',duplicate_doi_candidates=candidates,
                            explicit_arxiv_ids=arxiv_ids,automatic_confirmation=False,reason='identifier/link candidates require independent evidence')
                if raw.get('type')=='preprint' and not candidates:
                    from .api import match, Matches, Reference, generation as current_generation
                    from starlette.requests import Request
                    from .model import canonical_id
                    authors=raw.get('authorships') or []
                    author=(authors[0].get('author') or {}).get('display_name') if authors else None
                    title=raw.get('title') or raw.get('display_name')
                    if title:
                        meta=current_generation()
                        if meta['generation']!=m['generation'] or (meta.get('_search_build') or {}).get('build')!=m['search_build']:
                            raise RuntimeError('pilot candidate search build changed')
                        request=Request({'type':'http','query_string':b''})
                        response=match(Matches(references=[Reference(title=title[:2000],author=author,year=raw.get('publication_year'))]),request)
                        if response['generation']!=m['generation']:
                            raise RuntimeError('pilot candidate generation changed')
                        result['title_candidates']=[dict(openalex_id=canonical_id(v['work']['id']),score=v['score']) for v in response['results'][0]['candidates'] if canonical_id(v['work']['id'])!=key]
                        if result['title_candidates']:result['state']='candidate'
                c.execute('UPDATE sample SET payload=? WHERE id=?',(json.dumps(result),key))
                if result['state']=='candidate':
                    c.execute('INSERT OR REPLACE INTO reviews VALUES (?,?)',('pilot:'+key,json.dumps(result)))
            after=rows[-1][0];commit(c,'pilot',after)
            m['counts']['pilot_processed']=c.execute('SELECT count(*) FROM sample WHERE payload IS NOT NULL').fetchone()[0];save(m)


def build_groups(m,backend):
    with connect(m['release']) as c:
        decisions=[]
        work_info={key:dict(type=kind,title=title,is_retracted=retracted) for key,kind,title,retracted in c.execute("SELECT id,json_extract(raw,'$.type'),coalesce(json_extract(raw,'$.title'),json_extract(raw,'$.display_name')),json_extract(raw,'$.is_retracted') FROM works")}
        for key,payload in c.execute('SELECT id,payload FROM decisions ORDER BY id'):
            d=json.loads(payload);ids=[v['openalex_id'] for v in d['links']['confirmed']]
            works={key:work_info[key] for key in ids if key in work_info}
            decisions.append(rules.seed_decision(key,d['links'],works))
        groups,conflicts=rules.groups(m['release'],decisions)
        c.execute('DELETE FROM groups');c.execute('DELETE FROM members');c.execute("DELETE FROM reviews WHERE id NOT LIKE 'pilot:%'")
        for g in groups:
            c.execute('INSERT INTO groups VALUES (?,?)',(g['group_id'],json.dumps(g)))
            c.executemany('INSERT INTO members VALUES (?,?)',[(key,g['group_id']) for key in g['members']])
        for d in [d for d in decisions if d['state'] in ('review','conflict')]+conflicts:
            c.execute('INSERT OR REPLACE INTO reviews VALUES (?,?)',(digest(d),json.dumps(d)))
        commit(c,'groups',len(groups))
        m['counts'].update(groups=len(groups),members=c.execute('SELECT count(*) FROM members').fetchone()[0],reviews=c.execute('SELECT count(*) FROM reviews').fetchone()[0])
        save(m)


def table(m):
    return 'oa_versions_'+generation_name(m['release'])


def store(m,backend):
    db=table(m);backend.ch(f'CREATE DATABASE IF NOT EXISTS {db}')
    for kind in ('groups','reviews'):
        backend.ch(f'CREATE TABLE IF NOT EXISTS {db}.{kind} (id String,payload String) ENGINE=ReplacingMergeTree ORDER BY id')
    backend.ch(f'CREATE TABLE IF NOT EXISTS {db}.members (id String,group_id String) ENGINE=ReplacingMergeTree ORDER BY id')
    with connect(m['release']) as c:
        for kind in ('groups','reviews','members'):
            after=checkpoint(c,'store-'+kind) or ''
            while batch:=c.execute(f"SELECT id,{'group_id' if kind=='members' else 'payload'} FROM {kind} WHERE id>? ORDER BY id LIMIT ?",(after,BATCH)).fetchall():
                protect(m)
                field='group_id' if kind=='members' else 'payload'
                payload=[dict(id=k,**{field:v}) for k,v in batch]
                backend.ch(f'INSERT INTO {db}.{kind} FORMAT JSONEachRow',b''.join(orjson.dumps(r)+b'\n' for r in payload))
                found=backend.ch(f'SELECT id,{field} FROM {db}.{kind} FINAL WHERE id IN {sql_list([r[0] for r in batch])} FORMAT JSONEachRow')
                if {r['id']:r[field] for r in found}!=dict(batch):
                    raise RuntimeError('version write reconciliation failed')
                after=batch[-1][0];commit(c,'store-'+kind,after)


def validate(m,backend):
    inputs(m)
    with connect(m['release']) as c:
        if c.execute('SELECT count(*) FROM sample WHERE payload IS NULL').fetchone()[0] or c.execute('SELECT count(*) FROM sample').fetchone()[0]!=sum(m['quotas'].values()):
            raise RuntimeError('incomplete stratified pilot')
        if c.execute('SELECT count(*) FROM works WHERE raw IS NULL').fetchone()[0]:
            raise RuntimeError('incomplete target verification')
        source=arxiv.read(m['arxiv_release'])
        if checkpoint(c,'seeds')!=source['selected']:
            raise RuntimeError('incomplete seed verification')
        sample_rows=c.execute('SELECT id,stratum,source_hash FROM sample ORDER BY stratum,id').fetchall()
        sample_proof=read_json(path(m['release'])/'sample.json')
        if digest(sample_rows)!=m['sample_sha256'] or digest(sample_proof['rows'])!=m['sample_sha256']:
            raise RuntimeError('pilot identity changed')
        for kind in ('groups','reviews','members'):
            field='group_id' if kind=='members' else 'payload'
            count=c.execute(f'SELECT count(*) FROM {kind}').fetchone()[0]
            actual=backend.ch(f'SELECT count() n FROM {table(m)}.{kind} FINAL FORMAT JSONEachRow')[0]['n']
            if actual!=count:
                raise RuntimeError('version stored count mismatch')
            after=''
            while batch:=c.execute(f'SELECT id,{field} FROM {kind} WHERE id>? ORDER BY id LIMIT ?', (after,BATCH)).fetchall():
                protect(m)
                found=backend.ch(f'SELECT id,{field} FROM {table(m)}.{kind} FINAL WHERE id IN {sql_list([r[0] for r in batch])} FORMAT JSONEachRow')
                if {r['id']:r[field] for r in found}!=dict(batch):
                    raise RuntimeError('version full reconciliation failed')
                after=batch[-1][0]
        for key,payload in c.execute('SELECT id,payload FROM groups'):
            g=json.loads(payload)
            members=c.execute('SELECT id FROM members WHERE group_id=? ORDER BY id',(key,)).fetchall()
            if [r[0] for r in members]!=g['members'] or g['preferred_openalex_id'] not in g['members'] or len(g['members'])<2:
                raise RuntimeError('invalid version membership')
    m['validation']=dict(passed=True,at=time.time(),signature=m['signature'],counts=m['counts'],sample_sha256=m['sample_sha256'],
                         automatic_rules=rules.RULE,title_matching='not confirmed; candidate only',full_earth_scan=False)
    atomic_json(path(m['release'])/'validation.json',m['validation']);save(m)


def run(release,until='validate'):
    from .workspace import claim
    if until not in STAGES:raise ValueError('unknown versions stage')
    if config().get('adopted_from'):raise ValueError('adopted workspace cannot run versions')
    claim()
    with lock(path(release)/'worker.lock'):
        m=read(release)
        if m['state']=='published':return m
        inputs(m);b=Backend(writer=True)
        try:
            for stage in STAGES[:STAGES.index(until)+1]:
                if stage in m['completed']:continue
                protect(m);m.update(state='running',stage=stage,error=None);save(m)
                {'seeds':seeds,'sample':sample,'targets':targets,'pilot':pilot,'groups':build_groups,'store':store,'validate':validate}[stage](m,b)
                m['completed'].append(stage);m['state']='validated' if stage=='validate' else 'paused';save(m)
            return m
        except Exception as error:
            m.update(state='paused' if isinstance(error,InterruptedError) else 'failed',error=type(error).__name__+': '+str(error));save(m);raise
        finally:b.close()


def lineage(m, previous):
    mappings=[]
    if previous and previous['release']!=m['release']:
        old=path(previous['release'])/'staging.sqlite'
        with connect(m['release']) as c, sqlite3.connect(old.as_uri()+'?mode=ro',uri=True) as prior:
            for key,payload in prior.execute('SELECT id,payload FROM groups ORDER BY id'):
                members=json.loads(payload)['members']
                targets=sorted({r[0] for r in c.execute('SELECT group_id FROM members WHERE id IN ('+','.join('?' for _ in members)+')',members)})
                mappings.append(dict(previous_group_id=key,current_group_ids=targets,
                                     state='retired' if not targets else 'split' if len(targets)>1 else 'linked'))
    atomic_json(path(m['release'])/'lineage.json',dict(previous_release=previous['release'] if previous else None,
                relationship_release=m['release'],mappings=mappings,sha256=digest(mappings)))


def publish(release):
    from .workspace import claim
    claim()
    with lock(ROOT/'var/run/versions-publish.lock'),lock(path(release)/'worker.lock'):
        m=read(release)
        if m['state'] not in ('validated','published'):raise ValueError('validate versions before publishing')
        if active()['generation']!=m['generation']:raise ValueError('version generation is not active')
        b=Backend(writer=True)
        try:validate(m,b)
        finally:b.close()
        pointer=ROOT/'var/versions-active.json';previous=read_json(pointer) if pointer.exists() else None
        if previous is None or previous['release']!=release:lineage(m,previous)
        m.update(state='published',published=time.time());save(m)
        if previous is None or previous['release']!=release:
            atomic_json(pointer,dict(release=release,signature=m['signature'],previous=previous))
        return m


def published():
    p=read_json(ROOT/'var/versions-active.json');m=read(p['release'])
    if p['signature']!=m['signature'] or m['state']!='published' or not m.get('validation',{}).get('passed') or active()['generation']!=m['generation']:
        raise RuntimeError('version relationships are unavailable for this metadata generation')
    return m


def status():
    if not (ROOT/'var/versions-active.json').exists():return dict(ready=False,error='not published')
    try:
        m=published()
        return dict(ready=True,release=m['release'],generation=m['generation'],counts=m['counts'],
                    seed_release=m['arxiv_release'],seed_complete=True,pilot_records=sum(m['quotas'].values()),
                    full_earth_scan=False,search_folding=False)
    except (OSError,RuntimeError,ValueError,KeyError,TypeError) as error:
        return dict(ready=False,error=str(error))
