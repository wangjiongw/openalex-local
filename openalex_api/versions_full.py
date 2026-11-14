"""Full-index, resumable bibliography matching independent of schema-1 releases."""
import inspect
import json
import time
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import orjson
import httpx

from . import version_matching as rules, versions_pipeline as legacy
from .backend import Backend, sql_list, sql_string
from .config import ROOT, active, atomic_json, config, generation_name
from .earth_control import read_json
from .ingest import lock
from .manticore_build import file_hash
from .manticore_engine import Client, checked_name
from .semantics import digest

STAGES = ('features','sample','candidates','review','groups','validate')
BATCH = 2000


def path(release):
    return ROOT/'var/versions'/generation_name(release)


def table(m):
    return 'oa_versions_'+generation_name(m['release'])


def signature():
    return digest({n:file_hash(Path(__file__).with_name(n)) for n in ('version_matching.py','versions_full.py')})


def save(m):
    m['updated'] = time.time()
    atomic_json(path(m['release'])/'release.json',m)


def read(release):
    m = read_json(path(release)/'release.json')
    if m['schema'] != 2 or m['release'] != release or m['signature'] != signature():
        raise RuntimeError('full matching implementation changed; create a new relationship release')
    return m


def plan(release, generation=None, collection='earth_scope_v1', seed=20261113, pilot_size=1000000, workers=4, stratum=None, batch_size=5000000):
    from . import versions_reader
    from .search_versions import active as search_active, build_path, require_metadata
    from .earth_collections import collection_path
    if config().get('adopted_from'): raise ValueError('adopted workspace cannot build relationships')
    generation = generation or active()['generation']
    parent = versions_reader.published()
    if parent['generation'] != generation: raise ValueError('relationship parent generation differs')
    search = search_active(generation)
    if not search: raise ValueError('published text index required')
    buildfile = build_path(generation,search['build'])/'build.json'
    build = read_json(buildfile)
    collectionfile = collection_path(generation,collection)/'manifest.json'
    frozen = read_json(collectionfile)
    if frozen['signature'] != search['collection_signature']: raise ValueError('collection differs from published search')
    segments = [{k:s[k] for k in ('index','group','count','snapshot')} for s in build['segments'] if s['entity']=='works']
    total = sum(s['count'] for s in segments)
    if stratum and stratum not in {v['group'] for v in segments}:raise ValueError('stratum absent from frozen search')
    if not 1 <= batch_size <= total:batch_size=min(5000000,total)
    if not 1 <= pilot_size <= total or not 1 <= workers <= 16: raise ValueError('invalid pilot size or workers')
    # Feature compatibility excludes candidate and confirmation policy changes.
    feature_signature = digest(dict(rule=rules.FEATURE_RULE,extract=inspect.getsource(rules.feature),posting_projection=inspect.getsource(rules.blocks),stop=sorted(rules.STOP),normalization=inspect.getsource(rules.normalize_title),notice=inspect.getsource(legacy.rules.notice),build=file_hash(buildfile)))
    spec = dict(schema=2,profile='full-index',release=release,generation=generation,collection=collection,
                search_build=search['build'],metadata_proof=require_metadata(generation)['signature'],
                parent_release=parent['release'],parent_signature=parent['signature'],
                files={str(buildfile):file_hash(buildfile),str(collectionfile):file_hash(collectionfile)},
                feature_signature=feature_signature,candidate_signature=digest(dict(rule=rules.CANDIDATE_RULE,blocks=inspect.getsource(rules.blocks),evidence=inspect.getsource(rules.evidence),window=rules.YEAR_WINDOW,negations=sorted(rules.NEGATIONS),stop=sorted(rules.STOP),cap=rules.MAX_CANDIDATES)),
                confirmation_signature=digest(dict(rule=rules.POLICY_RULE,grid=rules.GRID,proposed=inspect.getsource(rules.proposed))),
                feature_database='oa_vfeatures_'+feature_signature[:16],seed=seed,pilot_size=pilot_size,
                workers=workers,stratum=stratum,batch_size=batch_size,expected_works=total,signature=signature(),segments=segments)
    path(release).mkdir(parents=True,exist_ok=True)
    with lock(path(release)/'worker.lock'):
        if (path(release)/'release.json').exists():
            m=read(release)
            if any(m.get(k)!=v for k,v in spec.items()): raise ValueError('matching inputs changed; choose a new release')
            return m
        m=spec|dict(state='planned',completed=[],counts={},created=time.time(),error=None)
        if parent.get('calibration',{}).get('passed') and all(parent.get(k)==spec[k] for k in ('feature_signature','candidate_signature','confirmation_signature')):
            m['calibration']=parent['calibration']|dict(inherited_from=parent['release'])
            m['review_identity']=parent['review_identity']
        save(m);return m


def inputs(m):
    from . import versions_reader
    from .search_versions import require_metadata
    if require_metadata(m['generation'])['signature'] != m['metadata_proof']: raise RuntimeError('metadata proof changed')
    for filename, expected in m['files'].items():
        if file_hash(Path(filename)) != expected: raise RuntimeError('matching input changed: '+filename)
    parent=versions_reader.read(m['parent_release'])
    if parent['signature']!=m['parent_signature'] or parent['state']!='published': raise RuntimeError('relationship parent proof changed')
    return parent


def protect(m):
    legacy.protect(m)


def insert(b, name, rows):
    if rows: b.ch(f'INSERT INTO {name} FORMAT JSONEachRow',b''.join(orjson.dumps(r)+b'\n' for r in rows))


def native_rows(client, query):
    for attempt in range(4):
        try:return client.rows(query)
        except (httpx.TimeoutException,httpx.NetworkError):
            if attempt==3:raise
            time.sleep((0.2,1,3)[attempt])


def feature_root(m):
    return ROOT/'var/version-features'/m['feature_signature']


def feature_tables(m,b):
    db=m['feature_database']
    b.ch(f'CREATE DATABASE IF NOT EXISTS {db}')
    b.ch(f'CREATE TABLE IF NOT EXISTS {db}.features (id String,payload String) ENGINE=ReplacingMergeTree ORDER BY id')
    b.ch(f'CREATE TABLE IF NOT EXISTS {db}.postings (key String,id String) ENGINE=ReplacingMergeTree ORDER BY (key,id)')


def features(m,b):
    root=feature_root(m);root.mkdir(parents=True,exist_ok=True)
    with lock(root/'worker.lock'):
        progressfile=root/'progress.json'
        p=read_json(progressfile) if progressfile.exists() else dict(signature=m['feature_signature'],segments={},complete=False)
        if p['signature']!=m['feature_signature']: raise RuntimeError('feature checkpoint input mismatch')
        exists=all(int(b.ch(f"EXISTS TABLE {m['feature_database']}.{name} FORMAT JSONEachRow")[0]['result']) for name in ('features','postings'))
        if p['segments'] and not exists: raise RuntimeError('feature table missing after checkpoint; restore dependencies')
        feature_tables(m,b)
        if p['complete']:
            m['counts']['features']=m['expected_works'];m['feature_receipt']=file_hash(progressfile);save(m);return
        started=time.monotonic();initial=sum(v['count'] for v in p['segments'].values())
        def scan(segment):
            state=p['segments'].get(segment['index'],dict(after=0,count=0,chain='',bytes=0))
            backend=Backend(writer=True);client=Client(timeout=600)
            try:
                while state['count']<segment['count']:
                    protect(m)
                    size=min(BATCH,segment['count']-state['count'])
                    fields=('name_literal','publication_year','type','is_retracted','authorships','external_ids')
                    selection=','.join('attrs.'+f+' AS '+f for f in fields)
                    rows=native_rows(client,f'SELECT id,original_id,source_hash,{selection} FROM {checked_name(segment["index"])} WHERE id>{int(state["after"])} ORDER BY id ASC LIMIT {size} OPTION max_matches={size},threads=1')
                    if not rows or len(rows)>size: raise RuntimeError('matching feature source coverage mismatch')
                    projected=[]
                    for row in rows:
                        attrs={f:row[f] for f in fields}
                        for f in ('authorships','external_ids'):
                            if isinstance(attrs[f],str): attrs[f]=json.loads(attrs[f])
                        f=rules.feature(row|dict(attrs=attrs),segment['group'])
                        projected.append(dict(id=f['id'],payload=orjson.dumps(f).decode()))
                    ids=[r['id'] for r in projected]
                    insert(backend,m['feature_database']+'.features',projected)
                    postings=[dict(key=k,id=r['id']) for r in projected for k in rules.blocks(json.loads(r['payload']))]
                    insert(backend,m['feature_database']+'.postings',postings)
                    found=backend.ch(f'SELECT id,payload FROM {m["feature_database"]}.features FINAL WHERE id IN {sql_list(ids)} ORDER BY id FORMAT JSONEachRow')
                    if {v['id']:v['payload'] for v in found}!={v['id']:v['payload'] for v in projected}: raise RuntimeError('feature write reconciliation failed')
                    receipt=dict(after=int(rows[-1]['id']),count=state['count']+len(rows),
                                 chain=digest([state['chain'],projected,postings]),bytes=state['bytes']+sum(len(v['payload']) for v in projected))
                    atomic_json(root/(segment['index']+'.json'),receipt)
                    state=receipt
                    yield segment['index'],state
            finally: backend.close();client.close()
        # Keep one batch in flight per segment and checkpoint acknowledged writes only.
        generators={s['index']:scan(s) for s in m['segments']}
        with ThreadPoolExecutor(max_workers=m['workers']) as pool:
            pending={pool.submit(next,g,None):k for k,g in generators.items()}
            from concurrent.futures import wait,FIRST_COMPLETED
            while pending:
                done,_=wait(pending,timeout=5,return_when=FIRST_COMPLETED)
                for future in done:
                    key=pending.pop(future);result=future.result()
                    if result:
                        _,state=result;p['segments'][key]=state
                        atomic_json(progressfile,p)
                        pending[pool.submit(next,generators[key],None)]=key
                scanned=sum(v['count'] for v in p['segments'].values());elapsed=max(.001,time.monotonic()-started)
                speed=(scanned-initial)/elapsed
                m['counts']['features']=scanned
                m['progress']=dict(records_per_second=speed,eta_seconds=(m['expected_works']-scanned)/max(.001,speed),bytes=sum(v['bytes'] for v in p['segments'].values()),workers=m['workers'],updated=time.time())
                save(m)
        if any(p['segments'].get(s['index'],{}).get('count')!=s['count'] for s in m['segments']): raise RuntimeError('incomplete feature coverage')
        actual=int(b.ch(f'SELECT count() n FROM {m["feature_database"]}.features FINAL FORMAT JSONEachRow')[0]['n'])
        if actual!=m['expected_works']: raise RuntimeError('feature cardinality differs from frozen membership')
        p['complete']=True;atomic_json(progressfile,p)
        m['feature_receipt']=file_hash(progressfile);save(m)


def tables(m,b):
    db=table(m);b.ch(f'CREATE DATABASE IF NOT EXISTS {db}')
    for name in ('groups','reviews','sample','candidates','coverage'):
        b.ch(f'CREATE TABLE IF NOT EXISTS {db}.{name} (id String,payload String) ENGINE=ReplacingMergeTree ORDER BY id')
    b.ch(f'CREATE TABLE IF NOT EXISTS {db}.members (id String,group_id String) ENGINE=ReplacingMergeTree ORDER BY id')


def sample(m,b):
    tables(m,b);parent=inputs_parent(m);db=table(m)
    exclusion=''
    if parent.get('schema',1)==2:
        for name in ('coverage','candidates'):
            b.ch(f'INSERT INTO {db}.{name} SELECT * FROM {table(parent)}.{name} FINAL')
        exclusion=f"id NOT IN (SELECT id FROM {table(parent)}.coverage FINAL)"
    conditions=[exclusion] if exclusion else []
    if m.get('stratum'):conditions.append(f"JSONExtractString(payload,'stratum')={sql_string(m['stratum'])}")
    where=' WHERE '+' AND '.join(conditions) if conditions else ''
    order='id' if m.get('stratum') else f"sipHash64(concat(id,{sql_string(str(m['seed']))}))"
    limit=m['batch_size'] if m.get('stratum') else m['pilot_size']
    b.ch(f'CREATE TABLE IF NOT EXISTS {db}.sample_ids (id String) ENGINE=ReplacingMergeTree ORDER BY id')
    receipt=path(m['release'])/'sample-selection.json'
    identity=digest(dict(feature=m['feature_signature'],parent=m['parent_signature'],seed=m['seed'],stratum=m.get('stratum'),limit=limit))
    if receipt.exists():
        selected=read_json(receipt)
        if selected['identity']!=identity:raise RuntimeError('sample selection identity changed')
    else:
        # Sorting full payloads exceeds query memory on large collections; rank IDs only.
        b.ch(f'INSERT INTO {db}.sample_ids SELECT id FROM {m["feature_database"]}.features FINAL{where} ORDER BY {order} LIMIT {int(limit)} SETTINGS max_threads=2,max_final_threads=2,max_memory_usage=8589934592,max_bytes_before_external_sort=268435456')
        selected=dict(identity=identity,**b.ch(f'SELECT count() n,sum(cityHash64(id)) checksum FROM {db}.sample_ids FINAL FORMAT JSONEachRow')[0])
        if not 0<int(selected['n'])<=limit:raise RuntimeError('no unprocessed works or invalid sample cardinality')
        atomic_json(receipt,selected)
    actual=b.ch(f'SELECT count() n,sum(cityHash64(id)) checksum FROM {db}.sample_ids FINAL FORMAT JSONEachRow')[0]
    if any(actual[k]!=selected[k] for k in ('n','checksum')):raise RuntimeError('frozen sample IDs changed')
    checkpoint=path(m['release'])/'sample-progress.json'
    progress=read_json(checkpoint) if checkpoint.exists() else dict(identity=identity,after='',processed=0)
    if progress['identity']!=identity:raise RuntimeError('sample checkpoint identity changed')
    initial=progress['processed'];started=time.monotonic()
    while ids:=b.ch(f'SELECT id FROM {db}.sample_ids FINAL WHERE id>{sql_string(progress["after"])} ORDER BY id LIMIT {BATCH} FORMAT JSONEachRow'):
        protect(m);keys=[v['id'] for v in ids]
        rows=b.ch(f'SELECT id,payload FROM {m["feature_database"]}.features FINAL WHERE id IN {sql_list(keys)} ORDER BY id FORMAT JSONEachRow')
        if [v['id'] for v in rows]!=keys:raise RuntimeError('selected sample feature missing')
        insert(b,db+'.sample',rows)
        found=b.ch(f'SELECT id,payload FROM {db}.sample FINAL WHERE id IN {sql_list(keys)} ORDER BY id FORMAT JSONEachRow')
        if found!=rows:raise RuntimeError('sample feature write reconciliation failed')
        progress.update(after=keys[-1],processed=progress['processed']+len(keys));atomic_json(checkpoint,progress)
        speed=(progress['processed']-initial)/max(.001,time.monotonic()-started)
        m['counts'].update(sample_ids=int(selected['n']),sample=progress['processed'])
        m['progress']=dict(records_per_second=speed,eta_seconds=(int(selected['n'])-progress['processed'])/max(.001,speed),updated=time.time());save(m)
    count=int(b.ch(f'SELECT count() n FROM {db}.sample FINAL FORMAT JSONEachRow')[0]['n'])
    if count!=int(selected['n']) or progress['processed']!=count:raise RuntimeError('sample hydration coverage mismatch')
    m['selected_works']=count;m['counts']['sample']=count;save(m)


def key_batches(values):
    batch=[];size=0
    for value in sorted(set(values)):
        length=len(sql_string(value).encode('utf-8'))+1
        if length>131072:raise ValueError('single candidate key exceeds SQL byte budget')
        if batch and (size+length>131072 or len(batch)>=BATCH):
            yield batch;batch=[];size=0
        batch.append(value);size+=length
    if batch:yield batch


def read_batches(backend, queries, workers):
    with ThreadPoolExecutor(max_workers=max(1,int(workers))) as pool:
        for rows in pool.map(backend.ch,queries):
            yield from rows


def fetch_features(m,b,ids):
    queries=(f'SELECT id,payload FROM {m["feature_database"]}.features FINAL WHERE id IN {sql_list(batch)} FORMAT JSONEachRow' for batch in key_batches(ids))
    return {v['id']:json.loads(v['payload']) for v in read_batches(b,queries,m.get('workers',1))}


def candidate_evidence(source, target, methods):
    shared_doi='shared_doi_review' in methods
    if not shared_doi:
        if source['year'] and target['year'] and abs(source['year']-target['year'])>rules.YEAR_WINDOW:return None
        matcher=SequenceMatcher(None,source['normalized'],target['normalized'],autojunk=False)
        if matcher.real_quick_ratio()<0.6 or matcher.quick_ratio()<0.6:return None
    result=rules.evidence(source,target,methods)
    if not shared_doi and result['title_similarity']<0.6:return None
    return result


def inputs_parent(m):
    from . import versions_reader
    return versions_reader.read(m['parent_release'])


def candidates(m,b):
    tables(m,b);checkpoint=path(m['release'])/'candidates-progress.json'
    p=read_json(checkpoint) if checkpoint.exists() else dict(after='',processed=0,pairs=0,overflow=0)
    initial=p['processed'];started=time.monotonic()
    while rows:=b.ch(f'SELECT id,payload FROM {table(m)}.sample FINAL WHERE id>{sql_string(p["after"])} ORDER BY id LIMIT {BATCH} FORMAT JSONEachRow'):
        protect(m)
        source={v['id']:json.loads(v['payload']) for v in rows}
        key_sources={};overflow=set()
        for key,f in source.items():
            for block in rules.blocks(f):
                if len(sql_string(block).encode('utf-8'))+1>131072:
                    overflow.add(key);continue
                key_sources.setdefault(block,[]).append(key)
        targets={k:set() for k in source};methods={}
        if key_sources:
            by_key={}
            queries=(f'SELECT key,id FROM {m["feature_database"]}.postings FINAL WHERE key IN {sql_list(batch)} ORDER BY key,id LIMIT {rules.MAX_CANDIDATES+1} BY key FORMAT JSONEachRow' for batch in key_batches(key_sources))
            for r in read_batches(b,queries,m['workers']):by_key.setdefault(r['key'],[]).append(r['id'])
            for block,ids in by_key.items():
                for key in key_sources[block]:
                    if len(ids)>rules.MAX_CANDIDATES:overflow.add(key)
                    for target in ids:
                        if target==key:continue
                        targets[key].add(target)
                        methods.setdefault((key,target),set()).add('normalized_title' if block.startswith('t:') else 'author_title_block')
        fallback=[k for k,f in source.items() if f['kind']=='preprint' and not targets[k] and f['normalized'] and k not in overflow]
        def native(key):
            f=source[key]
            words=sorted({w for w in f['normalized'].split() if len(w)>=5 and w not in rules.STOP},key=lambda w:(-len(w),w))[:3]
            if not words:return key,[]
            client=Client(timeout=120)
            try:
                indexes=','.join(checked_name(s['index']) for s in m['segments'])
                year=f" AND publication_year BETWEEN {max(0,f['year']-rules.YEAR_WINDOW)} AND {f['year']+rules.YEAR_WINDOW}" if f['year'] else ''
                rows=native_rows(client,f"SELECT original_id FROM {indexes} WHERE MATCH({sql_string('@name '+ ' '.join(words))}){year} LIMIT {rules.MAX_CANDIDATES+1} OPTION max_matches={rules.MAX_CANDIDATES+1},threads=1")
                return key,[v['original_id'] for v in rows if v['original_id']!=key]
            finally:client.close()
        with ThreadPoolExecutor(max_workers=m['workers']) as pool:
            for key,ids in pool.map(native,fallback):
                if len(ids)>=rules.MAX_CANDIDATES:overflow.add(key)
                for target in ids:
                    targets[key].add(target);methods.setdefault((key,target),set()).add('native_title_terms')
        # DOI ambiguity is retained as review, including targets outside the text scope.
        dois={v[4:]:[] for f in source.values() for v in f['identifiers'] if v.startswith('doi:')}
        resolved={}
        for batch in key_batches(dois):resolved.update(b.resolve_many(m['generation'],'works','doi',batch))
        outside=set()
        for key,f in source.items():
            for value in f['identifiers']:
                if not value.startswith('doi:'):continue
                for target in resolved.get(value[4:],[]):
                    if target!=key:
                        targets[key].add(target);outside.add(target)
                        methods.setdefault((key,target),set()).add('shared_doi_review')
            if len(targets[key])>rules.MAX_CANDIDATES:overflow.add(key)
        selected={k:sorted(v)[:rules.MAX_CANDIDATES] for k,v in targets.items()}
        all_targets={v for ids in selected.values() for v in ids}
        found=fetch_features(m,b,all_targets)
        # Only explicit-ID candidates may hydrate outside-scope metadata.
        missing=sorted((all_targets-found.keys()) & outside)
        for offset in range(0,len(missing),BATCH):
            raw=b.details(m['generation'],'works',missing[offset:offset+BATCH])
            from . import model
            for key,record in raw.items():
                attrs=model.project('works',record);attrs['name_literal']=record.get('title') or record.get('display_name') or ''
                found[key]=rules.feature(dict(id=int(key[1:]),original_id=key,source_hash=digest(record),attrs=attrs),'outside_text_scope')
        parent_map={};parent_table=legacy.table(inputs_parent(m))
        queries=(f'SELECT id,group_id FROM {parent_table}.members FINAL WHERE id IN {sql_list(batch)} FORMAT JSONEachRow' for batch in key_batches(set(source)|all_targets))
        parent_map.update({v['id']:v['group_id'] for v in read_batches(b,queries,m['workers'])})
        output=[];coverage=[]
        for key,f in source.items():
            scored=[]
            for target in selected[key]:
                if target not in found: raise RuntimeError('candidate target missing from pinned metadata/index')
                e=candidate_evidence(f,found[target],methods[(key,target)])
                if e is None:continue
                if 'shared_doi_review' in e['methods']:e['conflicts'].append('duplicate_doi_requires_review')
                if key in overflow:e['conflicts'].append('candidate_overflow')
                anchor=target if f['kind']=='preprint' else key
                scored.append(dict(id=rules.pair_id(key,target),work_id=key,target_id=target,anchor=anchor,
                                   stratum=f['stratum'],source=f,target=found[target],evidence=e,state='review',parent_groups=sorted({parent_map[v] for v in (key,target) if v in parent_map}),known_pair=bool(parent_map.get(key) and parent_map.get(key)==parent_map.get(target))))
            eligible=[r for r in scored if rules.proposed(r['evidence'],min(rules.GRID))]
            anchors={r['anchor'] for r in eligible}
            ranked=sorted((r['evidence']['title_similarity'] for r in scored if not r['evidence']['conflicts']),reverse=True)
            margin=ranked[0]-ranked[1] if len(ranked)>1 else 1.0
            for r in scored:
                r['evidence']['candidate_margin']=margin
                r['unique_anchor']=len(anchors)==1 and r['anchor'] in anchors and key not in overflow
                if not r['unique_anchor']:r['evidence']['conflicts'].append('competing_or_unqualified_anchor')
                # Direction-specific row IDs preserve processing coverage for both records.
                output.append(dict(id=key+':'+r['target_id'],payload=json.dumps(r)))
            coverage.append(dict(id=key,payload=json.dumps(dict(state='overflow_review' if key in overflow else 'processed',candidates=len(scored)))))
        insert(b,table(m)+'.candidates',output);insert(b,table(m)+'.coverage',coverage)
        check=b.ch(f'SELECT count() n FROM {table(m)}.coverage FINAL WHERE id IN {sql_list(list(source))} FORMAT JSONEachRow')[0]['n']
        if int(check)!=len(source):raise RuntimeError('candidate coverage write mismatch')
        p.update(after=rows[-1]['id'],processed=p['processed']+len(rows),pairs=p['pairs']+len(output),overflow=p['overflow']+len(overflow))
        atomic_json(checkpoint,p)
        speed=(p['processed']-initial)/max(.001,time.monotonic()-started)
        m['counts'].update(matched=p['processed'],pairs=p['pairs'],overflow=p['overflow'])
        m['progress']=dict(records_per_second=speed,eta_seconds=(m['selected_works']-p['processed'])/max(.001,speed),updated=time.time());save(m)


def candidate_rows(m,b):
    for row in b.ch_stream(f"SELECT payload FROM {table(m)}.candidates FINAL WHERE splitByChar(':',id)[1] IN (SELECT id FROM {table(m)}.sample FINAL) ORDER BY id FORMAT JSONEachRow"):
        yield json.loads(row['payload'])


def review(m,b):
    """Freeze disjoint connected components before exporting development/holdout pairs."""
    if m.get('calibration',{}).get('passed'):return
    root=path(m['release']);target=root/'review-package.json'
    if target.exists():
        package=read_json(target)
        if package['identity']!=m.get('review_identity'):raise RuntimeError('review package identity changed')
        return
    rows={};parents={}
    def find(k):
        parents.setdefault(k,k)
        if parents[k]!=k:parents[k]=find(parents[k])
        return parents[k]
    for r in candidate_rows(m,b):
        rows.setdefault(r['id'],r)
        nodes=[r['work_id'],r['target_id']]+r.get('parent_groups',[])
        for key in nodes[1:]:
            a,c=find(nodes[0]),find(key);parents[max(a,c)]=min(a,c)
    components={}
    for key in parents:components.setdefault(find(key),[]).append(key)
    choices=dict(development=[],blind=[],hard_negative=[]);selected_studies=set()
    for r in sorted(rows.values(),key=lambda r:digest([m['seed'],r['id']])):
        part=rules.partition(components[find(r['work_id'])],m['seed'])
        role='development' if part<3 else 'blind' if part<8 and rules.proposed(r['evidence']) and r['unique_anchor'] and not r.get('known_pair') else 'hard_negative' if part>=8 and not rules.proposed(r['evidence']) else None
        study=find(r['work_id'])
        if study in selected_studies:continue
        if role and len(choices[role])<dict(development=500,blind=600,hard_negative=400)[role]:
            choices[role].append(r|dict(role=role,label=None,reviewer=None));selected_studies.add(study)
    selected=[r for values in choices.values() for r in values]
    identity=digest(dict(feature=m['feature_signature'],candidate=m['candidate_signature'],confirmation=m['confirmation_signature'],pairs=[(r['id'],r['role'],r['evidence']) for r in selected]))
    package=dict(threshold=0.99,schema=1,release=m['release'],identity=identity,counts={k:len(v) for k,v in choices.items()},required=dict(development=500,blind=600,hard_negative=400),pairs=selected)
    atomic_json(target,package);m['review_identity']=identity;m['calibration']=dict(state='awaiting_independent_labels',counts=package['counts']);save(m)
    # Blind reviewers receive metadata without policy scores or proposed decisions.
    public=[{k:r[k] for k in ('id','source','target')}|dict(label=None,reviewer=None) for r in selected if r['role']!='development']
    atomic_json(root/'review-blind.json',dict(identity=identity,pairs=public))
    atomic_json(root/'review-development.json',dict(identity=identity,pairs=choices['development']))


def import_labels(release,filename,threshold=0.99):
    from .workspace import claim
    claim()
    with lock(path(release)/'worker.lock'):
        m=read(release)
        if m['state']=='published':raise ValueError('published relationship release is immutable')
        package=read_json(path(release)/'review-package.json');labels=read_json(Path(filename))
        if threshold!=package['threshold']:raise ValueError('holdout threshold is frozen; create a new rule and independent package to change it')
        prior=path(release)/'calibration.json'
        if prior.exists():
            if digest(labels)==digest(read_json(path(release)/'review-labels.json')):return read_json(prior)
            raise ValueError('holdout already consumed; create a new rule and independent holdout')
        if labels.get('identity')!=package['identity']: raise ValueError('label package identity mismatch')
        if labels.get('independent_review') is not True: raise ValueError('independent_review attestation required; generated labels are not an audit')
        by_id={r['id']:r for r in labels['pairs']}
        if len(by_id)!=len(labels['pairs']) or set(by_id)!={r['id'] for r in package['pairs']}:raise ValueError('label IDs differ from frozen review package')
        rows=[r|{k:by_id[r['id']].get(k) for k in ('label','reviewer')} for r in package['pairs']]
        report=rules.calibrate(rows,threshold,package['identity'])
        atomic_json(path(release)/'calibration.json',report)
        atomic_json(path(release)/'review-labels.json',labels)
        m['calibration']=report|dict(state='passed' if report['passed'] else 'failed_new_holdout_required')
        m['completed']=[s for s in m['completed'] if s not in ('groups','validate')]
        save(m);return report


def groups(m,b):
    parent=inputs(m);tables(m,b);db=table(m);prior=legacy.table(parent)
    # Full parent snapshots retain every confirmed arXiv group, including other disciplines.
    for kind in ('groups','members','reviews'):
        b.ch(f'INSERT INTO {db}.{kind} SELECT * FROM {prior}.{kind} FINAL')
    calibration=m.get('calibration',{});passed=calibration.get('passed',False)
    stars={};seen={};reviews=[]
    for r in candidate_rows(m,b):
        eligible=passed and r['stratum'] in calibration['allowed_strata'] and rules.proposed(r['evidence'],calibration['threshold']) and r['unique_anchor']
        if not eligible:
            reviews.append(dict(id=r['id'],payload=json.dumps(r)))
            if len(reviews)>=BATCH:insert(b,db+'.reviews',reviews);reviews=[]
            continue
        anchor=r['anchor'];member=r['work_id'] if r['target_id']==anchor else r['target_id']
        stars.setdefault(anchor,{}).setdefault(member,r)
        seen.setdefault(member,set()).add(anchor)
    insert(b,db+'.reviews',reviews)
    expected={}
    for members in stars.values():
        for r in members.values():
            for f in (r['source'],r['target']):
                if f['id'] in expected and expected[f['id']]!=f['source_hash']:raise RuntimeError('contradictory candidate source hashes')
                expected[f['id']]=f['source_hash']
    verified={};ordered=sorted(expected)
    for offset in range(0,len(ordered),BATCH):
        protect(m);keys=ordered[offset:offset+BATCH];raws=b.details(m['generation'],'works',keys)
        if set(raws)!=set(keys) or any(digest(raws[k])!=expected[k] for k in keys):raise RuntimeError('proposed confirmation raw checksum mismatch')
        verified.update({k:not legacy.rules.notice(raws[k]) for k in keys})
    m['counts']['raw_verified']=len(verified)
    for anchor,members in sorted(stars.items()):
        protect(m);keys=sorted({anchor,*members})
        old=b.ch(f'SELECT id,group_id FROM {prior}.members FINAL WHERE id IN {sql_list(keys)} FORMAT JSONEachRow')
        gids=sorted({r['group_id'] for r in old})
        parents=b.ch(f'SELECT id,payload FROM {prior}.groups FINAL WHERE id IN {sql_list(gids)} FORMAT JSONEachRow') if gids else []
        inherited=[json.loads(r['payload']) for r in parents]
        if len(inherited)>1 or any(g['preferred_openalex_id']!=anchor for g in inherited) or any(len(seen[k])>1 for k in members):continue
        if not all(verified[k] for k in keys):raise RuntimeError('notice reached automatic confirmation')
        old_group=inherited[0] if inherited else None
        all_members=sorted(set(keys+(old_group['members'] if old_group else [])))
        gid=old_group['group_id'] if old_group else 'vg_'+digest(dict(release=m['release'],members=all_members))[:32]
        payload=dict(group_id=gid,members=all_members,preferred_openalex_id=anchor,state='confirmed',relation_type='preprint_publication',evidence=(old_group['evidence'] if old_group else [])+[r['evidence']|dict(method='calibrated_bibliographic',calibration=m['review_identity']) for r in members.values()])
        insert(b,db+'.groups',[dict(id=gid,payload=json.dumps(payload))])
        insert(b,db+'.members',[dict(id=k,group_id=gid) for k in all_members])
        insert(b,db+'.candidates',[dict(id=r['work_id']+':'+r['target_id'],payload=json.dumps(r|dict(state='confirmed',group_id=gid))) for r in members.values()])
    m['counts'].update({kind:int(b.ch(f'SELECT count() n FROM {db}.{kind} FINAL FORMAT JSONEachRow')[0]['n']) for kind in ('groups','members','reviews')});save(m)


def validate(m,b):
    parent=inputs(m);db=table(m)
    if m['counts'].get('features')!=m['expected_works'] or m['counts'].get('matched')!=m['selected_works']:raise RuntimeError('matching scope is incomplete')
    previous_coverage=parent['counts'].get('covered_works',0) if parent.get('schema',1)==2 else 0
    m['counts']['covered_works']=previous_coverage+m['selected_works']
    for name,n in (('sample',m['selected_works']),('coverage',m['counts']['covered_works'])):
        if int(b.ch(f'SELECT count() n FROM {db}.{name} FINAL FORMAT JSONEachRow')[0]['n'])!=n:raise RuntimeError('matching coverage reconciliation failed')
    # Validate every star and membership, rather than randomly sampling the new groups.
    bad=b.ch(f'''SELECT count() n FROM (SELECT id,group_id FROM {db}.members FINAL) m
      LEFT JOIN (SELECT id,arrayJoin(JSONExtract(payload,'members','Array(String)')) member FROM {db}.groups FINAL) g
      ON m.group_id=g.id AND m.id=g.member WHERE g.id='' FORMAT JSONEachRow''')[0]['n']
    if int(bad):raise RuntimeError('relationship membership reconciliation failed')
    reverse=b.ch(f"""SELECT count() n FROM (SELECT id,arrayJoin(JSONExtract(payload,'members','Array(String)')) member FROM {db}.groups FINAL) g
      LEFT JOIN (SELECT id,group_id FROM {db}.members FINAL) m ON g.member=m.id AND g.id=m.group_id WHERE m.id='' FORMAT JSONEachRow""")[0]['n']
    if int(reverse):raise RuntimeError('group payload has missing memberships')

    missing=b.ch(f'''SELECT count() n FROM (SELECT id,group_id FROM {legacy.table(parent)}.members FINAL) p
      LEFT JOIN (SELECT id,group_id FROM {db}.members FINAL) n ON p.id=n.id WHERE n.id='' OR p.group_id!=n.group_id FORMAT JSONEachRow''')[0]['n']
    if int(missing):raise RuntimeError('parent confirmed relationships were lost')
    for row in b.ch_stream(f'SELECT id,payload FROM {db}.groups FINAL ORDER BY id FORMAT JSONEachRow'):
        g=json.loads(row['payload'])
        if g['group_id']!=row['id'] or len(g['members'])<2 or len(set(g['members']))!=len(g['members']) or g['preferred_openalex_id'] not in g['members']:raise RuntimeError('invalid publication star')
    m['validation']=dict(passed=True,signature=m['signature'],at=time.time(),counts=m['counts'].copy(),parent_signature=parent['signature'],full_earth_scan=m['counts']['covered_works']==m['expected_works'],calibration_passed=m.get('calibration',{}).get('passed',False))
    atomic_json(path(m['release'])/'validation.json',m['validation']);save(m)


def run(release,until='review'):
    from .workspace import claim
    claim()
    if until not in STAGES:raise ValueError('unknown full-index matching stage')
    with lock(path(release)/'worker.lock'):
        m=read(release)
        if m['state']=='published':return m
        inputs(m);b=Backend(writer=True)
        try:
            for stage in STAGES[:STAGES.index(until)+1]:
                if stage in m['completed']:continue
                protect(m);m.update(state='running',stage=stage,error=None);save(m)
                globals()[stage](m,b);m['completed'].append(stage);m['state']='validated' if stage=='validate' else 'paused';save(m)
            return m
        except Exception as error:
            m.update(state='paused' if isinstance(error,InterruptedError) else 'failed',error=type(error).__name__+': '+str(error));save(m);raise
        finally:b.close()


def publish(release):
    from .workspace import claim
    from . import versions_reader
    claim()
    with lock(ROOT/'var/run/versions-publish.lock'),lock(path(release)/'worker.lock'):
        m=read(release)
        if m['state'] not in ('validated','published'):raise ValueError('validate relationships before publishing')
        current=versions_reader.published()
        if current['release']!=m['parent_release'] and current['release']!=release:raise ValueError('active parent changed; build a new cumulative release')
        if active()['generation']!=m['generation']:raise ValueError('metadata generation changed')
        b=Backend(writer=True)
        try:validate(m,b)
        finally:b.close()
        pointer=ROOT/'var/versions-active.json';previous=read_json(pointer)
        m.update(state='published',published=time.time());save(m)
        if previous['release']!=release:atomic_json(pointer,dict(release=release,signature=m['signature'],previous=previous))
        atomic_json(path(release)/'lineage.json',dict(parent_release=m['parent_release'],unchanged_group_ids='retained',new_members='directly verified against publication anchor'))
        return m
