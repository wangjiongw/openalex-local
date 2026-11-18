"""Verified read-only historical producer compatibility and checkpoint migration."""
import json
from pathlib import Path
from .semantics import digest
from .earth_control import read_json


def historical_read(release, unpublished=False):
    from . import versions_full as f
    m=read_json(f.path(release)/'release.json')
    known=read_json(Path(__file__).with_name('resources')/'version-implementation-compatibility.json')
    entry=known.get(m.get('signature'))
    if not entry or digest(entry['files'])!=m['signature'] or m.get('schema')!=entry['schema'] or m.get('release')!=release:
        raise RuntimeError('unsupported historical relationship implementation')
    if not unpublished:
        proof=read_json(f.path(release)/'validation.json')
        if m.get('state')!='published' or not proof.get('passed') or proof.get('signature')!=m['signature'] or proof!=m.get('validation'):
            raise RuntimeError('historical relationship validation changed')
    return m


def table_digest(b,query,columns):
    import hashlib
    import orjson
    h=hashlib.sha256();n=0
    for row in b.ch_stream(f'SELECT {columns} FROM {query} ORDER BY id FORMAT JSONEachRow'):
        h.update(orjson.dumps(row,option=orjson.OPT_SORT_KEYS)+b'\n');n+=1
    return dict(rows=n,sha256=h.hexdigest())


def migrate(source,target):
    import shutil
    from . import versions_full as f
    from .backend import Backend
    from .workspace import claim
    claim()
    if source==target:raise ValueError('migration requires a new relationship release')
    with f.lock(f.path(source)/'worker.lock'),f.lock(f.path(target)/'worker.lock'):
        sourcefile=f.path(source)/'release.json'
        raw=read_json(sourcefile)
        old=f.read(source) if raw['signature']==f.signature() else historical_read(source,unpublished=True)
        m=f.read(target)
        if old['state']!='paused' or not {'features','sample'}<=set(old['completed']):
            raise ValueError('pause the source after complete feature and sample stages')
        if m['state'] not in ('planned','paused') or 'candidates' in m['completed']:
            raise ValueError('migration target must not have completed candidate processing')
        keys=('generation','collection','search_build','metadata_proof','parent_signature','feature_signature',
              'candidate_signature','confirmation_signature','seed','stratum','batch_size','pilot_size','files','segments')
        if any(old[k]!=m[k] for k in keys):raise ValueError('migration semantic inputs differ')
        f.inputs(m)
        if f.file_hash(f.feature_root(m)/'progress.json')!=old['feature_receipt']:
            raise RuntimeError('feature receipt changed before migration')
        cursor=f.read_json(f.path(source)/'candidates-progress.json')
        receiptfile=f.path(target)/'migration.json'
        identity=digest(dict(source=source,target=target,source_manifest=f.file_hash(sourcefile),cursor=cursor,signature=m['signature']))
        receipt=f.read_json(receiptfile) if receiptfile.exists() else dict(identity=identity,source_release=source,target_release=target,tables={},complete=False)
        if receipt['identity']!=identity:raise RuntimeError('migration source checkpoint changed')
        b=f.Backend(writer=True)
        try:
            f.tables(m,b)
            b.ch(f'CREATE TABLE IF NOT EXISTS {f.table(m)}.sample_ids (id String) ENGINE=ReplacingMergeTree ORDER BY id')
            db=f.table(old);sample=f'(SELECT id FROM {db}.sample FINAL)'
            tail=f.sql_string(cursor['after'])
            filters={'sample_ids':'','sample':'',
                     'coverage':f' WHERE id NOT IN {sample} OR id<={tail}',
                     'candidates':f" WHERE splitByChar(':',id)[1] NOT IN {sample} OR splitByChar(':',id)[1]<={tail}"}
            for name,where in filters.items():
                f.protect(m);columns='id' if name=='sample_ids' else 'id,payload'
                source_query=f'{db}.{name} FINAL{where}';target_query=f'{f.table(m)}.{name} FINAL'
                if name in receipt['tables']:
                    if table_digest(b,target_query,columns)!=receipt['tables'][name]:raise RuntimeError('acknowledged migration table changed')
                    continue
                expected=table_digest(b,source_query,columns)
                b.ch(f'INSERT INTO {f.table(m)}.{name} SELECT {columns} FROM {source_query}')
                actual=table_digest(b,target_query,columns)
                if actual!=expected:raise RuntimeError('migration content reconciliation failed: '+name)
                receipt['tables'][name]=actual;f.atomic_json(receiptfile,receipt)
            n=int(b.ch(f'SELECT count() n FROM {f.table(m)}.candidates FINAL WHERE splitByChar(\':\',id)[1] IN (SELECT id FROM {f.table(m)}.sample FINAL) FORMAT JSONEachRow')[0]['n'])
            if n!=cursor['pairs']:raise RuntimeError('migration candidate checkpoint differs')
            parent=f.inputs(m)
            from .version_progress import cache_summary
            if parent.get('schema',1)==2:cache_summary(parent,b)
            coverage=parent['counts'].get('covered_works',0)+cursor['processed']
            if receipt['tables']['coverage']['rows']!=coverage or any(receipt['tables'][name]['rows']!=old['selected_works'] for name in ('sample','sample_ids')) or not 0<=cursor['processed']<=old['selected_works']:
                raise RuntimeError('migration work coverage differs')
            for name in ('sample-selection.json','sample-progress.json'):
                shutil.copyfile(f.path(source)/name,f.path(target)/name)
            f.atomic_json(f.path(target)/'candidates-progress.json',cursor)
            m.update(completed=['features','sample'],state='paused',stage='candidates',error=None,
                     selected_works=old['selected_works'],feature_receipt=old['feature_receipt'],
                     counts=dict(features=old['counts']['features'],sample=old['selected_works'],sample_ids=old['selected_works'],
                                 matched=cursor['processed'],pairs=cursor['pairs'],overflow=cursor['overflow']),
                     reuse_from=source)
            f.save(m);receipt['complete']=True;f.atomic_json(receiptfile,receipt)
            return m
        finally:b.close()
