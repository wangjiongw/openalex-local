"""Disjoint, independently recoverable catalog blocks over frozen author members."""
import hashlib
import signal
import math
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from multiprocessing import get_context

from .backend import Backend, sql_string, sql_list
from .config import config, atomic_json, read_generation
from .earth_control import read_json
from .ingest import lock
from .manticore_build import MAX_BLOCK, save, seal, restore, tables, write_batch
from .manticore_engine import Client
from .search_versions import build_path


def prepare(b, c, m):
    target = build_path(m['generation'],m['build'])
    file = target/'catalog-blocks/plan.json'
    if file.exists():
        plan = read_json(file)
        if any(plan[key] != m[key] for key in ('metadata_signature','parent_manifest_sha256','expected_authors')):
            raise RuntimeError('catalog block inputs changed')
        return plan
    progress = m['catalog_progress']['authors']
    base = [s for s in m['segments'] if s['entity']=='authors' and s['count']]
    if sum(s['count'] for s in base) != progress['selected']-progress['missing']:
        raise RuntimeError('author prefix checkpoint mismatch')
    for segment in base:
        if not segment.get('sealed'):
            if segment['index'] not in tables(c):
                from .catalogs import replay
                replay(b,c,m,segment)
            seal(c,target,segment)
    # One immutable sorted member part avoids repeatedly merging duplicates on reads.
    b.ch(f"OPTIMIZE TABLE {m['author_membership_table']} FINAL")
    remaining = m['expected_authors']-progress['selected']
    if remaining < 0:
        raise RuntimeError('author prefix exceeds frozen membership')
    plan = dict(schema=1,base=progress.copy(),base_segments=base,blocks=[],
                metadata_signature=m['metadata_signature'],parent_manifest_sha256=m['parent_manifest_sha256'],expected_authors=m['expected_authors'])
    desired=max(1,min(int(config().get('catalog_workers',8)),int(config()['cpus']),max(1,int(config()['memory_gib'])//4)))
    block_rows=min(MAX_BLOCK,max(1,math.ceil(remaining/desired)))
    plan['block_rows']=block_rows
    lower = progress['after']
    for offset in range(0,remaining,block_rows):
        count = min(block_rows,remaining-offset)
        bound = b.ch(f"SELECT id FROM {m['author_membership_table']} FINAL WHERE id>{sql_string(progress['after'])} ORDER BY id LIMIT 1 OFFSET {offset+count-1} FORMAT JSONEachRow")
        if len(bound)!=1:
            raise RuntimeError('author block boundary missing')
        upper = bound[0]['id']
        index = 'oa_mc_' + hashlib.sha256((m['generation']+m['build']).encode()).hexdigest()[:12] + f'_authors_{len(plan["blocks"]):03d}'
        segment = dict(index=index,entity='authors',group='',count=0,after='',batches=[],physical_layout=m.get('physical_layout','legacy'),sealed=False)
        name = f'catalog-blocks/{len(plan["blocks"]):03d}.json'
        state = dict(schema=1,generation=m['generation'],build=m['build'],member_table=m['author_membership_table'],
                     metadata_signature=m['metadata_signature'],lower=lower,upper=upper,expected=count,
                     after=lower,selected=0,missing=0,missing_sample=[],segment=segment,state='building')
        atomic_json(target/name,state)
        plan['blocks'].append(name)
        lower=upper
    atomic_json(file,plan)
    return plan


def initialize(stop):
    global BACKEND, SEARCH, STOP
    signal.signal(signal.SIGTERM,signal.SIG_DFL)
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    BACKEND, SEARCH, STOP = Backend(writer=True),Client(timeout=600),stop


def read_batch(state,after,size):
    from .official import source_table
    ids = [r['id'] for r in BACKEND.ch(f"SELECT id FROM {state['member_table']} FINAL WHERE id>{sql_string(after)} AND id<={sql_string(state['upper'])} ORDER BY id LIMIT {size} FORMAT JSONEachRow")]
    if not ids:
        return [],[]
    source=source_table(BACKEND,read_generation(state['generation']),'authors')
    rows=BACKEND.ch(f"SELECT id,raw,version,digest FROM {source} WHERE entity='authors' AND id IN {sql_list(ids)} ORDER BY id FORMAT JSONEachRow")
    return ids,rows


def run_block(file):
    with lock(file.with_suffix('.lock')):
        state=read_json(file)
        segment=state['segment']
        target=build_path(state['generation'],state['build'])
        from .search_versions import require_metadata
        if require_metadata(state['generation'])['signature']!=state['metadata_signature']:
            raise RuntimeError('catalog metadata changed')
        present=segment['index'] in tables(SEARCH)
        if segment['count'] and not present:
            if segment.get('sealed'):
                restore(SEARCH,target,segment)
            else:
                from .catalogs import replay
                replay(BACKEND,SEARCH,read_json(target/'build.json'),segment)
            present=True
        if state['state']=='complete':
            return state
        try:
            size=max(1,min(int(config().get('catalog_batch_rows',10000)),10000))
            with ThreadPoolExecutor(1) as reader:
                future=reader.submit(read_batch,state,state['after'],min(size,state['expected']-state['selected'])) if state['selected']<state['expected'] else None
                while state['selected']<state['expected'] and not STOP.is_set():
                    if shutil.disk_usage(target).free < config().get('min_free_gib',1)*1024**3:
                        raise RuntimeError('catalog disk reserve reached')
                    started=time.monotonic()
                    ids,rows=future.result()
                    if not ids or len(ids)>state['expected']-state['selected']:
                        raise RuntimeError('catalog block membership ended early or overflowed')
                    source_wait=time.monotonic()-started
                    remaining=state['expected']-state['selected']-len(ids)
                    future=reader.submit(read_batch,state,ids[-1],min(size,remaining)) if remaining else None
                    if rows:
                        if not present:
                            from .manticore_formats import create as native_create
                            from .manticore_engine import create as legacy_create
                            (native_create if segment['physical_layout']=='native_v2' else legacy_create)(SEARCH,segment['index'])
                            present=True
                        timings=write_batch(SEARCH,BACKEND,target,segment,rows,bulk_workers=2 if config()['cpus']>=2 else 1)
                    else:
                        timings={}
                    missing=sorted(set(ids)-{r['id'] for r in rows})
                    state.update(after=ids[-1],selected=state['selected']+len(ids),missing=state['missing']+len(missing),updated=time.time(),
                                 timings=timings|{'source_wait_seconds':source_wait})
                    state['missing_sample']=(state['missing_sample']+missing)[:100]
                    atomic_json(file,state)
            if state['selected']==state['expected']:
                state['state']='indexed'
            else:
                state['state']='paused'
            atomic_json(file,state)
            return state
        except Exception as error:
            state.update(state='failed',error=type(error).__name__+': '+str(error),updated=time.time())
            atomic_json(file,state)
            raise


def merge(m,plan,states):
    base=plan['base']
    m['segments']=[s for s in m['segments'] if s['entity']!='authors']+plan['base_segments']+[s['segment'] for s in states if s['segment']['count']]
    progress=dict(after=base['after'],selected=base['selected']+sum(s['selected'] for s in states),
                  missing=base['missing']+sum(s['missing'] for s in states),
                  missing_sample=(base['missing_sample']+[key for s in states for key in s['missing_sample']])[:100],
                  done=all(s['state']=='complete' for s in states))
    if progress['done'] and states:
        progress['after']=states[-1]['after']
    m['catalog_progress']['authors']=progress
    m['entities']['authors']=dict(count=sum(s['count'] for s in m['segments'] if s['entity']=='authors'))
    if progress['done'] and (progress['selected']!=m['expected_authors'] or m['entities']['authors']['count']+progress['missing']!=progress['selected']):
        raise RuntimeError('catalog block coverage mismatch')
    save(m)
    return progress


def build_authors(b,c,m,stop):
    target=build_path(m['generation'],m['build'])
    m.update(state='building',error=None)
    save(m)
    plan=prepare(b,c,m)
    m['author_block_plan']='catalog-blocks/plan.json'
    m.update(state='building',error=None)
    states=[read_json(target/name) for name in plan['blocks']]
    progress=merge(m,plan,states)
    if progress['done']:
        return
    workers=max(1,min(int(config().get('catalog_workers',8)),int(config()['cpus']),max(1,int(config()['memory_gib'])//4),len(states)))
    context=get_context('fork')
    shared_stop=context.Event()
    initial=progress['selected'];started=time.monotonic()
    try:
        with ProcessPoolExecutor(max_workers=workers,mp_context=context,initializer=initialize,initargs=(shared_stop,)) as pool:
            pending={pool.submit(run_block,target/name):target/name for name,state in zip(plan['blocks'],states) if state['state']!='complete'}
            try:
                while pending:
                    done,_=wait(pending,timeout=5,return_when=FIRST_COMPLETED)
                    for future in done:
                        file=pending.pop(future)
                        state=future.result()
                        if state['state']=='indexed':
                            if state['segment']['count']:
                                seal(c,target,state['segment'])
                            state['state']='complete'
                            atomic_json(file,state)
                    states=[read_json(target/name) for name in plan['blocks']]
                    progress=merge(m,plan,states)
                    speed=(progress['selected']-initial)/max(time.monotonic()-started,.001)
                    atomic_json(target/'progress.json',dict(state='building_catalogs',entity='authors',progress=progress,
                        expected=m['expected_authors'],indexed=m['entities']['authors']['count'],workers=workers,
                        records_per_second=speed,eta_seconds=(m['expected_authors']-progress['selected'])/max(speed,.001),
                        blocks=[dict(index=s['segment']['index'],state=s['state'],selected=s['selected'],timings=s.get('timings')) for s in states],updated=time.time()))
                    if stop.is_set():
                        shared_stop.set()
                        raise RuntimeError('catalog paused by signal; resume with the same build')
            except BaseException:
                shared_stop.set()
                raise
    finally:
        states=[read_json(target/name) for name in plan['blocks']]
        merge(m,plan,states)

    if not m['catalog_progress']['authors']['done']:
        raise RuntimeError('author blocks did not complete')
