"""Publish immutable disjoint blocks, then extend the published collection."""
import copy
import json
import math
import re
import time

from .config import ENTITIES, config, generation_path, read_generation, atomic_json
from .backend import Backend, sql_string
from . import search_scopes as scopes, search_versions as versions, search_projection


def prepare(g, build, scope):
    from .search_storage import require_qualification, check_runtime
    from .official import source_table
    from .resources import checkpoint
    proof = versions.require_metadata(g)
    catalog = scopes.load(g)
    qualification = require_qualification(g)
    if qualification.get("catalog_signature") != catalog["signature"]:
        raise RuntimeError("incremental search needs qualification for this scope catalog")
    path = versions.build_path(g, build)
    if (path / 'build.json').exists():
        value = versions.read(g, build)
        if value.get('state') == 'failed' or value.get('batch') != scope:
            raise RuntimeError('failed or different incremental build cannot be resumed')
        return value
    pending = scopes.next_batch(g)
    if not pending or pending[0] != scope:
        raise RuntimeError('build must follow the frozen priority queue')
    owner, entity, expected = pending
    b = Backend(writer=True)
    try:
        runtime = check_runtime(b)
        previous = versions.active(g)
        if previous and not previous.get('incremental'):
            raise RuntimeError('cannot append a legacy full search manifest')
        segments = copy.deepcopy(previous['segments']) if previous else []
        source = catalog['table'] + ' FINAL' if entity == 'works' else source_table(b, read_generation(g), entity)
        where = 'owner=' + sql_string(owner) if entity == 'works' else 'entity=' + sql_string(entity)
        low = ''
        sample = qualification['samples'][entity]
        while True:
            boundary = b.ch(f"SELECT max(id) high,count() n FROM (SELECT id FROM {source} WHERE {where} AND id>{sql_string(low)} ORDER BY id LIMIT {scopes.BLOCK_ROWS}) FORMAT JSONEachRow")[0]
            n = int(boundary['n'])
            if not n:
                break
            estimate = math.ceil(sample['bytes'] / max(sample['count'], 1) * n * 1.5)
            checkpoint(g, large=True, additional=estimate * 2, temporary=estimate // 2)
            shards = max(4 if entity == 'works' else 1, math.ceil(estimate / (32 * 1024**3)),
                         math.ceil(sample['lucene_docs'] / max(sample['count'], 1) * n / 1_000_000_000))
            segments.append(dict(entity=entity, owner=owner, low=low, high=boundary['high'], expected=n,
                                 index='oa-block-' + g.replace('_', '-') + '-' + build.replace('_', '-') + '-' + str(len(segments)),
                                 shards=shards, after=low, count=0, done=False))
            low = boundary['high']
        if sum(s['expected'] for s in segments if s['owner'] == owner) != expected:
            raise RuntimeError('incremental batch membership changed')
        value = dict(generation=g, build=build, state='building', incremental=True, created=time.time(),
                     batch=owner, batch_entity=entity, parent=previous['build'] if previous else None,
                     ready_scopes=list(previous.get('ready_scopes', [])) if previous else [],
                     data_path=runtime['data_path'], catalog_signature=catalog['signature'],
                     projection_signature=versions.signature(), metadata_signature=proof['signature'],
                     qualification=qualification['path'], segments=segments, indexes={}, entities={}, global_complete=False)
        refresh(value)
        versions.save(value)
        return value
    finally:
        b.close()


def refresh(manifest):
    for entity in ENTITIES:
        selected = [s for s in manifest['segments'] if s['entity'] == entity]
        if selected:
            manifest['indexes'][entity] = manifest.get('views',{}).get(entity,','.join(s['index'] for s in selected))
            manifest['entities'][entity] = dict(count=sum(s['count'] for s in selected),
                                               verified=sum(s.get('verified', 0) for s in selected))


def segment_manifest(manifest, segment, index=None):
    result = dict(manifest, indexes={segment['entity']: index or segment['index']},
                  selection=segment, entities={segment['entity']: segment})
    result.pop('incremental', None)
    return result


def seal_views(b,manifest):
    """An immutable alias per version bounds URI length without moving old cursors."""
    actions=[]
    views={}
    for entity in ENTITIES:
        names=[s['index'] for s in manifest['segments'] if s['entity']==entity]
        if not names:
            continue
        alias='oa-view-'+manifest['generation'].replace('_','-')+'-'+manifest['build'].replace('_','-')+'-'+entity
        reply=b.http.get(b.cfg['os_url']+'/_alias/'+alias)
        if reply.status_code==200:
            if set(reply.json())!=set(names):
                raise RuntimeError('immutable search view changed')
        elif reply.status_code==404:
            actions += [{'add':{'index':name,'alias':alias}} for name in names]
        else:
            reply.raise_for_status()
        views[entity]=alias
    if actions:
        result=b.os('POST','/_aliases',{'actions':actions})
        if not result.get('acknowledged'):
            raise RuntimeError('search view creation was not acknowledged')
    manifest['views']=views
    prior=versions.read(manifest['generation'],manifest['parent']) if manifest.get('parent') else {}
    manifest['scope_views']=dict(prior.get('scope_views',{}))
    if manifest['batch_entity']=='works' and manifest['batch'] in (scopes.EARTH,'unclassified'):
        owner=manifest['batch']
        names=[s['index'] for s in manifest['segments'] if s['entity']=='works' and s['owner']==owner]
        if owner==scopes.EARTH:
            manifest['scope_views'][owner]=views['works']
        else:
            alias=views['works']+'-unclassified'
            result=b.os('POST','/_aliases',{'actions':[{'add':{'index':n,'alias':alias}} for n in names]})
            if not result.get('acknowledged'):
                raise RuntimeError('scope view creation was not acknowledged')
            manifest['scope_views'][owner]=alias
    refresh(manifest)


def restore_prefix(b,g,manifest,segment):
    if segment['entity'] != 'works' or segment['owner'] != scopes.EARTH or segment.get('shards',4)!=4:
        return False
    from .search_storage import require_qualification
    from .config import ROOT
    receipt = require_qualification(g)
    source = next((x for x in receipt.get('blocks',[]) if x['low']==segment['low'] and x['high']==segment['high'] and x['count']==segment['expected']),None)
    if not source:
        return False
    snapshot = receipt['snapshot']
    b.os('PUT','/_snapshot/'+snapshot['repository'],{'type':'fs','settings':{'location':str(ROOT / 'var/backups/opensearch/qualification')}})
    b.os('POST','/_snapshot/'+snapshot['repository']+'/'+snapshot['name']+'/_restore?wait_for_completion=true',
         {'indices':source['index'],'include_global_state':False,'include_aliases':False,
          'rename_pattern':'^'+re.escape(source['index'])+'$','rename_replacement':segment['index']})
    if b.os('GET','/'+segment['index']+'/_count')['count'] != segment['expected']:
        raise RuntimeError('restored qualification prefix count mismatch')
    return True


def build(g, build_id, scope):
    from .ingest import lock
    from .resources import checkpoint
    with lock(generation_path(g) / 'search-controller.lock'):
        manifest = prepare(g, build_id, scope)
        if manifest['state'] in ('sealed', 'validated'):
            return manifest
        b = Backend(writer=True)
        try:
            for segment in manifest['segments']:
                if segment.get('done'):
                    continue
                entity, name = segment['entity'], segment['index']
                one = segment_manifest(manifest, segment)
                with versions.pinned(g, one):
                    head = b.http.head(b.cfg['os_url'] + '/' + name)
                    if head.status_code == 404:
                        if segment['count']:
                            raise RuntimeError('acknowledged block index is missing')
                        restored = restore_prefix(b,g,manifest,segment)
                        if restored:
                            segment.update(count=segment['expected'],after=segment['high'])
                        else:
                            mapping = search_projection.mapping(segment['shards'])
                            mapping['settings']['refresh_interval'] = '-1'
                            b.os('PUT', '/' + name, mapping)
                    else:
                        head.raise_for_status()
                    settings = b.os('GET', '/' + name + '/_settings')[name]['settings']['index']
                    if settings['uuid'] != segment.get('uuid', settings['uuid']):
                        raise RuntimeError('incremental index identity changed')
                    segment['uuid'] = settings['uuid']
                    while segment['count'] < segment['expected']:
                        yield_to_queries(g)
                        checkpoint(g)
                        started = time.monotonic()
                        rows = scopes.source_rows(b, g, entity, segment['after'], selection_value=segment)
                        read_seconds = time.monotonic() - started
                        if not rows:
                            raise RuntimeError('incremental source ended prematurely')
                        versions.write_documents(b, g, one, entity, rows)
                        segment.update(after=rows[-1]['id'], count=segment['count'] + len(rows))
                        timing = manifest.setdefault('timings', {})
                        timing['source_seconds'] = timing.get('source_seconds', 0) + read_seconds
                        timing['write_verify_seconds'] = timing.get('write_verify_seconds', 0) + time.monotonic() - started - read_seconds
                        refresh(manifest)
                        if segment["count"] % 10000 == 0 or segment["count"] >= segment["expected"]:
                            versions.save(manifest)
                    segment['verified'] = versions.verify(b, g, one, entity)
                    if segment['verified'] != segment['expected']:
                        raise RuntimeError('block membership count mismatch')
                    b.os('PUT', '/' + name + '/_settings', {'index.blocks.write': True, 'index.refresh_interval': '30s'})
                    segment['done'] = True
                    refresh(manifest)
                    versions.save(manifest)
            seal_views(b,manifest)
            manifest['state'] = 'sealed'
            if manifest['batch_entity']=='works':
                manifest['ready_scopes'].append(scope)
                if scope==scopes.EARTH:
                    manifest['ready_scopes'] += ['field_19','field_23']
            refresh(manifest)
            manifest['global_complete'] = all(manifest['entities'].get(e, {}).get('verified') == read_generation(g)['expected'][e]['records'] for e in ENTITIES)
            versions.save(manifest)
            return manifest
        except BaseException as error:
            from .resources import Paused
            manifest.update(state='building' if isinstance(error, Paused) else 'failed', error=repr(error))
            versions.save(manifest)
            raise
        finally:
            b.close()


_last_probe = {}


def yield_to_queries(g):
    """Pause new work at batch boundaries while the published search is slow."""
    import httpx
    from .config import config
    from .resources import checkpoint
    if time.monotonic()-_last_probe.get(g,0)<30:
        return
    current = versions.active(g)
    if not current or scopes.EARTH not in current.get('ready_scopes',[]):
        return
    path = generation_path(g)/'search-background.json'
    healthy = 0
    paused = False
    c = config()
    with httpx.Client(base_url='http://127.0.0.1:'+str(c['port']),headers={'X-API-Key':c['api_keys'][0]},timeout=c['query_timeout']+5,trust_env=False) as client:
        while True:
            checkpoint(g)
            started = time.monotonic()
            try:
                reply = client.get('/v1/works',params=dict(search='climate change',search_scope=scopes.EARTH,per_page=25,select='id,doi,title'))
                seconds = time.monotonic()-started
                good = reply.status_code==200
            except httpx.HTTPError:
                seconds,good = time.monotonic()-started,False
            _last_probe[g] = time.monotonic()
            if not paused and good and seconds<=2:
                return
            paused = True
            healthy = healthy+1 if good and seconds<=1.5 else 0
            atomic_json(path,dict(state='resuming' if healthy>=5 else 'paused_for_queries',probe_seconds=seconds,healthy_probes=healthy,updated=time.time()))
            if healthy>=5:
                return
            time.sleep(30)


def pipeline(g):
    from . import manage,recovery
    from .search_storage import require_qualification,Qualification
    from .ingest import lock
    from .resources import Paused
    with lock(generation_path(g) / 'search-pipeline.lock'):
        state_path = generation_path(g) / 'search-pipeline.json'
        state = dict(state='running',stage='catalog',started=time.time())
        atomic_json(state_path,state)
        try:
            catalog = scopes.build_catalog(g)
            state.update(catalog_signature=catalog['signature'],stage='qualification',updated=time.time())
            atomic_json(state_path,state)
            recovery.require_evidence(g,'recovery-tests')
            try:
                require_qualification(g)
            except RuntimeError:
                Qualification(g,'earth_v1').run()
            manage.start('opensearch')
            current = versions.active(g)
            if current and current.get('batch') == scopes.EARTH:
                observation = versions.build_path(g,current['build'])/'observation.json'
                if not observation.exists() or json.loads(observation.read_text()).get('state')!='complete':
                    versions.observe(g,current['build'])
            while pending := scopes.next_batch(g):
                scope = pending[0]
                build_id = 'priority_' + scope
                state.update(stage='building',batch=scope,build=build_id,updated=time.time())
                atomic_json(state_path,state)
                build(g,build_id,scope)
                state.update(stage='validation',updated=time.time());atomic_json(state_path,state)
                versions.validate(g,build_id)
                versions.publish(g,build_id)
                if scope == scopes.EARTH:
                    state.update(stage='observation',updated=time.time());atomic_json(state_path,state)
                    versions.observe(g,build_id)
            current = versions.active(g)
            if current:
                versions.observe(g,current['build'])
            state.update(state='complete',stage='complete',completed=time.time())
        except BaseException as error:
            state.update(state='paused' if isinstance(error,Paused) else 'failed',error=repr(error),updated=time.time())
            raise
        finally:
            atomic_json(state_path,state)
