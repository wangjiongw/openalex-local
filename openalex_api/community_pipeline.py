"""Current ClickHouse/Manticore pipeline, independent of incident-era reports."""
import json
import socket
import time
from pathlib import Path

from .config import ROOT, RESOURCE_ROOT, atomic_json, config, generation_path, read_generation
from .ingest import lock
from .proofs import digest

STAGES = ('metadata', 'collections', 'index', 'validated', 'published')


def path(g):
    return generation_path(g) / 'pipeline.json'


def plan(g, profile='earth_sciences'):
    from .snapshot import inventory
    if profile != 'earth_sciences':
        raise ValueError('only the earth_sciences indexing profile is supported')
    inventory_value = inventory(config()['snapshot'])
    if any(v['missing'] or v['size_mismatch'] for v in inventory_value['entities'].values()):
        raise ValueError('snapshot is incomplete on disk; finish its download first')
    if not inventory_value['files']:
        raise ValueError('snapshot has no data files')
    rules = {n: json.loads((RESOURCE_ROOT / n).read_text()) for n in
             ['earth-sciences-rules.json', 'earth-journal-scopes.json']}
    return dict(schema=1, generation=g, profile=profile, stages=list(STAGES),
                snapshot=inventory_value['snapshot'], manifest_sha256=inventory_value['manifest_sha256'],
                snapshot_complete=inventory_value['complete'], rule_signature=digest(rules),
                source_records=sum(v['records'] for v in inventory_value['entities'].values()),
                build=g[:40] + '_earth', engine='manticore')


def ensure_service(name):
    from . import manage
    state = manage.identity(manage.state_path(name))
    if not state and manage.health(name):
        raise RuntimeError('endpoint is already served by an unowned process: ' + name)
    if name in ('clickhouse', 'manticore'):
        from .workspace import claim
        claim()
    return manage.start(name)


def metadata(g):
    from . import ingest, recovery
    meta = read_generation(g)
    if meta.get('metadata_frozen'):
        recovery.require_evidence(g, 'metadata-complete')
        return
    ingest.run_import(g)
    recovery.metadata_complete(g)
    meta = read_generation(g)
    meta.update(mutable=False, validated=True, state='validated', metadata_only=True,
                complete=meta.get('snapshot_complete', False))
    atomic_json(generation_path(g) / 'generation.json', meta)
    ingest.metadata_release(g)


def run(g, profile='earth_sciences', until='validated'):
    from . import ingest, earth_catalog, earth_collections
    from .manticore_build import build
    if config().get("adopted_from"):
        raise ValueError("adopted workspaces are read-only; use a separate workspace for building")
    expected = plan(g, profile)
    if until not in STAGES:
        raise ValueError('unknown pipeline stage')
    if not generation_path(g).exists():
        ensure_service('clickhouse')
        ingest.create_generation(g, mode='full', metadata_only=True)
    if read_generation(g).get('pipeline_version') != 3:
        raise ValueError('existing incident-era generation requires separate read-only adoption validation')
    with lock(generation_path(g) / 'pipeline.lock'):
        state = json.loads(path(g).read_text()) if path(g).exists() else dict(plan=expected, completed=[], state='created')
        if state['plan'] != expected:
            raise ValueError('pipeline input/profile changed; create a new generation')
        ensure_service('clickhouse')
        try:
            for stage in STAGES[:STAGES.index(until) + 1]:
                if stage in state['completed']:
                    continue
                state.update(state='running', phase=stage, updated=time.time(), error=None)
                atomic_json(path(g), state)
                if stage == 'metadata':
                    metadata(g)
                elif stage == 'collections':
                    earth_catalog.build_catalog(g)
                    earth_collections.run(g)
                    earth_collections.verify(g, 'earth_scope_v1')
                elif stage == 'index':
                    ensure_service('manticore')
                    build(g, expected['build'], list(earth_collections.GROUPS), layout='native_v2')
                elif stage == 'validated':
                    from .acceptance import qualify
                    qualify(g, expected['build'])
                elif stage == 'published':
                    publish(g, expected['build'])
                state['completed'].append(stage)
                state.update(state='complete', updated=time.time())
                atomic_json(path(g), state)
        except BaseException as error:
            state.update(state='failed', error=type(error).__name__ + ': ' + str(error), updated=time.time())
            atomic_json(path(g), state)
            raise
    return state


def publish(g, build):
    from . import ingest, manage
    from .manticore_qualification import publish as publish_search
    from .search_versions import build_path
    pointer = ROOT / 'var/active.json'
    with lock(ROOT / 'var/run/community-publish.lock'):
        previous = pointer.read_bytes() if pointer.exists() else None
        # Existing qualification checks the current implementation and all sealed dependencies.
        receipt = json.loads((build_path(g, build) / 'qualification/acceptance.json').read_text())
        if not receipt.get('passed') or not receipt.get('performance_passed'):
            raise RuntimeError('publication requires completed functional/load/restore qualification')
        ensure_service('manticore')
        ensure_service('api')
        try:
            ingest.publish(g, metadata_only=True)
            publish_search(g, build)
        except BaseException:
            if previous is None:
                pointer.unlink(missing_ok=True)
            else:
                atomic_json(pointer, json.loads(previous))
            raise
    return dict(published=True, generation=g, build=build,
                base_url='http://' + config()['host'] + ':' + str(config()['port']))


def search_action(action, g, build):
    from .manticore_build import build as create_index
    from .earth_collections import GROUPS
    if action == 'search-build':
        ensure_service('manticore')
        return create_index(g, build, list(GROUPS), layout='native_v2')
    if action == 'search-validate':
        from .acceptance import qualify
        return qualify(g, build)
    return publish(g, build)
