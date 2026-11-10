"""Bind an isolated API workspace to existing frozen artifacts, without changing them."""
import json
import secrets
from pathlib import Path

from .config import ROOT, atomic_json, generation_name, init
from .manticore_build import file_hash
from .compatibility import supports
from .ingest import lock


def prepare(source, generation, build, port=8088):
    source = source.expanduser().resolve()
    generation_name(generation)
    generation_name(build)
    if source == ROOT or not 1 <= port <= 65535:
        raise ValueError('adoption requires a distinct workspace and a valid API port')
    directory = source / 'var/generations' / generation / 'searches' / build
    manifest_path = directory / 'build.json'
    manifest = json.loads(manifest_path.read_text())
    published = json.loads((directory.parent.parent / 'search-active.json').read_text())
    meta = json.loads((directory.parent.parent / 'generation.json').read_text())
    if (published.get('build') != build or manifest.get('state') != 'validated'
            or not meta.get('metadata_frozen') or manifest['generation'] != generation or manifest['build'] != build
            or not supports(manifest)):
        raise ValueError('adopt only a published frozen build with an explicitly supported projection')
    binding = dict(workspace=str(source), generation=generation, build=build,
                   source_manifest_sha256=file_hash(manifest_path))
    with lock(ROOT / 'var/run/adoption.lock'):
        if (ROOT / 'var/active.json').exists():
            raise ValueError('adoption target already has an active dataset; use a new workspace')
        init()
        cfg = json.loads((source / 'config/local.json').read_text())
        cfg.update(adopted_from=binding, host='127.0.0.1', port=port,
                   api_workers=1, api_keys=[secrets.token_urlsafe(32)], cursor_secret=secrets.token_hex(32))
        cfg.pop('manticore_runtime', None)
        atomic_json(ROOT / 'config/local.json', cfg)
        for name in ['var/engines', 'var/generations']:
            path = ROOT / name
            if path.exists() and not path.is_symlink():
                if any(path.iterdir()):
                    raise ValueError('adoption target has existing artifacts: ' + name)
                path.rmdir()
            path.symlink_to(source / name, target_is_directory=True)
        (ROOT / 'exports').mkdir(exist_ok=True)
        if (source / 'exports/earth_collections').exists():
            (ROOT / 'exports/earth_collections').symlink_to(source / 'exports/earth_collections', target_is_directory=True)
        atomic_json(ROOT / 'var/active.json', dict(generation=generation, read_only_adoption=True))
        atomic_json(ROOT / 'adoption.json', binding | dict(state='bound', validated=False,
                    note='Binding is not deployment acceptance; run verify and fresh-engine restore.'))
    return dict(state='bound', validated=False, generation=generation, build=build,
                workspace=str(ROOT), api_key_file=str(ROOT / 'config/local.json'))
