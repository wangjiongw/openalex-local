"""Manifest adapters and resumable, anonymous public-snapshot downloads."""
import gzip
import hashlib
import json
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import httpx

from .config import ENTITIES, ROOT, atomic_json


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 * 1024**2), b''):
            h.update(chunk)
    return h.hexdigest()


def relative_object(url, entity):
    parsed = urlparse(url)
    if parsed.scheme not in ('s3', 'https') or parsed.query or parsed.fragment:
        raise ValueError('snapshot objects require unsigned s3/https URLs')
    parts = PurePosixPath(unquote(parsed.path)).parts
    if '..' in parts or '\\' in unquote(parsed.path):
        raise ValueError('snapshot object path escapes the entity')
    try:
        index = parts.index(entity)
    except ValueError:
        raise ValueError('snapshot object does not belong to entity') from None
    suffix = PurePosixPath(*parts[index:])
    if len(suffix.parts) < 2 or suffix.suffix != '.gz':
        raise ValueError('only gzipped JSONL part files can be imported')
    return suffix.as_posix()


def _entity_files(value):
    files = value.get('files', value.get('entries'))
    if not isinstance(files, list):
        raise ValueError('manifest requires a files/entries array')
    return files


def inventory(snapshot):
    root = Path(snapshot).expanduser().resolve()
    candidates = [root / 'data/jsonl/manifest.json', root / 'manifest.json']
    manifest_path = next((p for p in candidates if p.is_file()), None)
    sources = {}
    digest = hashlib.sha256()
    release = None
    subset = False
    if manifest_path:
        raw = manifest_path.read_bytes()
        combined = json.loads(raw)
        if combined.get('format') != 'jsonl':
            raise ValueError('Parquet import is not supported; choose the JSONL snapshot')
        release = combined.get('date')
        subset = bool(combined.get('snapshot_subset'))
        values = combined.get('entities')
        if not isinstance(values, list):
            values = [combined] if combined.get('entity') else None
        if values is None:
            raise ValueError('combined manifest requires an entities array')
        if len({v['entity'] for v in values}) != len(values):
            raise ValueError('duplicate entity manifests')
        sources = {v['entity']: (v, manifest_path.parent) for v in values}
        digest.update(raw)
    else:
        if (root / 'data/parquet/manifest.json').exists():
            raise ValueError('Parquet import is not supported; choose the JSONL snapshot')
        for entity in ENTITIES:
            path = root / 'data' / entity / 'manifest'
            if path.is_file():
                raw = path.read_bytes()
                sources[entity] = (json.loads(raw), root / 'data')
                # Preserve the legacy full-snapshot fingerprint algorithm.
                digest.update(entity.encode() + raw)
        if not sources:
            raise ValueError('no supported OpenAlex JSONL manifest found')
    if set(sources) - set(ENTITIES):
        raise ValueError('unsupported snapshot entities: ' + ', '.join(sorted(set(sources) - set(ENTITIES))))
    report, items, seen = {}, [], set()
    for entity in ENTITIES:
        value, base = sources.get(entity, ({'files': []}, root / 'data'))
        files = _entity_files(value)
        n = total = missing = mismatch = 0
        for entry in files:
            relative = relative_object(entry['url'], entity)
            if relative in seen:
                raise ValueError('duplicate snapshot shard: ' + relative)
            seen.add(relative)
            path = (base / relative).resolve()
            if not path.is_relative_to((base / entity).resolve()):
                raise ValueError('snapshot path escapes entity directory')
            size, count = entry['meta']['content_length'], entry['meta']['record_count']
            if type(size) is not int or type(count) is not int or min(size, count) < 0:
                raise ValueError('invalid shard size or record count')
            present = path.is_file()
            actual = path.stat().st_size if present else None
            missing += not present
            mismatch += present and actual != size
            n += count
            total += size
            items.append(dict(entity=entity, path=str(path), relative=relative,
                              expected=count, bytes=size, present=present, size_ok=actual == size))
        if value.get('record_count', n) != n or value.get('content_length', total) != total:
            raise ValueError('entity manifest totals differ from its files: ' + entity)
        report[entity] = dict(shards=len(files), records=n, compressed_bytes=total,
                              missing=missing, size_mismatch=mismatch, available=entity in sources)
    items.sort(key=lambda r: (r['entity'], r['relative']))
    for ordinal, item in enumerate(items, 1):
        item['ordinal'] = ordinal
    return dict(snapshot=str(root), manifest_sha256=digest.hexdigest(), entities=report,
                files=items, format='jsonl', release=release,
                complete=not subset and set(sources) == set(ENTITIES))


def _https(source):
    parsed = urlparse(source)
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError('use an unsigned public snapshot URL')
    if parsed.scheme == 's3':
        return 'https://' + parsed.netloc + '.s3.amazonaws.com' + parsed.path.rstrip('/')
    if parsed.scheme != 'https':
        raise ValueError('snapshot source must be s3:// or https://')
    return source.rstrip('/')


def remote_bytes(url):
    for attempt in range(4):
        try:
            response = httpx.get(url, follow_redirects=True, timeout=30, trust_env=False)
            response.raise_for_status()
            return response.content
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if isinstance(error, httpx.HTTPStatusError) and error.response.status_code < 500:
                raise
            if attempt == 3:
                raise
            time.sleep(.25 * 2**attempt)


def plan(source='s3://openalex', target=None, entities=None, max_shards=None):
    source = source.rstrip('/')
    if not source.endswith('/data/jsonl'):
        source += '/data/jsonl'
    url = _https(source) + '/manifest.json'
    raw = remote_bytes(url)
    combined = json.loads(raw)
    if combined.get('format') != 'jsonl' or not isinstance(combined.get('entities'), list):
        raise ValueError('download requires the official combined JSONL manifest')
    selected = set(entities or [v['entity'] for v in combined['entities']])
    known = {v['entity'] for v in combined['entities']}
    if not selected or selected - known or selected - set(ENTITIES):
        raise ValueError('unknown or empty entity selection')
    if max_shards is not None and (type(max_shards) is not int or max_shards < 1):
        raise ValueError('max-shards must be positive')
    target = Path(target or ROOT / 'snapshots' / str(combined['date'])).expanduser().resolve()
    chosen, objects, seen = [], [], set()
    for value in combined['entities']:
        entity = value['entity']
        if entity not in selected:
            continue
        files = _entity_files(value)
        files = files[:max_shards] if max_shards else files
        for entry in files:
            relative = relative_object(entry['url'], entity)
            if not entry['url'].startswith(source + '/') or relative in seen:
                raise ValueError('manifest object outside selected source or duplicate')
            seen.add(relative)
            objects.append(dict(url=entry['url'], relative=relative, **entry['meta']))
        chosen.append(dict(entity=entity, files=files,
                           record_count=sum(f['meta']['record_count'] for f in files),
                           content_length=sum(f['meta']['content_length'] for f in files)))
    local_manifest = dict(date=combined['date'], format='jsonl', entities=chosen,
                          snapshot_subset=selected != known or max_shards is not None)
    value = dict(schema=1, source=source, source_manifest=url,
                 source_manifest_sha256=sha(raw), source_manifest_raw=raw.decode(),
                 target=str(target), objects=objects, manifest=local_manifest,
                 compressed_bytes=sum(o['content_length'] for o in objects),
                 records=sum(o['record_count'] for o in objects),
                 complete=not local_manifest['snapshot_subset'], state='planned')
    path = target / 'download-plan.json'
    if path.exists() and json.loads(path.read_text()) != value:
        raise ValueError('target already has a different download plan; use another target')
    atomic_json(path, value)
    return {k: v for k, v in value.items() if k not in ('source_manifest_raw', 'objects', 'manifest')} | {'plan': str(path), 'files': len(objects)}


def download(plan_path, workers=4):
    if type(workers) is not int or not 1 <= workers <= 64:
        raise ValueError('download workers must be between 1 and 64')
    aws = shutil.which('aws')
    if not aws:
        raise RuntimeError('install AWS CLI v2 for anonymous snapshot downloads')
    value = json.loads(Path(plan_path).read_text())
    if sha(remote_bytes(value['source_manifest'])) != value['source_manifest_sha256']:
        raise RuntimeError('upstream release changed; create a new download plan')
    target = Path(value['target']).resolve()
    base = target / 'data/jsonl'
    def transfer(item):
        entity = item['relative'].split('/')[0]
        if relative_object(item['url'], entity) != item['relative'] or not item['url'].startswith(value['source'] + '/'):
            raise ValueError('invalid download-plan object')
        path = base / item['relative']
        path.parent.mkdir(parents=True, exist_ok=True)
        receipt = path.with_suffix('.receipt.json')
        if path.exists() and receipt.exists():
            proof = json.loads(receipt.read_text())
            if (proof.get('source_manifest_sha256') == value['source_manifest_sha256']
                    and proof.get('url') == item['url'] and path.stat().st_size == item['content_length']
                    and file_sha(path) == proof.get('sha256')):
                return
        temp = path.with_suffix('.partial')
        for attempt in range(4):
            result = subprocess.run([aws, 's3', 'cp', item['url'], str(temp), '--no-sign-request', '--only-show-errors'], capture_output=True)
            if result.returncode == 0 and temp.is_file() and temp.stat().st_size == item['content_length']:
                temp.replace(path)
                atomic_json(receipt, dict(url=item['url'], sha256=file_sha(path),
                                          source_manifest_sha256=value['source_manifest_sha256']))
                return
            if attempt == 3:
                raise RuntimeError('snapshot object transfer failed: ' + item['relative'])
            time.sleep(.5 * 2**attempt)
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(transfer, value['objects']))
    if sha(remote_bytes(value['source_manifest'])) != value['source_manifest_sha256']:
        raise RuntimeError('upstream release changed during transfer; snapshot not committed')
    # Preserve license/release notes and available auxiliary logs without applying them.
    extras = {}
    bucket = value['source'].split('/data/jsonl')[0]
    names = ['LICENSE.txt', 'RELEASE_NOTES.txt']
    for name in names:
        (target / name).write_bytes(remote_bytes(_https(bucket) + '/' + name))
    for entity in ([] if value['manifest']['snapshot_subset'] else [v['entity'] for v in value['manifest']['entities']]):
        for name in ['deleted_ids.csv.gz', 'merged_ids.csv.gz']:
            url = _https(value['source']) + '/' + entity + '/' + name
            try:
                path = base / entity / name
                path.parent.mkdir(parents=True, exist_ok=True)
                temp = path.with_suffix('.partial')
                with httpx.stream('GET', url, timeout=120, follow_redirects=True, trust_env=False) as response:
                    response.raise_for_status()
                    with temp.open('wb') as output:
                        for chunk in response.iter_bytes(8 * 1024**2):
                            output.write(chunk)
                temp.replace(path)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 404:
                    continue
                raise
            extras[entity + '/' + name] = dict(sha256=file_sha(path), applied=False)
    (target / 'source-manifest.json').write_text(value['source_manifest_raw'])
    atomic_json(base / 'manifest.json', value['manifest'])
    result = dict(state='downloaded', files=len(value['objects']), compressed_bytes=value['compressed_bytes'],
                  source_manifest_sha256=value['source_manifest_sha256'], auxiliary=extras,
                  note='transfer verified; gzip/JSON/content checks happen during import')
    atomic_json(target / 'download.json', result)
    return result


def verify(snapshot, deep=False):
    value = inventory(snapshot)
    errors = [k for k, v in value['entities'].items() if v['missing'] or v['size_mismatch']]
    if errors:
        raise ValueError('missing/wrong-size snapshot shards: ' + ', '.join(errors))
    checks = []
    if deep:
        for item in value['files']:
            count = 0
            h = hashlib.sha256()
            with gzip.open(item['path'], 'rb') as stream:
                for count, raw in enumerate(stream, 1):
                    if not isinstance(json.loads(raw), dict):
                        raise ValueError('snapshot records must be JSON objects')
                    h.update(raw)
            if count != item['expected']:
                raise ValueError('snapshot record count mismatch: ' + item['relative'])
            checks.append(dict(relative=item['relative'], rows=count, uncompressed_sha256=h.hexdigest()))
    return dict(passed=True, format='jsonl', release=value['release'], complete=value['complete'],
                manifest_sha256=value['manifest_sha256'], files=len(value['files']), deep=deep, checks=checks)
