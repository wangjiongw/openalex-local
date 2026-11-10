"""Read and verify user-provided local OpenAlex JSONL snapshots."""
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

from .config import ENTITIES


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
