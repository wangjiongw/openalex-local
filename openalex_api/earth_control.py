"""Bounded recovery outside the source-pinned bibliographic exporter."""
import errno
import json
import logging
import time
from pathlib import Path

from . import earth_collections as ec
from .config import atomic_json, generation_path
from .ingest import lock
from .resources import Paused

LOG = logging.getLogger(__name__)
DELAYS = (.1, .5, 1, 2, 5)


def read_json(path):
    for attempt in range(len(DELAYS) + 1):
        try:
            return json.loads(Path(path).read_text())
        except FileNotFoundError:
            if attempt == len(DELAYS):
                raise
            time.sleep(DELAYS[attempt])


def retryable(error, g, collection):
    allowed = (generation_path(g) / 'search-catalog.json',
               ec.collection_path(g, collection) / 'manifest.json')
    return (error.errno == errno.ENOENT and error.filename is not None
            and Path(error.filename).absolute() in [p.absolute() for p in allowed])


def run(g, collection='earth_scope_v1', follow=False, max_blocks=None):
    path = ec.collection_path(g, collection)
    manifest = path / 'manifest.json'
    recovery = path / 'export-recovery.json'
    with lock(path / 'export-control.lock'):
        state = dict(state='running', retries=0, consecutive_failures=0, started=time.time(),
                     initial_blocks=None, last_successful_blocks=None, last_error=None)
        def save(**values):
            state.update(values, updated=time.time())
            atomic_json(recovery, state)
        save()
        try:
            initial = read_json(manifest) if manifest.exists() or (path / 'blocks').exists() else {}
            start = initial.get('blocks', 0)
            last = start
            consecutive = 0
            save(initial_blocks=start, last_successful_blocks=start)
            while True:
                current = read_json(manifest) if manifest.exists() or (path / 'blocks').exists() else {}
                remaining = None if max_blocks is None else max(0, start + max_blocks - current.get('blocks', 0))
                try:
                    result = ec.run(g, collection, follow, remaining)
                except FileNotFoundError as error:
                    if not retryable(error, g, collection):
                        raise
                    current = read_json(manifest) if manifest.exists() or (path / 'blocks').exists() else {}
                    committed = current.get('blocks', 0)
                    if committed > last:
                        consecutive = 0
                    last = committed
                    if consecutive == len(DELAYS):
                        raise
                    delay = DELAYS[consecutive]
                    consecutive += 1
                    save(state='retrying', retries=state['retries'] + 1, consecutive_failures=consecutive,
                         last_successful_blocks=committed, last_error=str(error), error_path=error.filename,
                         next_delay_seconds=delay)
                    LOG.warning('earth_export_retry generation=%s collection=%s attempt=%d path=%s delay=%.1f blocks=%d',
                                g, collection, consecutive, error.filename, delay, committed)
                    time.sleep(delay)
                    continue
                save(state=result['state'], consecutive_failures=0, last_successful_blocks=result['blocks'],
                     next_delay_seconds=None, finished=time.time())
                return result
        except BaseException as error:
            save(state='paused' if isinstance(error, Paused) else 'failed', last_error=str(error),
                 error_path=getattr(error, 'filename', None), next_delay_seconds=None)
            LOG.log(logging.INFO if isinstance(error, Paused) else logging.ERROR,
                    'earth_export_%s generation=%s collection=%s error=%s', state['state'], g, collection, error)
            raise


def status(g, collection):
    path = ec.collection_path(g, collection)
    for attempt in range(len(DELAYS) + 1):
        try:
            value = ec.status(g, collection)
            break
        except FileNotFoundError as error:
            if not retryable(error, g, collection):
                raise
            if attempt == len(DELAYS):
                try:
                    value = read_json(path / 'manifest.json')
                except FileNotFoundError:
                    value = dict(state='unavailable')
                value.update(path=str(path), current_catalog=dict(state='unavailable', error=str(error)))
                break
            time.sleep(DELAYS[attempt])
    recovery = path / 'export-recovery.json'
    try:
        value['recovery'] = read_json(recovery) if recovery.exists() else None
    except FileNotFoundError:
        value['recovery'] = dict(state='unavailable')
    return value
