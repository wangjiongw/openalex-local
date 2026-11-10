"""Indexed exact set comparisons; directional evidence and bounded streaming planning."""
import hashlib
import heapq
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from .backend import Backend, database, sql_string
from .config import atomic_json, config, generation_path
from . import resources

LOG = logging.getLogger(__name__)
RANGE_SIZE = 250000


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:24]


def open_tasks(directory, left, right):
    """Keep legacy files; only inherit completion when its recorded direction matches."""
    target = directory / ('direction-' + digest([left, right]))
    target.mkdir(parents=True, exist_ok=True)
    path = target / 'tasks.sqlite'
    if not path.exists():
        legacy = directory / 'tasks.sqlite'
        # A finished plan partitions the union and is valid in both directions.
        # Reuse it without re-reading all IDs; completion itself stays directional.
        for candidate in sorted(directory.glob('direction-*/tasks.sqlite')):
            with sqlite3.connect(candidate) as peer:
                planned = peer.execute('SELECT done FROM planning WHERE singleton=1').fetchone()
            if planned and planned[0]:
                legacy = candidate
                break
        temp = target / 'migrating.sqlite'
        temp.unlink(missing_ok=True)
        if legacy.exists():
            with sqlite3.connect(legacy) as src, sqlite3.connect(temp) as dst:
                src.backup(dst)
                report = legacy.parent / 'result.json'
                proof = json.loads(report.read_text()) if report.exists() else {}
                # v1 relation caches aliased opposite directions; only source/entity
                # proofs had direction-distinct field lists. Do not bless aliased results.
                proven = ({left, right} == {'entities', 'audit'}
                          and proof.get('left') == left and proof.get('right') == right)
                for lo, in dst.execute('SELECT lo FROM ranges WHERE state="done"').fetchall():
                    filename = hashlib.sha256(lo.encode()).hexdigest()[:16] + '.jsonl'
                    original = legacy.parent / filename
                    if proven and original.exists():
                        shutil.copy2(original, target / filename)
                        with (target / filename).open('rb') as f:
                            os.fsync(f.fileno())
                    else:
                        dst.execute('UPDATE ranges SET state="pending",missing=NULL WHERE lo=?', (lo,))
                dst.commit()
            os.replace(temp, path)
        fd = os.open(target, os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
        atomic_json(target / 'identity.json', dict(left=left, right=right, legacy=str(legacy), version=2))
    db = sqlite3.connect(path, timeout=60)
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS ranges(lo TEXT PRIMARY KEY,hi TEXT,expected INTEGER,state TEXT,missing INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS planning(singleton INTEGER PRIMARY KEY,last TEXT,done INTEGER)')
    return target, db


def snapshot(b, g, table):
    rows = b.ch(f"SELECT name,uuid,sorting_key FROM system.tables WHERE database={sql_string(database(g))} AND name={sql_string(table)} FORMAT JSONEachRow")
    if len(rows) != 1:
        raise RuntimeError('exact source table missing: ' + table)
    parts = b.ch(f"SELECT name,rows,data_version,bytes_on_disk FROM system.parts WHERE active AND database={sql_string(database(g))} AND table={sql_string(table)} ORDER BY name FORMAT JSONEachRow")
    return dict(table=rows[0], parts=parts)


def ordered_source(g, b, table, fields, key):
    """Materialize FINAL once only when the original order cannot seek the range key."""
    before = snapshot(b, g, table)
    order = [s.strip(' `()') for s in before['table']['sorting_key'].split(',')]
    wanted = ['entity', key] if 'entity' in fields.split(',') else [key]
    if order[:len(wanted)] == wanted:
        return table, True
    identity = dict(source=before, fields=fields, key=key, version=1)
    token = digest(identity)
    directory = generation_path(g) / 'exact-projections'
    directory.mkdir(exist_ok=True)
    path = directory / (token + '.json')
    saved = json.loads(path.read_text()) if path.exists() else {}
    if saved.get('state') == 'ready':
        actual = b.ch(f"SELECT toString(uuid) uuid FROM system.tables WHERE database={sql_string(database(g))} AND name={sql_string(saved['table'])} FORMAT JSONEachRow")
        if actual != [{'uuid': saved['uuid']}]:
            raise RuntimeError('exact projection identity changed')
        return saved['table'], False
    size = sum(int(p['bytes_on_disk']) for p in before['parts'])
    resources.checkpoint(g, large=True, additional=max(size * 2, 1024**3), temporary=max(size, 2*1024**3))
    name = 'exact_sorted_' + token + '_' + uuid.uuid4().hex[:8]
    history = saved.get('attempts', []) + ([saved['table']] if saved.get('table') else [])
    state = dict(state='building', table=name, identity=identity, attempts=history, started=time.time())
    atomic_json(path, state)
    sort = wanted + [f for f in fields.split(',') if f not in wanted]
    b.ch(f"CREATE TABLE {database(g)}.{name} ENGINE=MergeTree ORDER BY ({','.join(sort)}) AS SELECT {fields} FROM {database(g)}.{table} FINAL WHERE 0")
    uid = b.ch(f"SELECT toString(uuid) uuid FROM system.tables WHERE database={sql_string(database(g))} AND name={sql_string(name)} FORMAT JSONEachRow")[0]['uuid']
    state['uuid'] = uid
    atomic_json(path, state)
    tick = time.monotonic()
    b.ch(f"INSERT INTO {database(g)}.{name} SELECT {fields} FROM {database(g)}.{table} FINAL SETTINGS max_threads=4,max_insert_threads=2,max_bytes_before_external_sort=2147483648", query_id='oa-exact-copy-' + uuid.uuid4().hex)
    if snapshot(b, g, table) != before:
        raise RuntimeError('exact source changed while creating sorted projection; preserve and retry')
    atomic_json(path, state | dict(state='ready', completed=time.time(), seconds=time.monotonic()-tick))
    LOG.info('exact projection source=%s table=%s seconds=%.3f', table, name, time.monotonic()-tick)
    return name, False


KEY_PAGE_SIZE = 250000


def merged_keys(b, g, sources, key, entity, lower):
    # Finish each bounded HTTP response before yielding to a potentially slow
    # consumer. Leaving a whole-table response open during difference writes
    # exceeded the server socket send timeout on the full dataset.
    def pages(table, final):
        cursor = lower
        where = ('entity=' + sql_string(entity) + ' AND ') if entity else ''
        while True:
            literal = str(int(cursor)) if key == 'citing' else sql_string(cursor)
            query = (f"SELECT {key} FROM {database(g)}.{table}{' FINAL' if final else ''} "
                     f"WHERE {where}{key}>{literal} GROUP BY {key} ORDER BY {key} LIMIT {KEY_PAGE_SIZE} "
                     "SETTINGS optimize_aggregation_in_order=1,max_bytes_before_external_group_by=2147483648 FORMAT JSONEachRow")
            stream = b.ch_stream(query)
            try:
                page = list(stream)
            finally:
                stream.close()
            if not page:
                return
            cursor = page[-1][key]
            yield from page
            if len(page) < KEY_PAGE_SIZE:
                return
    streams = [pages(table, final) for table, final in sources]
    try:
        previous = None
        for row in heapq.merge(*streams, key=lambda r: r[key]):
            value = row[key]
            if value != previous:
                yield value
                previous = value
    finally:
        for stream in streams:
            stream.close()


def difference(g, sources, fields, key, entity, lo, hi, path):
    b = Backend(writer=True)
    try:
        resources.checkpoint(g)
        literal = (lambda v: str(int(v))) if key == 'citing' else sql_string
        predicate = f"{key}>{literal(lo)} AND {key}<={literal(hi)}"
        if entity:
            predicate = f"entity={sql_string(entity)} AND " + predicate
        def source(item):
            table, final = item
            return f"(SELECT {fields} FROM {database(g)}.{table}{' FINAL' if final else ''} WHERE {predicate})"
        query = (f"SELECT {fields} FROM {source(sources[0])} l LEFT ANTI JOIN {source(sources[1])} r USING({fields}) "
                 "SETTINGS join_algorithm='grace_hash',max_bytes_in_join=1073741824 FORMAT JSONEachRow")
        count = 0
        with path.with_suffix('.tmp').open('w') as out:
            for row in b.ch_stream(query):
                out.write(json.dumps(row, separators=(',', ':')) + '\n')
                count += 1
            out.flush()
            os.fsync(out.fileno())
        os.replace(path.with_suffix('.tmp'), path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return count
    finally:
        b.close()


def compare_ranges(g, b, token, identities, parts, left, right, fields, key, entity):
    directory = generation_path(g) / 'exact-differences' / token
    directory.mkdir(parents=True, exist_ok=True)
    target, db = open_tasks(directory, left, right)
    streams = None
    active = {}
    workers = max(1, min(4, config().get('exact_workers', 4)))
    started = time.monotonic()
    def progress(phase):
        n, done, missing = db.execute('SELECT count(*),coalesce(sum(state="done"),0),coalesce(sum(missing),0) FROM ranges').fetchone()
        plan = db.execute('SELECT done FROM planning WHERE singleton=1').fetchone()
        atomic_json(generation_path(g) / 'exact-progress.json', dict(state='complete' if phase == 'complete' else 'running', phase=phase, left=left, right=right, entity=entity, total=n, done=done, missing=missing, planning_complete=bool(plan and plan[0]), workers=workers, updated=time.time(), elapsed_seconds=time.monotonic()-started, directory=str(target)))
    def confirm(future):
        lo = active.pop(future)
        missing = future.result()
        with db:
            db.execute('UPDATE ranges SET state="done",missing=? WHERE lo=?', (missing, lo))
    try:
        row = db.execute('SELECT last,done FROM planning WHERE singleton=1').fetchone()
        pending = db.execute('SELECT count(*) FROM ranges WHERE state!="done"').fetchone()[0]
        if row and row[1] and not pending:
            result = dict(left=left, right=right, fields=fields, entity=entity, missing=db.execute('SELECT coalesce(sum(missing),0) FROM ranges').fetchone()[0], identities=identities)
            atomic_json(target/'result.json', result)
            progress('complete')
            return result['missing']
        progress('preparing-sorted-sources')
        sources = [ordered_source(g, b, t, fields, key) for t in (left, right)]
        lower = row[0] if row else (-1 if key == 'citing' else '')
        if key == 'citing': lower = int(lower)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            def drain(block=False):
                resources.checkpoint(g)
                busy = set(active.values())
                for lo, hi in db.execute('SELECT lo,hi FROM ranges WHERE state!="done"').fetchall():
                    if len(active) >= workers: break
                    if lo in busy: continue
                    path = target / (hashlib.sha256(lo.encode()).hexdigest()[:16] + '.jsonl')
                    active[pool.submit(difference, g, sources, fields, key, entity, lo, hi, path)] = lo
                if active:
                    ready, _ = wait(active, timeout=1 if block else 0, return_when=FIRST_COMPLETED)
                    for future in ready: confirm(future)
                progress('checking-and-planning')
            try:
                # Confirm already planned ranges immediately; retain their exact boundaries.
                while db.execute('SELECT count(*) FROM ranges WHERE state!="done"').fetchone()[0] >= workers * 2:
                    drain(True)
                if not row or not row[1]:
                    streams = merged_keys(b, g, sources, key, entity, lower)
                    last, n = lower, 0
                    for value in streams:
                        last, n = value, n + 1
                        if n == RANGE_SIZE:
                            resources.checkpoint(g)
                            with db:
                                db.execute('INSERT OR IGNORE INTO ranges VALUES(?,?,?,?,NULL)', (str(lower), str(last), n, 'pending'))
                                db.execute('INSERT OR REPLACE INTO planning VALUES(1,?,0)', (str(last),))
                            lower, n = last, 0
                            drain()
                            while db.execute('SELECT count(*) FROM ranges WHERE state!="done"').fetchone()[0] >= workers * 2:
                                drain(True)
                    with db:
                        if n: db.execute('INSERT OR IGNORE INTO ranges VALUES(?,?,?,?,NULL)', (str(lower), str(last), n, 'pending'))
                        db.execute('INSERT OR REPLACE INTO planning VALUES(1,?,1)', (str(last),))
                while db.execute('SELECT count(*) FROM ranges WHERE state!="done"').fetchone()[0]:
                    drain(True)
            finally:
                if streams is not None: streams.close()
                # Completed sinks remain durable even when another task fails or pause arrives.
                first_error = None
                for future in list(active):
                    try: confirm(future)
                    except resources.Paused: pass
                    except BaseException as error: first_error = first_error or error
                if first_error: raise first_error
        total = db.execute('SELECT coalesce(sum(missing),0) FROM ranges').fetchone()[0]
        atomic_json(target/'result.json', dict(left=left, right=right, fields=fields, entity=entity, missing=total, identities=identities))
        progress('complete')
        return total
    finally:
        db.close()


def conflict_count(g, b):
    """Ordered aggregation over audit's entity/id prefix, with a source-bound receipt."""
    from .proofs import semantic

    resources.checkpoint(g)
    before = snapshot(b, g, 'audit')
    identity = dict(source=before, semantic=semantic(g), method='digest-conflicts-v1')
    path = generation_path(g) / 'exact-conflicts' / (digest(identity) + '.json')
    if path.exists():
        saved = json.loads(path.read_text())
        if saved.get('identity') == identity:
            return saved['conflicts']
    count = int(b.ch(
        f"SELECT count() n FROM (SELECT entity,id FROM {database(g)}.audit FINAL "
        "GROUP BY entity,id HAVING uniqExact(digest)>1) "
        "SETTINGS optimize_aggregation_in_order=1 FORMAT JSONEachRow"
    )[0]['n'])
    if snapshot(b, g, 'audit') != before:
        raise RuntimeError('source audit changed while checking digest conflicts')
    atomic_json(path, dict(identity=identity, conflicts=count, completed=time.time()))
    return count
