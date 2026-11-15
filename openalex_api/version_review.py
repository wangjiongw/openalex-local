"""Disk-backed, resumable review sampling without retaining candidate payloads."""
from contextlib import closing
import json
import sqlite3
import time

from . import version_matching as rules
from .backend import sql_list
from .config import atomic_json
from .earth_control import read_json
from .semantics import digest

QUOTAS = dict(development=500, blind=600, hard_negative=400)
COLUMNS = ("id,JSONExtractString(payload,'id') AS pair_id,"
           "JSONExtractString(payload,'work_id') AS work_id,"
           "JSONExtractString(payload,'target_id') AS target_id,"
           "JSONExtract(payload,'parent_groups','Array(String)') AS parent_groups,"
           "JSONExtractRaw(payload,'evidence') AS evidence,"
           "JSONExtractBool(payload,'unique_anchor') AS unique_anchor,"
           "JSONExtractBool(payload,'known_pair') AS known_pair")


def find(parents, key):
    parents.setdefault(key, key)
    root = key
    while parents[root] != root:
        root = parents[root]
    while key != root:
        parent = parents[key]
        parents[key] = root
        key = parent
    return root


def exports(m, package):
    from . import versions_full as full
    root = full.path(m['release'])
    if m.get('review_identity') not in (None, package['identity']):
        raise RuntimeError('review package identity changed')
    m['review_identity'] = package['identity']
    m['calibration'] = dict(state='awaiting_independent_labels', counts=package['counts'])
    full.save(m)
    public = [{k: r[k] for k in ('id', 'source', 'target')} | dict(label=None, reviewer=None)
              for r in package['pairs'] if r['role'] != 'development']
    atomic_json(root/'review-package.json', package)
    atomic_json(root/'review-blind.json', dict(identity=package['identity'], pairs=public))
    atomic_json(root/'review-development.json', dict(identity=package['identity'],
                pairs=[r for r in package['pairs'] if r['role'] == 'development']))


def review(m, b):
    from . import versions_full as full
    if m.get('calibration', {}).get('passed'):
        return
    root = full.path(m['release'])
    identity = digest({k: m[k] for k in ('release', 'signature', 'feature_signature',
                     'candidate_signature', 'confirmation_signature', 'seed', 'selected_works')} |
                     dict(candidate_records=m['counts']['pairs']))
    with closing(sqlite3.connect(root/'review-staging.sqlite')) as db:
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA cache_size=-65536')
        db.execute('PRAGMA temp_store=FILE')
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('review staging integrity check failed')
        db.executescript('''CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY,value TEXT);
          CREATE TABLE IF NOT EXISTS pairs (pair_id TEXT UNIQUE,rank TEXT,candidate_id TEXT,
            work_id TEXT,proposed INTEGER,unique_anchor INTEGER,known_pair INTEGER);
          CREATE TABLE IF NOT EXISTS graph (nodes TEXT PRIMARY KEY) WITHOUT ROWID;''')
        state = dict(db.execute('SELECT key,value FROM state'))
        if state and state.get('identity') != identity:
            raise RuntimeError('review staging inputs changed')
        if not state:
            state = dict(identity=identity, after='', scanned='0')
            with db:
                db.executemany('INSERT INTO state VALUES (?,?)', state.items())
        if 'package' in state:
            package = json.loads(state['package'])
            target = root/'review-package.json'
            if target.exists() and read_json(target) != package:
                raise RuntimeError('review package differs from committed staging')
            exports(m, package)
            return
        started = time.monotonic()
        initial = int(state['scanned'])
        last_saved = 0.

        def progress(phase, processed, total):
            nonlocal last_saved
            full.protect(m)
            elapsed = max(.001, time.monotonic()-started)
            speed = max(0, int(state['scanned'])-initial)/elapsed
            value = dict(phase=phase, processed=processed, total=total,
                         unit='candidate_records' if phase == 'ingest' else 'graph_records',
                         records_per_second=speed if phase == 'ingest' else None,
                         eta_seconds=(total-processed)/speed if phase == 'ingest' and speed else None,
                         updated=time.time())
            atomic_json(root/'review-progress.json', value)
            old_phase = m.get('progress', {}).get('phase')
            m['progress'] = value
            m['counts']['review_candidates_scanned'] = int(state['scanned'])
            if phase != old_phase or time.monotonic()-last_saved >= 5:
                full.save(m)
                last_saved = time.monotonic()

        for page in full.candidate_pages(m, b, COLUMNS, size=10000, after=state['after']):
            pairs, graph = [], []
            for row in page:
                if not all(row.get(k) for k in ('pair_id', 'work_id', 'target_id')):
                    raise ValueError('candidate review identifiers missing')
                nodes = sorted({row['work_id'], row['target_id'], *row['parent_groups']})
                graph.append((json.dumps(nodes, separators=(',', ':')),))
                pairs.append((row['pair_id'], digest([m['seed'], row['pair_id']]), row['id'],
                              row['work_id'], int(rules.proposed(json.loads(row['evidence']))),
                              int(row['unique_anchor']), int(row['known_pair'])))
            with db:
                db.executemany('INSERT OR IGNORE INTO pairs VALUES (?,?,?,?,?,?,?)', pairs)
                db.executemany('INSERT OR IGNORE INTO graph VALUES (?)', graph)
                state.update(after=page[-1]['id'], scanned=str(int(state['scanned'])+len(page)))
                db.executemany('INSERT OR REPLACE INTO state VALUES (?,?)',
                               [(k, state[k]) for k in ('after', 'scanned')])
            progress('ingest', int(state['scanned']), m['counts']['pairs'])
        if int(state['scanned']) != m['counts']['pairs']:
            raise RuntimeError('review candidate coverage mismatch')
        parents = {}
        graph_count = db.execute('SELECT count(*) FROM graph').fetchone()[0]
        progress('components', 0, graph_count)
        for n, (raw,) in enumerate(db.execute('SELECT nodes FROM graph'), 1):
            nodes = json.loads(raw)
            find(parents, nodes[0])
            for key in nodes[1:]:
                a, c = find(parents, nodes[0]), find(parents, key)
                parents[max(a, c)] = min(a, c)
            if n % 10000 == 0:
                progress('components', n, graph_count)
        components = {}
        for key in parents:
            components.setdefault(find(parents, key), []).append(key)
        parts = {key: rules.partition(keys, m['seed']) for key, keys in components.items()}
        m['counts'].update(review_components=len(parts),
                           review_largest_component=max(map(len, components.values()), default=0))
        del components
        progress('selection', graph_count, graph_count)
        db.execute('CREATE INDEX IF NOT EXISTS pair_rank ON pairs(rank)')
        choices = {k: [] for k in QUOTAS}
        selected_studies = set()
        for pair_id, candidate_id, work_id, proposed, unique, known in db.execute(
                'SELECT pair_id,candidate_id,work_id,proposed,unique_anchor,known_pair '
                'FROM pairs ORDER BY rank,rowid'):
            study = find(parents, work_id)
            part = parts[study]
            role = ('development' if part < 3 else 'blind' if part < 8 and proposed and unique and not known
                    else 'hard_negative' if part >= 8 and not proposed else None)
            if study not in selected_studies and role and len(choices[role]) < QUOTAS[role]:
                choices[role].append((pair_id, candidate_id))
                selected_studies.add(study)
            if all(len(choices[k]) == QUOTAS[k] for k in QUOTAS):
                break
        full.protect(m)
        selected = [item for values in choices.values() for item in values]
        payloads = {}
        for batch in full.key_batches([item[1] for item in selected]):
            for row in b.ch(f'SELECT id,payload FROM {full.table(m)}.candidates FINAL '
                            f'WHERE id IN {sql_list(batch)} FORMAT JSONEachRow'):
                payloads[row['id']] = json.loads(row['payload'])
        if set(payloads) != {item[1] for item in selected}:
            raise RuntimeError('selected review payload missing')
        rows = []
        for role, items in choices.items():
            for pair_id, candidate_id in items:
                row = payloads[candidate_id]
                if row['id'] != pair_id:
                    raise RuntimeError('selected review pair identity changed')
                rows.append(row | dict(role=role, label=None, reviewer=None))
        package_identity = digest(dict(feature=m['feature_signature'], candidate=m['candidate_signature'],
            confirmation=m['confirmation_signature'], pairs=[(r['id'], r['role'], r['evidence']) for r in rows]))
        package = dict(threshold=0.99, schema=1, release=m['release'], identity=package_identity,
                       counts={k: len(v) for k, v in choices.items()}, required=QUOTAS, pairs=rows)
        with db:
            db.execute('INSERT INTO state VALUES (?,?)', ('package', json.dumps(package)))
        exports(m, package)
        progress('complete', int(state['scanned']), m['counts']['pairs'])
