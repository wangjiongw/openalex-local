"""Durable, bounded automatic assessment and global publication-star conflicts."""
from contextlib import closing
import json
import sqlite3
import time

from .semantics import digest


def connect(m):
    from . import versions_full as f
    db = sqlite3.connect(f.path(m['release'])/'assessment.sqlite')
    try:
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA cache_size=-65536')
        db.execute('PRAGMA temp_store=FILE')
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('assessment staging integrity check failed')
        db.executescript('''CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY,value TEXT);
          CREATE TABLE IF NOT EXISTS eligible (anchor TEXT,member TEXT,candidate_id TEXT,payload TEXT,
            PRIMARY KEY(anchor,member)) WITHOUT ROWID;
          CREATE INDEX IF NOT EXISTS member_anchors ON eligible(member,anchor);
          CREATE TABLE IF NOT EXISTS features (id TEXT PRIMARY KEY,hash TEXT,verified INTEGER)
            WITHOUT ROWID;
          CREATE INDEX IF NOT EXISTS pending_features ON features(verified,id);''')
        identity = digest(dict(signature=m['signature'],parent=m['parent_signature'],
                               review=m['automatic_review']['identity'],pairs=m['counts']['pairs']))
        state = dict(db.execute('SELECT key,value FROM state'))
        if state and state.get('identity') != identity:
            raise RuntimeError('assessment staging inputs changed')
        if not state:
            state = dict(identity=identity,after='',processed='0',eligible='0')
            with db:
                db.executemany('INSERT INTO state VALUES (?,?)',state.items())
        return db,state
    except BaseException:
        db.close()
        raise


def assess(m,b,db,state):
    from . import versions_full as f
    initial = int(state['processed']);started = time.monotonic()
    if state.get('complete') == '1':
        if initial!=m['counts']['pairs']:raise RuntimeError('automatic assessment coverage mismatch')
        return
    for page in f.candidate_pages(m,b,size=10000,after=state['after']):
        reviews = [];accepted = []
        for row in page:
            r = json.loads(row['payload'])
            if not f.automatic.allows(r):
                reviews.append(dict(id=r['id'],payload=json.dumps(r)))
            else:
                accepted.append((row['id'],r))
        # ClickHouse replay is idempotent; acknowledge its writes before the SQLite cursor.
        f.insert(b,f.table(m)+'.reviews',reviews)
        with db:
            for candidate_id,r in accepted:
                anchor=r['anchor'];member=r['work_id'] if r['target_id']==anchor else r['target_id']
                inserted=db.execute('INSERT OR IGNORE INTO eligible VALUES (?,?,?,?)',
                                    (anchor,member,candidate_id,json.dumps(r))).rowcount
                if not inserted:continue
                for feature in (r['source'],r['target']):
                    old=db.execute('SELECT hash FROM features WHERE id=?',(feature['id'],)).fetchone()
                    if old and old[0]!=feature['source_hash']:
                        raise RuntimeError('contradictory candidate source hashes')
                    db.execute('INSERT OR IGNORE INTO features VALUES (?,?,NULL)',
                               (feature['id'],feature['source_hash']))
            updated=dict(after=page[-1]['id'],processed=str(int(state['processed'])+len(page)),
                         eligible=str(int(state['eligible'])+len(accepted)))
            db.executemany('INSERT OR REPLACE INTO state VALUES (?,?)',updated.items())
        state.update(updated)
        processed=int(state['processed']);speed=(processed-initial)/max(.001,time.monotonic()-started)
        m['progress']=dict(phase='automatic_assessment',processed=processed,total=m['counts']['pairs'],
                           unit='candidate_records',records_per_second=speed,
                           eta_seconds=(m['counts']['pairs']-processed)/max(.001,speed),updated=time.time())
        f.protect(m);f.save(m)
    if int(state['processed']) != m['counts']['pairs']:
        raise RuntimeError('automatic assessment coverage mismatch')
    with db:
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")
    state['complete']='1'


def verify(m,b,db):
    from . import versions_full as f
    while rows:=db.execute('SELECT id,hash FROM features WHERE verified IS NULL ORDER BY id LIMIT ?',
                           (f.BATCH,)).fetchall():
        f.protect(m);keys=[r[0] for r in rows];expected=dict(rows)
        raws=b.details(m['generation'],'works',keys)
        if set(raws)!=set(keys) or any(digest(raws[k])!=expected[k] for k in keys):
            raise RuntimeError('proposed confirmation raw checksum mismatch')
        with db:
            db.executemany('UPDATE features SET verified=? WHERE id=?',
                           [(int(not f.legacy.rules.notice(raws[k])),k) for k in keys])
        m['progress']=dict(phase='raw_verification',after=keys[-1],updated=time.time());f.save(m)
    m['counts']['raw_verified']=db.execute('SELECT count(*) FROM features').fetchone()[0]


def write(m,b,parent,db):
    from . import versions_full as f
    from .version_group_batch import LIMIT,write_stars
    checkpoint=f.path(m['release'])/'groups-progress.json'
    after=''
    if checkpoint.exists():
        p=f.read_json(checkpoint)
        identity=digest(dict(signature=m['signature'],parent=m['parent_signature'],
                             review=m.get('review_identity'),automatic=m['automatic_review']['identity']))
        if p['identity']!=identity:raise RuntimeError('group checkpoint identity changed')
        after=p['after_anchor']
    while anchors:=db.execute('SELECT DISTINCT anchor FROM eligible WHERE anchor>? ORDER BY anchor LIMIT ?',
                              (after,m['group_batch_size'])).fetchall():
        # Bound payloads as well as anchor counts; global conflicts remain on disk.
        stars={};size=0
        for (anchor,) in anchors:
            members={};star_size=0
            for member,payload in db.execute('SELECT member,payload FROM eligible WHERE anchor=? ORDER BY candidate_id',(anchor,)):
                star_size+=len(payload.encode())
                if star_size>LIMIT:raise RuntimeError('single publication star exceeds write byte budget')
                members[member]=json.loads(payload)
            if stars and size+star_size>LIMIT:break
            stars[anchor]=members;size+=star_size
        seen={};verified={}
        for anchor,members in stars.items():
            for member in members:
                # Only cardinality matters; two different anchors already block confirmation.
                seen[member]={r[0] for r in db.execute('SELECT anchor FROM eligible WHERE member=? LIMIT 2',(member,))}
            for key in (anchor,*members):
                row=db.execute('SELECT verified FROM features WHERE id=?',(key,)).fetchone()
                if row is None or row[0] is None:raise RuntimeError('unverified publication star')
                verified[key]=bool(row[0])
        write_stars(m,b,parent,stars,seen,verified)
        after=max(stars)


def run(m,b,parent):
    from . import versions_full as f
    db,state=connect(m)
    with closing(db):
        assess(m,b,db,state)
        verify(m,b,db)
        write(m,b,parent,db)
        assessed=int(state['processed']);eligible=int(state['eligible'])
        m['automatic_review'].update(state='complete',evaluated=assessed,eligible=eligible,
                                     unresolved=assessed-eligible)
        f.atomic_json(f.path(m['release'])/'automatic-review.json',m['automatic_review'])
