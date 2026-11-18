"""Bounded publication-star writes with read-back before advancing the anchor cursor."""
import json

LIMIT = 16 * 1024**2


def write_stars(m,b,parent,stars,seen,verified):
    from . import versions_full as f
    from .semantics import digest
    checkpoint=f.path(m['release'])/'groups-progress.json'
    identity=digest(dict(signature=m['signature'],parent=m['parent_signature'],review=m.get('review_identity'),automatic=m.get('automatic_review',{}).get('identity')))
    p=f.read_json(checkpoint) if checkpoint.exists() else dict(identity=identity,after_anchor='')
    if p['identity']!=identity:raise RuntimeError('group checkpoint identity changed')
    prior=f.table(parent);db=f.table(m)
    entries=[(a,rows) for a,rows in sorted(stars.items()) if a>p['after_anchor']]
    for offset in range(0,len(entries),m['group_batch_size']):
        f.protect(m);chunk=entries[offset:offset+m['group_batch_size']]
        keys={key for anchor,rows in chunk for key in (anchor,*rows)}
        old={}
        for batch in f.key_batches(keys):
            old.update({r['id']:r['group_id'] for r in b.ch(f'SELECT id,group_id FROM {prior}.members FINAL WHERE id IN {f.sql_list(batch)} FORMAT JSONEachRow')})
        parents={}
        for batch in f.key_batches(old.values()):
            parents.update({r['id']:json.loads(r['payload']) for r in b.ch(f'SELECT id,payload FROM {prior}.groups FINAL WHERE id IN {f.sql_list(batch)} FORMAT JSONEachRow')})
        pending={name:[] for name in ('groups','members','candidates')};size=0;last=p['after_anchor']
        def flush():
            for name,rows in pending.items():
                f.insert(b,db+'.'+name,rows)
            for name,rows in pending.items():
                expected={r['id']:r for r in rows};found={}
                columns='id,group_id' if name=='members' else 'id,payload'
                for batch in f.key_batches(expected):
                    found.update({r['id']:r for r in b.ch(f'SELECT {columns} FROM {db}.{name} FINAL WHERE id IN {f.sql_list(batch)} FORMAT JSONEachRow')})
                if found!=expected:raise RuntimeError('group batch write reconciliation failed: '+name)
            p['after_anchor']=last;f.atomic_json(checkpoint,p)
            m['progress']=dict(phase='group_writes',after_anchor=last,updated=f.time.time())
            f.save(m)
        for anchor,members in chunk:
            keys=sorted({anchor,*members});gids=sorted({old[k] for k in keys if k in old})
            inherited=[parents[g] for g in gids]
            if len(inherited)>1 or any(g['preferred_openalex_id']!=anchor for g in inherited) or any(len(seen[k])>1 for k in members):
                last=anchor;continue
            if not all(verified[k] for k in keys):raise RuntimeError('notice reached automatic confirmation')
            old_group=inherited[0] if inherited else None
            all_members=sorted(set(keys+(old_group['members'] if old_group else [])))
            gid=old_group['group_id'] if old_group else 'vg_'+digest(dict(release=m['release'],members=all_members))[:32]
            auto=m.get('review_mode')=='automatic'
            provenance=dict(method='automatic_bibliographic',policy=f.automatic.POLICY,independent_review=False) if auto else dict(method='calibrated_bibliographic',calibration=m['review_identity'])
            payload=dict(group_id=gid,members=all_members,preferred_openalex_id=anchor,state='confirmed',relation_type='preprint_publication',evidence=(old_group['evidence'] if old_group else [])+[r['evidence']|provenance for r in members.values()])
            if auto:payload['assessment_mode']='automatic_rules'
            records=dict(groups=[dict(id=gid,payload=json.dumps(payload))],members=[dict(id=k,group_id=gid) for k in all_members],candidates=[dict(id=r['work_id']+':'+r['target_id'],payload=json.dumps(r|dict(state='confirmed',group_id=gid))) for r in members.values()])
            n=sum(len(json.dumps(row).encode())+1 for rows in records.values() for row in rows)
            if n>LIMIT:raise RuntimeError('single publication star exceeds write byte budget')
            if size+n>LIMIT:
                flush();pending={name:[] for name in pending};size=0
            for name,rows in records.items():pending[name].extend(rows)
            size+=n;last=anchor
        flush()
