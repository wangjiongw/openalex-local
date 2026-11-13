"""Optional version-group views; raw work and arXiv /versions contracts stay intact."""
import json

from fastapi import HTTPException

from . import versions_reader as pipeline
from .version_groups import group_id, notice
from .model import canonical_id
from .backend import sql_string


def current():
    if not pipeline.status()['ready']:
        raise HTTPException(503,'version relationships are not published or unavailable')
    return pipeline.published()


def group(backend, value, manifest=None):
    value=group_id(value)
    m=manifest or current()
    rows=backend.ch(f'SELECT payload FROM {pipeline.table(m)}.groups FINAL WHERE id={sql_string(value)} FORMAT JSONEachRow')
    if not rows:raise HTTPException(404,'version group not found in this relationship release')
    result=json.loads(rows[0]['payload'])
    found=backend.details(m['generation'],'works',result['members'])
    if set(found)!=set(result['members']):raise HTTPException(503,'version group target unavailable')
    return dict(relationship_release=m['release'],generation=m['generation'],**result,
                records=[dict(openalex_id=k,record=found[k]) for k in result['members']],
                bibliography='references, OA, authors and citation counts belong to each selected record; never summed or transferred')


def work(backend, value):
    key=canonical_id(value)
    if not key or not key.startswith('W'):raise ValueError('expected OpenAlex work ID')
    m=current()
    found=backend.details(m['generation'],'works',[key])
    if key not in found:raise HTTPException(404,'work not present')
    rows=backend.ch(f'SELECT group_id FROM {pipeline.table(m)}.members FINAL WHERE id={sql_string(key)} FORMAT JSONEachRow')
    base=dict(relationship_release=m['release'],generation=m['generation'],requested_openalex_id=key,requested_record=found[key],automatic_display_allowed=not notice(found[key]))
    if m.get('schema',1)==2:
        coverage=backend.ch(f'SELECT payload FROM {pipeline.table(m)}.coverage FINAL WHERE id={sql_string(key)} FORMAT JSONEachRow')
        base['processing_state']=json.loads(coverage[0]['payload'])['state'] if coverage else 'not_processed'
    if not rows:return base|dict(state='unmatched',group_id=None,members=[],preferred_openalex_id=None,evidence=[])
    return group(backend,rows[0]['group_id'],m)|base


def candidates(backend, value, release=None, cursor=None, per_page=25):
    import base64
    from .config import generation_name
    key=canonical_id(value)
    if not key or not key.startswith('W'):raise ValueError('expected OpenAlex work ID')
    if not 1<=per_page<=100:raise ValueError('per_page must be 1..100')
    after=''
    if cursor:
        try:
            if len(cursor)>4096:raise ValueError()
            pin=json.loads(base64.urlsafe_b64decode(cursor))
            if pin['work']!=key or (release and release!=pin['release']):raise ValueError()
            release=generation_name(pin['release']);after=pin['after']
            if not after.startswith(key+':') or canonical_id(after.split(':')[1])!=after.split(':')[1]:raise ValueError()
        except (ValueError,KeyError,TypeError,IndexError) as error:
            raise ValueError('invalid version candidate cursor') from error
    m=pipeline.published(release) if release else current()
    if m.get('schema',1)==1:
        return dict(relationship_release=m['release'],work_id=key,processing_state='legacy_pilot_only',results=[],next_cursor=None)
    prefix=key+':'
    rows=backend.ch(f"SELECT id,payload FROM {pipeline.table(m)}.candidates FINAL WHERE startsWith(id,{sql_string(prefix)}) AND JSONExtractString(payload,'state')!='confirmed' AND id>{sql_string(after)} ORDER BY id LIMIT {per_page+1} FORMAT JSONEachRow")
    coverage=backend.ch(f'SELECT payload FROM {pipeline.table(m)}.coverage FINAL WHERE id={sql_string(key)} FORMAT JSONEachRow')
    result=[json.loads(r['payload']) for r in rows[:per_page]]
    next_cursor=base64.urlsafe_b64encode(json.dumps(dict(work=key,release=m['release'],after=rows[per_page-1]['id'])).encode()).decode() if len(rows)>per_page else None
    return dict(relationship_release=m['release'],generation=m['generation'],work_id=key,
                processing_state=json.loads(coverage[0]['payload'])['state'] if coverage else 'not_processed',
                calibration=m.get('calibration'),results=result,next_cursor=next_cursor,
                semantics='candidates require review; source records and reference lists remain separate')
