"""Optional version-group views; raw work and arXiv /versions contracts stay intact."""
import json

from fastapi import HTTPException

from . import versions_pipeline as pipeline
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
    if not rows:return base|dict(state='unmatched',group_id=None,members=[],preferred_openalex_id=None,evidence=[])
    return group(backend,rows[0]['group_id'],m)|base
