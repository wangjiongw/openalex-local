"""Source-specific retrieval without changing OpenAlex raw-record contracts."""
from fastapi import HTTPException

from . import arxiv_pipeline as source, arxiv_index
from .arxiv_source import identifier
from .backend import sql_string


def current(release=None):
    try:
        if release is None and not source.status()['ready']:
            raise RuntimeError('arXiv source is unavailable')
        return source.published(release)
    except (FileNotFoundError,RuntimeError) as error:
        raise HTTPException(503, str(error)) from error


def listing(backend, client, params, per_page=25, cursor=None):
    release = None
    if cursor:
        from .query import decode_cursor
        from .config import config
        release = decode_cursor(cursor,config()['cursor_secret']).get('release')
    m = current(release)
    hits,next_cursor = arxiv_index.search(client,m,params,per_page,cursor)
    keys = [h['_source']['original_id'] for h in hits['hits']]
    records = source.hydrate(m,keys,backend)
    if len(records) != len(keys) or len(set(keys)) != len(keys):
        raise RuntimeError('arXiv search/hydration mismatch')
    return dict(meta=dict(count=hits['total'],count_relation='eq',next_cursor=next_cursor,
                          source='arxiv',source_release=m['release'],generation=m['generation']),
                results=[records[k] for k in keys])


def lookup(backend, value):
    m = current()
    key = 'arxiv:'+identifier(value)
    records = source.hydrate(m,[key],backend)
    if key not in records:
        raise HTTPException(404,'arXiv record not found in the published source release')
    return records[key]


def versions(backend, work_id):
    from .model import canonical_id
    key = canonical_id(work_id)
    if not key or not key.startswith('W'):
        raise ValueError('expected OpenAlex work ID')
    m = current()
    rows = backend.ch(f'SELECT arxiv_id FROM {source.table(m)}.versions FINAL WHERE openalex_id={sql_string(key)} ORDER BY arxiv_id FORMAT JSONEachRow')
    records = source.hydrate(m,[r['arxiv_id'] for r in rows],backend)
    return dict(source_release=m['release'],generation=m['generation'],work_id=key,
                results=list(records.values()), bibliography='belongs to each OpenAlex record; never transferred to preprint raw data')


def export_plan(payload, plan):
    m = current()
    plan.update(kind='arxiv',source='arxiv',source_release=m['release'],source_signature=m['signature'],generation=m['generation'])
    if 'identifiers' in payload:
        values = payload['identifiers']
        if not isinstance(values,list) or not values or len(values)>100000 or any(not isinstance(v,str) for v in values):
            raise ValueError('provide arXiv identifiers')
        plan['identifiers'] = sorted(set('arxiv:'+identifier(v) for v in values))
    else:
        q = payload['query']
        if not isinstance(q,dict) or set(q)!={'route','params'} or q['route']!='/v1/arxiv/works' or not isinstance(q['params'],dict):
            raise ValueError('arXiv export requires /v1/arxiv/works query')
        params = arxiv_index.parameters(q['params'])
        params['sort'] = 'id:asc'
        plan['arxiv_query'] = params
    return plan


def export_rows(plan, after, backend, limit):
    from .manticore_engine import Client
    from .backend import sql_list
    m = current(plan['source_release'])
    if m['signature'] != plan['source_signature'] or m['generation'] != plan['generation']:
        raise RuntimeError('arXiv export source identity changed')
    if 'identifiers' in plan:
        rows = backend.ch(f"SELECT number,id FROM {source.table(m)}.records FINAL WHERE id IN {sql_list(plan['identifiers'])} AND number>{int(after or 0)} ORDER BY number LIMIT {limit} FORMAT JSONEachRow")
        from .exports import path
        from .config import atomic_json
        target = path(plan['job'])/'resolved.json'
        if not target.exists():
            found = source.hydrate(m,plan['identifiers'],backend)
            atomic_json(target, dict(unresolved=[dict(identifier=k,status='not_found') for k in plan['identifiers'] if k not in found]))
        keys = [r['id'] for r in rows]
        numbers = [r['number'] for r in rows]
    else:
        client = Client()
        try:
            data = client.search(arxiv_index.request(m,plan['arxiv_query'],limit,after=after))
        finally:
            client.close()
        keys = [h['_source']['original_id'] for h in data['hits']['hits']]
        numbers = [int(h['_id']) for h in data['hits']['hits']]
    found = source.hydrate(m,keys,backend)
    if len(found) != len(keys):
        raise RuntimeError('arXiv export hydration mismatch')
    return [(str(n),found[k]) for n,k in zip(numbers,keys)]
