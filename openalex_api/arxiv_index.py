"""Independent arXiv lexical projection; never changes qualified OpenAlex signatures."""
import hashlib
import json
import time

from .config import config
from .backend import sql_string
from .manticore_engine import checked_name, normalize
from .semantics import digest
from .query import encode_cursor, decode_cursor

VERSION = 1


def category_id(value):
    return int.from_bytes(hashlib.sha256(value.encode()).digest()[:7], 'big')


def create(client, table):
    client.sql(f"CREATE TABLE {checked_name(table)} (name text, abstract text, author_name text, "
               "publication_year integer, submitted bigint, categories multi64, original_id string, "
               "source_version string, source_hash string, projection_hash string) "
               "morphology='stem_en' index_exact_words='1' charset_table='non_cjk' "
               "ngram_len='1' ngram_chars='cjk' rt_mem_limit='256M'")


def project(number, record):
    from datetime import datetime
    doc = dict(name=normalize(record['title']), abstract=normalize(record['abstract']),
               author_name=normalize(record['authors']), publication_year=record['publication_year'],
               submitted=int(datetime.fromisoformat(record['first_submitted']).timestamp()),
               categories=[category_id(c) for c in record['categories']], original_id=record['id'],
               source_version=record['latest_version'], source_hash=digest(record))
    doc['projection_hash'] = digest(doc)
    return number, doc


PARAMS = {'search', 'search_field', 'author_search', 'year', 'category', 'sort'}


def parameters(params):
    if set(params) - PARAMS:
        raise ValueError('unsupported arXiv query parameters')
    p = dict(search='', search_field='all', author_search='', year='', category='', sort='relevance') | params
    if p['search_field'] not in ('all', 'title', 'abstract', 'author') or p['sort'] not in ('relevance', 'submitted:desc', 'submitted:asc', 'id:asc'):
        raise ValueError('unsupported arXiv search field or sort')
    if any(not isinstance(v, str) for v in p.values()) or len(p['search']) > 2000 or len(p['author_search']) > 500:
        raise ValueError('invalid arXiv query values')
    if p['year']:
        parts = p['year'].split('..')
        if len(parts) > 2 or any(not v.isdigit() or not 1000 <= int(v) <= 3000 for v in parts):
            raise ValueError('year must be YYYY or YYYY..YYYY')
        if int(parts[0]) > int(parts[-1]):
            raise ValueError('reversed arXiv year range')
    if p['category']:
        import re
        if not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9.\-]{0,64}', p['category']):
            raise ValueError('invalid arXiv category')
    return p


def request(manifest, params, limit, after=None, offset=0):
    p = parameters(params)
    must = []
    for text, field in ((p['search'], {'all':'name,abstract', 'title':'name', 'abstract':'abstract', 'author':'author_name'}[p['search_field']]), (p['author_search'], 'author_name')):
        if text.strip():
            must.append({'match': {field: {'query': normalize(text), 'operator': 'and'}}})
    if p['year']:
        years = p['year'].split('..')
        must.append({'range': {'publication_year': {'gte':int(years[0]), 'lte':int(years[-1])}}})
    if p['category']:
        must.append({'equals': {'any(categories)': category_id(p['category'])}})
    if after is not None:
        must.append({'range': {'id': {'gt':int(after)}}})
    sorts = {'relevance':[{'_score':'desc'}, {'id':'asc'}], 'id:asc':[{'id':'asc'}],
             'submitted:desc':[{'submitted':'desc'}, {'id':'asc'}], 'submitted:asc':[{'submitted':'asc'}, {'id':'asc'}]}
    return dict(index=manifest['segment']['index'], query={'bool':{'must':must}} if must else {'match_all':{}},
                limit=limit, offset=offset, sort=sorts[p['sort']], options={'max_matches':max(1000, offset+limit), 'cutoff':0, 'accurate_aggregation':1},
                _source=['original_id'])


def search(client, manifest, params, per_page=25, cursor=None):
    p = parameters(params)
    if not 1 <= per_page <= 100:
        raise ValueError('per_page must be 1..100')
    key = digest(dict(release=manifest['release'], query=p, per_page=per_page))
    body = request(manifest, p, per_page)
    token = None
    if cursor:
        token = decode_cursor(cursor, config()['cursor_secret'])
        if token.get('query') != key or token.get('release') != manifest['release']:
            raise ValueError('arXiv cursor does not match release/query')
    body['options']['scroll'] = token['scroll'] if token else True
    result = client.search(body)
    hits = result['hits']
    if hits.get('total_relation') != 'eq':
        raise RuntimeError('arXiv exact count unavailable')
    total = token['total'] if token else hits['total']
    consumed = (token['consumed'] if token else 0) + len(hits['hits'])
    hits['total'] = total
    next_cursor = None
    if consumed < total and hits['hits']:
        if not result.get('scroll'):
            raise RuntimeError('arXiv continuation missing')
        next_cursor = encode_cursor(dict(query=key, release=manifest['release'], scroll=result['scroll'],
                                        total=total, consumed=consumed,
                                        expires=token['expires'] if token else time.time()+config()['cursor_ttl']), config()['cursor_secret'])
    return hits, next_cursor
