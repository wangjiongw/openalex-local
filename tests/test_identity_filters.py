import json

import pytest

from openalex_api import identity_filters as identity
from openalex_api.query import query


def test_pairs_keep_work_identity_and_deduplicate_repeated_authorships():
    authors = [dict(author_id='A1', institution_id=['I2', 'I2']), dict(author_id='A1', institution_id=[])]
    rows = [dict(id=3, authorships=json.dumps(authors)), dict(id=4, authorships=authors), dict(id=5, authorships=None)]
    assert identity.pairs(rows) == [dict(kind=0, entity_id=1, work_id=3), dict(kind=0, entity_id=1, work_id=4),
                                    dict(kind=1, entity_id=2, work_id=3), dict(kind=1, entity_id=2, work_id=4)]
    with pytest.raises(ValueError):
        identity.pairs([dict(id=3, authorships=[dict(author_id='W1')])])


@pytest.mark.parametrize('filters,expected', [('author:A1', [(0, 1)]), ('institution:I2', [(1, 2)]),
    ('author:A1,institution:I2', [(0, 1), (1, 2)]), ('author:!A1', []), ('author:A1|A2', []),
    ('author:A1|A2,institution:I2', [(1, 2)]), ('author:null', [])])
def test_only_mandatory_positive_identities_can_restrict_candidates(filters, expected):
    assert list(identity.mandatory(query('works', filters=filters)['query'])) == expected


class Backend:
    def __init__(self, batches):
        self.batches, self.calls = iter(batches), []
    def ch(self, sql):
        self.calls.append(sql)
        return next(self.batches)


def manifest():
    return dict(generation='demo', identity_index=dict(complete=True, table='oa_demo.identity_fixture'))


@pytest.mark.parametrize('ids,expected', [([3, 5], {'in': {'id': [3, 5]}}), ([], {'equals': {'id': 0}})])
def test_prefilter_retains_original_predicate_and_handles_no_matches(ids, expected):
    native = dict(query=dict(bool=dict(must=[{'equals': {'original_predicate': 1}}])), options=dict(threads=96))
    backend = Backend([[dict(work_id=i) for i in ids]])
    identity.apply(native, query('works', filters='author:A1,institution:I2'), manifest(), backend)
    assert native['query']['bool']['must'] == [{'equals': {'original_predicate': 1}}, expected]
    assert 'threads' not in native['options']
    assert 'kind=0 AND entity_id=1' in backend.calls[0]


def test_large_candidate_sets_are_never_silently_truncated(monkeypatch):
    monkeypatch.setattr(identity, 'MAX_CANDIDATES', 2)
    native = dict(query=dict(match_all={}), options=dict(threads=96))
    backend = Backend([[dict(work_id=i) for i in range(3)], [dict(work_id=9)]])
    identity.apply(native, query('works', filters='author:A1,institution:I2'), manifest(), backend)
    assert native['query']['bool']['must'] == [{'in': {'id': [9]}}]
    assert len(backend.calls) == 2
    native = dict(query=dict(match_all={}), options=dict(threads=96))
    identity.apply(native, query('works', filters='author:A1'), manifest(), Backend([[dict(work_id=i) for i in range(3)]]))
    assert native == dict(query=dict(match_all={}), options=dict(threads=96))


def test_old_builds_and_optional_or_conditions_do_not_require_auxiliary_data():
    native = dict(query=dict(match_all={}), options={})
    identity.apply(native, query('works', filters='author:A1'), {}, Backend([]))
    identity.apply(native, query('works', filters='author:A1|A2'), manifest(), Backend([]))
    assert native == dict(query=dict(match_all={}), options={})


def test_validation_requires_durable_complete_source_coverage(tmp_path, monkeypatch):
    import hashlib
    from openalex_api.config import atomic_json
    monkeypatch.setattr(identity, 'build_path', lambda *args: tmp_path)
    checkpoint = tmp_path / 'identity-progress.json'
    atomic_json(checkpoint, dict(signature='frozen', segments=dict(works=dict(count=2))))
    m = dict(generation='demo', build='catalogs', segments=[dict(entity='works', index='works', count=2)],
             identity_index=dict(complete=True, table='oa_demo.identity_fixture', signature='frozen', scanned_works=2,
                                 checkpoint_sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest()))
    identity.validate(Backend([[dict(result=1)]]), m)
    with pytest.raises(RuntimeError, match='missing'):
        identity.validate(Backend([[dict(result=0)]]), m)
    m['identity_index']['scanned_works'] = 1
    with pytest.raises(RuntimeError, match='coverage'):
        identity.validate(Backend([]), m)
    checkpoint.write_text('{}')
    with pytest.raises(RuntimeError, match='checksum'):
        identity.validate(Backend([]), m)


def test_lookup_sql_keeps_predicates_sort_options_and_native_scroll():
    import httpx
    calls = []
    result = dict(hits=dict(total=2, total_relation='eq', hits=[]), scroll='next', timed_out=False)
    class Client:
        class HTTP:
            def post(self, path, data):
                calls.append((path, data['query']))
                return httpx.Response(200, request=httpx.Request('POST','http://local/sql'), json=result)
        http = HTTP()
    native = dict(table='fixture', query={'bool':{'must':[{'equals':{'oa_predicate':1}},{'in':{'id':[1,2]}}]}},
                  expressions=dict(oa_predicate="ANY(x.author_id='A1' FOR x IN attrs.authorships)"),
                  sort=[{'_score':'desc'},{'id':'asc'}],limit=2,offset=0,
                  options=dict(scroll='prior',cutoff=0,field_weights=dict(name=5)))
    assert identity.search(Client(),native,True) == result
    path, sql = calls[0]
    assert path == '/sql' and 'oa_predicate=1' in sql and 'id IN (1,2)' in sql
    assert 'ORDER BY oa_weight DESC,id ASC' in sql and "scroll='prior'" in sql
    assert 'field_weights=(name=5)' in sql and '/*+ DocidIndex(id) */' in sql
    assert identity.sql_condition({'bool':{'must_not':[{'range':{'id':{'gte':1,'lt':4}}}]}}) == '((id<1 OR id>=4))'


def test_negated_multi_value_filter_excludes_every_matching_value():
    assert identity.sql_condition({'equals': {'any(filter_topic_ids)': 1}}, True) == 'all(filter_topic_ids)!=1'
