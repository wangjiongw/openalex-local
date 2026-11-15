import json
import re
import sqlite3
from unittest.mock import Mock

import httpx
import pytest

from openalex_api import version_matching as rules, version_review, versions_full as full
from openalex_api.semantics import digest


def fixture(tmp_path, monkeypatch, n=5000):
    monkeypatch.setattr(full, 'ROOT', tmp_path)
    monkeypatch.setattr(full, 'protect', lambda m: None)
    root = full.path('fixture')
    root.mkdir(parents=True)
    m = dict(release='fixture', signature='implementation', feature_signature='features',
             candidate_signature='candidates', confirmation_signature='confirmation', seed=42,
             selected_works=n, counts={})
    rows = []
    for i in range(n):
        a, b = f'W{i:06}', f'W{100000+i:06}'
        evidence = dict(conflicts=[] if i % 2 else ['notice'], title_similarity=1., token_overlap=1.)
        row = dict(id=rules.pair_id(a, b), work_id=a, target_id=b,
                   parent_groups=[f'group{i//1000}'] if i % 1000 < 3 else [],
                   evidence=evidence, unique_anchor=i % 13 != 0, known_pair=i % 17 == 0,
                   source=dict(id=a, title='Synthetic preprint'), target=dict(id=b, title='Synthetic article'))
        rows.append(dict(id=a+':'+b, payload=json.dumps(row)))
        if i < 30:
            reverse = row | dict(work_id=b, target_id=a, evidence=evidence | dict(conflicts=['reverse']))
            rows.append(dict(id=b+':'+a, payload=json.dumps(reverse)))
    rows.sort(key=lambda r: r['id'])
    m['counts']['pairs'] = len(rows)
    calls = []

    def pages(m, backend, columns, size, after):
        calls.append(after)
        remaining = [r for r in rows if r['id'] > after]
        for i in range(0, len(remaining), 113):
            yield [compact(r) for r in remaining[i:i+113]]

    monkeypatch.setattr(full, 'candidate_pages', pages)
    backend = Mock()
    backend.ch.side_effect = lambda sql: [r for r in rows if r['id'] in re.findall(r"'(W[^']+:W[^']+)'", sql)]
    return m, rows, backend, calls, root


def compact(r):
    p = json.loads(r['payload'])
    return dict(id=r['id'], pair_id=p['id'], work_id=p['work_id'], target_id=p['target_id'],
                parent_groups=p['parent_groups'], evidence=json.dumps(p['evidence']),
                unique_anchor=p['unique_anchor'], known_pair=p['known_pair'])


def original_selection(m, records):
    # Retained pre-fix algorithm is the equivalence oracle, including first-direction deduplication.
    rows, parents = {}, {}
    def find(k):
        parents.setdefault(k, k)
        if parents[k] != k:
            parents[k] = find(parents[k])
        return parents[k]
    for value in records:
        r = json.loads(value['payload'])
        rows.setdefault(r['id'], r)
        nodes = [r['work_id'], r['target_id']] + r['parent_groups']
        for key in nodes[1:]:
            a, c = find(nodes[0]), find(key)
            parents[max(a, c)] = min(a, c)
    components = {}
    for key in parents:
        components.setdefault(find(key), []).append(key)
    choices = {k: [] for k in version_review.QUOTAS}
    selected = set()
    for r in sorted(rows.values(), key=lambda r: digest([m['seed'], r['id']])):
        part = rules.partition(components[find(r['work_id'])], m['seed'])
        role = ('development' if part < 3 else 'blind' if part < 8 and rules.proposed(r['evidence'])
                and r['unique_anchor'] and not r['known_pair'] else 'hard_negative'
                if part >= 8 and not rules.proposed(r['evidence']) else None)
        study = find(r['work_id'])
        if study not in selected and role and len(choices[role]) < version_review.QUOTAS[role]:
            choices[role].append(r | dict(role=role, label=None, reviewer=None))
            selected.add(study)
    return [r for values in choices.values() for r in values]


def test_review_matches_original_selection_and_blinding(tmp_path, monkeypatch):
    m, rows, b, _, root = fixture(tmp_path, monkeypatch)
    expected = original_selection(m, rows)
    full.review(m, b)
    package = json.loads((root/'review-package.json').read_text())
    assert package['pairs'] == expected
    assert package['counts'] == version_review.QUOTAS
    assert package['identity'] == digest(dict(feature='features', candidate='candidates',
        confirmation='confirmation', pairs=[(r['id'], r['role'], r['evidence']) for r in expected]))
    blind = json.loads((root/'review-blind.json').read_text())
    assert all(set(r) == {'id', 'source', 'target', 'label', 'reviewer'} for r in blind['pairs'])
    assert m['counts']['review_candidates_scanned'] == len(rows)


def test_review_resumes_committed_page_after_network_failure(tmp_path, monkeypatch):
    m, rows, b, calls, root = fixture(tmp_path, monkeypatch, n=40)
    pages = full.candidate_pages
    def interrupted(*args, **kwargs):
        yield [compact(r) for r in rows[:10]]
        raise httpx.RemoteProtocolError('incomplete chunked read')
    monkeypatch.setattr(full, 'candidate_pages', interrupted)
    with pytest.raises(httpx.RemoteProtocolError):
        full.review(m, b)
    with sqlite3.connect(root/'review-staging.sqlite') as db:
        assert dict(db.execute('SELECT key,value FROM state'))['after'] == rows[9]['id']
    monkeypatch.setattr(full, 'candidate_pages', pages)
    full.review(m, b)
    assert calls == [rows[9]['id']]
    package = json.loads((root/'review-package.json').read_text())
    assert package['pairs'] == original_selection(m, rows)
    assert any(package['counts'][k] < version_review.QUOTAS[k] for k in package['counts'])


def test_review_recovers_exports_and_rejects_changed_inputs(tmp_path, monkeypatch):
    m, _, b, _, root = fixture(tmp_path, monkeypatch, n=30)
    original = version_review.atomic_json
    def fail_export(path, value):
        if path.name == 'review-blind.json':
            raise OSError('transient export failure')
        return original(path, value)
    monkeypatch.setattr(version_review, 'atomic_json', fail_export)
    with pytest.raises(OSError, match='transient'):
        full.review(m, b)
    monkeypatch.setattr(version_review, 'atomic_json', original)
    monkeypatch.setattr(full, 'candidate_pages', lambda *a, **k: pytest.fail('committed review was re-read'))
    full.review(m, b)
    assert (root/'review-blind.json').exists() and (root/'review-development.json').exists()
    with pytest.raises(RuntimeError, match='inputs changed'):
        full.review(m | dict(seed=99), b)
    (root/'review-package.json').write_text('{}')
    with pytest.raises(RuntimeError, match='committed staging'):
        full.review(m, b)


def test_candidate_pages_close_responses_before_processing_and_advance(monkeypatch):
    monkeypatch.setattr(full, 'protect', lambda m: None)
    b = Mock()
    b.ch.side_effect = [[dict(id='W1:W2', payload='{}')], [dict(id='W2:W3', payload='{}')], []]
    assert list(full.candidate_rows(dict(release='fixture'), b)) == [{}, {}]
    queries = [c.args[0] for c in b.ch.call_args_list]
    assert "id>'W1:W2'" in queries[1] and "id>'W2:W3'" in queries[2]
    assert all('LIMIT 2000' in q and '.sample FINAL' in q for q in queries)
    b.ch_stream.assert_not_called()
    b.ch.side_effect = [[dict(id='W1')], [dict(id='W1')]]
    with pytest.raises(RuntimeError, match='cursor'):
        list(full.candidate_pages(dict(release='fixture'), b))


def test_find_handles_deep_components_without_python_recursion():
    parents = {str(i): str(i-1) for i in range(1, 3000)} | {'0': '0'}
    assert version_review.find(parents, '2999') == '0'
    assert parents['2999'] == '0'


def test_completed_candidate_checkpoint_restores_counts_without_recomputing(tmp_path, monkeypatch):
    monkeypatch.setattr(full, 'ROOT', tmp_path)
    root = full.path('fixture')
    root.mkdir(parents=True)
    checkpoint = dict(after='W2', processed=2, pairs=1, overflow=0)
    (root/'candidates-progress.json').write_text(json.dumps(checkpoint))
    backend = Mock()
    backend.ch.return_value = []
    m = dict(release='fixture', counts={})
    full.candidates(m, backend)
    assert m['counts'] == dict(matched=2, pairs=1, overflow=0)
    assert not any(call.args[0].startswith('INSERT ') for call in backend.ch.call_args_list)
    assert json.loads((root/'candidates-progress.json').read_text()) == checkpoint
