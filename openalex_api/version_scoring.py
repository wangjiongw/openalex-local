"""Pure source-level scoring; database ownership stays in the coordinator."""
import json
import multiprocessing
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor


@contextmanager
def pool(workers):
    if workers == 1:
        yield None
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn')) as executor:
            yield executor


def score_source(task):
    from . import versions_full as full
    rules = full.rules
    key, f, targets, overflow, parent_map = task
    scored = []
    for target, feature, methods in targets:
        e = full.candidate_evidence(f, feature, methods)
        if e is None:
            continue
        if 'shared_doi_review' in e['methods']:
            e['conflicts'].append('duplicate_doi_requires_review')
        if overflow:
            e['conflicts'].append('candidate_overflow')
        anchor = target if f['kind'] == 'preprint' else key
        scored.append(dict(id=rules.pair_id(key, target),work_id=key,target_id=target,anchor=anchor,
                           stratum=f['stratum'],source=f,target=feature,evidence=e,state='review',
                           parent_groups=sorted({parent_map[v] for v in (key,target) if v in parent_map}),
                           known_pair=bool(parent_map.get(key) and parent_map.get(key)==parent_map.get(target))))
    eligible = [r for r in scored if rules.proposed(r['evidence'], min(rules.GRID))]
    anchors = {r['anchor'] for r in eligible}
    ranked = sorted((r['evidence']['title_similarity'] for r in scored if not r['evidence']['conflicts']), reverse=True)
    margin = ranked[0]-ranked[1] if len(ranked)>1 else 1.0
    output = []
    for r in scored:
        r['evidence']['candidate_margin'] = margin
        r['unique_anchor'] = len(anchors)==1 and r['anchor'] in anchors and not overflow
        if not r['unique_anchor']:
            r['evidence']['conflicts'].append('competing_or_unqualified_anchor')
        output.append(dict(id=key+':'+r['target_id'],payload=json.dumps(r)))
    coverage = dict(id=key,payload=json.dumps(dict(state='overflow_review' if overflow else 'processed',candidates=len(scored))))
    return output, coverage


def score_chunk(tasks):
    return [score_source(task) for task in tasks]


def score_batch(source, selected, found, methods, overflow, parent_map, executor=None):
    tasks = []
    for key, feature in source.items():
        if any(target not in found for target in selected[key]):
            raise RuntimeError('candidate target missing from pinned metadata/index')
        keys = {key, *selected[key]}
        tasks.append((key,feature,[(target,found[target],methods[(key,target)]) for target in selected[key]],
                      key in overflow,{k:parent_map[k] for k in keys if k in parent_map}))
    chunks = [tasks[i:i+32] for i in range(0,len(tasks),32)]
    results = executor.map(score_chunk,chunks) if executor else map(score_chunk,chunks)
    output, coverage = [], []
    for chunk in results:
        for rows, record in chunk:
            output.extend(rows)
            coverage.append(record)
    return output, coverage
