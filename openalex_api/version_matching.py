"""Explainable bibliographic candidates, never a probability or an identity rewrite."""
import re
from difflib import SequenceMatcher

from .model import canonical_id, normalize_title
from .semantics import digest
from .version_groups import notice

FEATURE_RULE = 'earth-bibliographic-features-v1'
CANDIDATE_RULE = 'title-author-blocks-v1'
POLICY_RULE = 'publication-anchor-v1'
MAX_CANDIDATES = 200
YEAR_WINDOW = 10
GRID = (0.99, 0.98, 0.97, 0.96)
NEGATIONS = {'no', 'not', 'without', 'non', 'versus', 'vs'}
STOP = {'the', 'and', 'for', 'with', 'from', 'using', 'study', 'analysis', 'based', 'model', 'models'}


def feature(row, stratum):
    a = row.get('attrs') or {}
    if isinstance(a, str):
        import json
        a = json.loads(a)
    authors = a.get('authorships') or []
    title = a.get('name_literal') or ''
    key = canonical_id(row['original_id'])
    if not key or not key.startswith('W') or int(key[1:]) != int(row['id']):
        raise ValueError('matching directory source ID mismatch')
    return dict(id=key, stratum=stratum, source_hash=row['source_hash'], title=title,
                normalized=normalize_title(title), year=int(a.get('publication_year') or 0),
                kind=a.get('type') or '', blocked=bool(notice(dict(title=title,type=a.get('type'),is_retracted=a.get('is_retracted'))) or re.match(r'^(?:withdrawn|addendum|comment on|response to|reply to)\b',title,re.I)),
                authors=sorted({canonical_id(v['author_id']) for v in authors if v.get('author_id')}),
                orcids=sorted({v['orcid'] for v in authors if v.get('orcid')}),
                names=sorted({n for v in authors for n in v.get('author_name', []) if n}),
                identifiers=sorted(a.get('external_ids') or []))


def blocks(f):
    title = f['normalized']
    result = {'t:'+title} if title else set()
    # shortcut: longest four content words approximate rare terms; expand retrieval only with measured recall evidence.
    words = sorted({w for w in title.split() if len(w) >= 5 and w not in STOP},key=lambda w:(-len(w),w))[:4]
    for author in f['authors']:
        result.update('a:'+author+':'+word for word in words)
    return sorted(result)


def evidence(a, b, methods=()):
    left, right = set(a['normalized'].split()), set(b['normalized'].split())
    shared = sorted(set(a['authors']) & set(b['authors']))
    orcids = sorted(set(a['orcids']) & set(b['orcids']))
    identity = bool(orcids or len(shared) >= 2 or (len(a['authors']) == len(b['authors']) == len(shared) == 1))
    numbers = lambda words: {v for w in words for v in re.findall(r'\d+(?:\.\d+)?', w)}
    conflicts = []
    if numbers(left) != numbers(right): conflicts.append('numeric_conflict')
    if left & NEGATIONS != right & NEGATIONS: conflicts.append('negation_conflict')
    if a['blocked'] or b['blocked']: conflicts.append('notice_or_retracted')
    if not identity: conflicts.append('weak_author_identity')
    if not a['year'] or not b['year']: conflicts.append('missing_year')
    elif abs(a['year']-b['year']) > YEAR_WINDOW: conflicts.append('year_window')
    roles = (a['kind']=='preprint') != (b['kind']=='preprint') and all(v['kind'] in {'preprint','article','proceedings-article','book-chapter'} for v in (a,b))
    if not roles: conflicts.append('publication_duplicate_or_extension')
    similarity = SequenceMatcher(None,a['normalized'],b['normalized'],autojunk=False).ratio()
    return dict(rule=POLICY_RULE,methods=sorted(set(methods)),title_similarity=similarity,
                exact_title=bool(a['normalized'] and a['normalized']==b['normalized']),
                token_overlap=len(left & right)/max(1,len(left | right)),shared_author_ids=shared,
                shared_orcids=orcids,year_distance=abs(a['year']-b['year']) if a['year'] and b['year'] else None,
                conflicts=conflicts,source_hashes={v['id']:v['source_hash'] for v in (a,b)})


def proposed(e, threshold=0.99):
    return not e['conflicts'] and e['title_similarity'] >= threshold and e['token_overlap'] >= 0.85 and e.get('candidate_margin',1.0)>=0.03


def pair_id(a, b):
    return 'vp_'+digest(sorted((a,b)))[:32]


def partition(keys, seed):
    """Call with a whole connected study component, never a single candidate edge."""
    return int(digest(dict(ids=sorted(keys),seed=seed))[:16],16) % 10


def calibrate(rows, threshold, identity):
    """The independent holdout is fixed before importing labels, not selected by accuracy."""
    if threshold not in GRID:
        raise ValueError('threshold is outside the frozen explainable grid')
    counts = dict(development=0,blind=0,hard_negative=0)
    errors = []; strata = {}
    for r in rows:
        role = r['role']; counts[role] += 1
        if r.get('label') not in {'same_study','different_study','uncertain'} or not r.get('reviewer'):
            raise ValueError('every review pair needs an independent reviewer and label')
        if role == 'development': continue
        auto = proposed(r['evidence'],threshold) and r.get('unique_anchor',False)
        if role == 'blind':
            strata[r['stratum']] = strata.get(r['stratum'],0)+1
            if not auto or r['label'] != 'same_study': errors.append(r['id'])
        elif auto and r['label'] != 'same_study': errors.append(r['id'])
    passed = counts['development'] >= 500 and counts['blind'] >= 600 and counts['hard_negative'] >= 400 and not errors
    return dict(passed=passed,rule=POLICY_RULE,identity=identity,threshold=threshold,counts=counts,errors=errors,
                allowed_strata=sorted(k for k,v in strata.items() if v>=100) if passed else [],
                precision_lower_95=0.05**(1/max(1,counts['blind'])) if passed else None)
