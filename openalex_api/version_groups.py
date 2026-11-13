"""Conservative version decisions shared by the independent release and readers."""
import re

from .semantics import digest

RULE = 'arxiv-exact-pairs-v1'
NOTICE_TYPES = {'correction', 'retraction', 'erratum', 'expression-of-concern'}


def notice(raw):
    return bool(raw.get('is_retracted') or raw.get('type') in NOTICE_TYPES or re.match(
        r'^(?:correction|erratum|retraction|corrigendum)\s*[:—-]', raw.get('title') or raw.get('display_name') or '', re.I))


def seed_decision(source_id, links, works):
    confirmed = links.get('confirmed', [])
    ids = sorted({v['openalex_id'] for v in confirmed})
    pubs = sorted({v['openalex_id'] for v in confirmed if v['kind'] == 'publication'})
    evidence = dict(source_id=source_id, rule=RULE, confirmed=confirmed,
                    ambiguous=links.get('ambiguous', []))
    if any(k not in works for k in ids):
        raise RuntimeError('version seed target is missing')
    if any(notice(works[k]) for k in ids):
        return dict(state='review', relation_type='notice_or_retracted', members=ids, evidence=evidence)
    if len(pubs) > 1 or links.get('ambiguous'):
        return dict(state='conflict', relation_type='version_candidate', members=ids, evidence=evidence)
    if len(ids) >= 2 and len(pubs) == 1:
        return dict(state='confirmed', relation_type='preprint_publication', members=ids,
                    publication=pubs[0], evidence=evidence)
    return dict(state='unmatched', relation_type='insufficient_members', members=ids, evidence=evidence)


def groups(release, decisions):
    """Only identical-publication stars can combine; conflicting stars remain review."""
    stars, assignments, blocked = {}, {}, set()
    for d in decisions:
        if d['state'] in ('conflict', 'review'):
            blocked.update(d['members'])
        if d['state'] != 'confirmed':
            continue
        pub = d['publication']
        stars.setdefault(pub, []).append(d)
        for key in d['members']:
            assignments.setdefault(key, set()).add(pub)
    blocked.update(k for k, pubs in assignments.items() if len(pubs) > 1)
    result, conflicts = [], []
    for pub, rows in sorted(stars.items()):
        keys = sorted({k for d in rows for k in d['members']})
        evidence = [d['evidence'] for d in rows]
        if blocked.intersection(keys):
            conflicts.append(dict(state='conflict', relation_type='competing_publication_or_notice', members=keys, evidence=evidence))
            continue
        result.append(dict(group_id='vg_'+digest(dict(release=release,members=keys))[:32],
                           members=keys, preferred_openalex_id=pub, evidence=evidence,
                           relation_type='preprint_publication', state='confirmed'))
    return result, conflicts


def group_id(value):
    if not re.fullmatch(r'vg_[0-9a-f]{32}', value):
        raise ValueError('invalid version group ID')
    return value
