"""Deterministic automatic assessment, separate from independent calibration."""
from . import version_matching as rules
from .semantics import digest

POLICY = 'automatic-bibliographic-v1'


def allows(row):
    evidence = row['evidence']
    source, target = row['source'], row['target']
    title = source['normalized']
    identity = bool(evidence['shared_orcids'] or len(evidence['shared_author_ids']) >= 2 or
                    len(source['authors']) == len(target['authors']) == len(evidence['shared_author_ids']) == 1)
    return bool(rules.proposed(evidence) and row.get('unique_anchor') and not row.get('known_pair')
                and evidence['exact_title'] and 0 <= evidence['year_distance'] <= 3
                and len(title) >= 20 and len(title.split()) >= 4 and identity)


def prepare(manifest):
    return dict(mode='automatic_rules', policy=POLICY, independent_review=False,
                precision_lower_95=None, state='ready_for_full_assessment',
                identity=digest({k: manifest[k] for k in ('release','signature','feature_signature',
                    'candidate_signature','confirmation_signature','selected_works')}))


def require(manifest):
    report = manifest.get('automatic_review', {})
    expected = prepare(manifest)
    if any(report.get(k) != expected[k] for k in ('mode','policy','identity','independent_review','precision_lower_95')):
        raise RuntimeError('automatic review policy or input identity changed')
    return report
