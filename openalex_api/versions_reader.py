"""Schema-aware read boundary; schema-1 signature-pinned code remains unchanged."""
from . import versions_pipeline as legacy
from .config import ROOT, active
from .earth_control import read_json


def module(release):
    m=read_json(legacy.path(release)/'release.json')
    if m.get('schema',1)==1:return legacy
    if m.get('schema')==2:
        from . import versions_full
        return versions_full
    raise RuntimeError('unsupported relationship schema')


def read(release):
    implementation=module(release)
    if implementation is not legacy:
        m=read_json(legacy.path(release)/'release.json')
        if m.get('signature')!=implementation.signature():
            from .version_resume import historical_read
            return historical_read(release)
    return implementation.read(release)


def table(m):
    return legacy.table(m)


def published(release=None):
    pointer=read_json(ROOT/'var/versions-active.json') if release is None else None
    m=read(pointer['release'] if pointer else release)
    if (pointer and pointer['signature']!=m['signature']) or m['state']!='published' or not m.get('validation',{}).get('passed') or active()['generation']!=m['generation']:
        raise RuntimeError('version relationships are unavailable for this metadata generation')
    return m


def status():
    if not (ROOT/'var/versions-active.json').exists():return legacy.status()
    try:
        m=published()
        if m.get('schema',1)==1:return legacy.status()
        from .version_progress import for_status
        return dict(progress_summary=for_status(m),ready=True,release=m['release'],generation=m['generation'],counts=m['counts'],
                    feature_coverage=m['counts'].get('features',0),matching_coverage=m['counts'].get('covered_works',m['counts'].get('matched',0)),
                    expected_works=m['expected_works'],full_earth_scan=m['validation']['full_earth_scan'],
                    calibration=m.get('calibration'),review_mode=m.get('review_mode','independent'),automatic_review=m.get('automatic_review'),candidate_scope='published text index; explicit DOI targets use imported metadata',
                    search_folding=False)
    except (OSError,RuntimeError,ValueError,KeyError,TypeError) as error:
        return dict(ready=False,error=str(error))
