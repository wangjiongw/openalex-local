"""Scope-aligned progress, with expensive association counts sealed at validation."""
from .earth_control import read_json


def summary(m):
    from . import versions_full as f,versions_reader
    current=m
    published=m if m['state']=='published' else versions_reader.read(m['parent_release'])
    checkpoint=f.path(current['release'])/'candidates-progress.json'
    p=read_json(checkpoint) if checkpoint.exists() else {'processed':0}
    completed=published['counts'].get('covered_works',0)
    visited=completed+(p['processed'] if current['state']!='published' else 0)
    sealed=published.get('progress_summary') or cached(published)
    return dict(associated_records=sealed.get('associated_records'),fully_checked_records=completed,
                total_candidate_records=m['expected_works'],visited_records=visited,
                candidate_pairs=m['counts'].get('pairs',0),overflow_records=m['counts'].get('overflow',0),
                unit='unique_source_works',stage=m.get('stage'),state=m.get('state'),error=m.get('error'),updated=m.get('updated'),
                phase_progress=m.get('progress'),association_count_complete='associated_records' in sealed)


def compute(m,b):
    from . import versions_full as f
    count=int(b.ch(f'''SELECT count() n FROM {m['feature_database']}.features FINAL
      WHERE id IN (SELECT id FROM {f.table(m)}.members FINAL)
      AND id IN (SELECT id FROM {f.table(m)}.coverage FINAL)
      SETTINGS max_threads=4,max_memory_usage=8589934592 FORMAT JSONEachRow''')[0]['n'])
    checked=m['counts']['covered_works']
    if not 0<=count<=checked<=m['expected_works']:raise RuntimeError('progress scope reconciliation failed')
    return dict(associated_records=count,fully_checked_records=checked,
                              total_candidate_records=m['expected_works'],visited_records=checked,
                              unit='unique_source_works',association_count_complete=True)


def cache_identity(m):
    return {k:m[k] for k in ('release','signature','generation')}|dict(counts={k:m['counts'][k] for k in ('groups','members','covered_works')})


def cached(m):
    from . import versions_full as f
    file=f.path(m['release'])/'progress-summary.json'
    if not file.exists():return {}
    receipt=read_json(file)
    if receipt['identity']!=cache_identity(m):raise RuntimeError('progress summary identity changed')
    return receipt['summary']


def cache_summary(m,b):
    from . import versions_full as f
    result=compute(m,b)
    f.atomic_json(f.path(m['release'])/'progress-summary.json',dict(identity=cache_identity(m),summary=result))
    return result


def seal(m,b):
    m['progress_summary']=cache_summary(m,b)


def _for_status(published):
    from . import versions_full as f
    file=f.ROOT/'var/versions-job-active.json'
    if file.exists():
        pointer=read_json(file)
        current=read_json(f.path(pointer['release'])/'release.json')
        if current.get('parent_release')==published['release'] and current.get('generation')==published['generation'] and current.get('signature')==pointer['signature']==f.signature():
            return summary(current)
    if published.get('progress_summary'):return published['progress_summary']
    result=cached(published)
    return result or None


def for_status(published):
    try:return _for_status(published)
    except (OSError,ValueError,KeyError,TypeError,RuntimeError) as error:
        return dict(published.get('progress_summary') or {},progress_error=str(error))
