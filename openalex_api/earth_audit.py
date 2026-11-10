"""Field parity, topic review material and conservative journal-policy drafts."""
import collections
import csv
import itertools
import json
import math
from pathlib import Path

from . import earth_collections as ec, earth_rules
from .backend import Backend, sql_list
from .config import atomic_json, generation_path
from .earth_control import read_json
from .earth_delivery import SEED, context, sample_exports, source_rows
from .model import abstract
from .proofs import digest

LABELS = {'earth', 'related', 'unrelated', 'insufficient'}


def journal_rank(journal):
    topics = journal.get('topic_profile') or []
    total = sum(t.get('count',0) for t in topics)
    earth = sum(t.get('count',0) for t in topics if earth_rules.tail((t.get('field') or {}).get('id')) in ('19','23'))
    return earth / max(total,1) * math.sqrt(max(0,journal.get('works_count',0)))


def prepare(g, collection, output):
    path,state,catalog,registry=context(g,collection)
    if state['state']!='complete':raise RuntimeError('final quality audit requires a complete export')
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    if (output/'audit.json').exists():raise RuntimeError('audit output exists; use a new directory or finalize existing reviews')
    samples=sample_exports(path,state,100)
    if any(sum(s['record']['group']==group for s in samples)!=100 for group in ec.GROUPS):
        raise RuntimeError('quality audit needs 100 records per group')
    b=Backend(writer=True)
    try:
        random_keys=[r['id'] for r in b.ch(f"SELECT id FROM {catalog['table']} FINAL WHERE owner!='earth_sciences' ORDER BY cityHash64(id,{SEED}) LIMIT 50 FORMAT JSONEachRow")]
        pool=[r['id'] for r in b.ch(f"SELECT id FROM {catalog['facts_table']} FINAL WHERE author_candidate=1 ORDER BY cityHash64(id,{SEED}) LIMIT 500 FORMAT JSONEachRow")]
        excluded=[r['id'] for r in b.ch(f"SELECT id FROM {catalog['table']} FINAL WHERE id IN {sql_list(pool)} AND owner!='earth_sciences' ORDER BY cityHash64(id,{SEED}) FORMAT JSONEachRow") if r['id'] not in random_keys][:50]
        if len(random_keys)!=50 or len(excluded)!=50:raise RuntimeError('negative control strata are incomplete')
        keys=[s['record']['openalex_id'].rsplit('/',1)[-1] for s in samples]+random_keys+excluded
        raw=source_rows(b,g,keys)
        members={r['id']:r for r in b.ch(f"SELECT id,reasons,owner FROM {catalog['table']} FINAL WHERE id IN {sql_list(keys)} FORMAT JSONEachRow")}
        facts={r['id']:r for r in b.ch(f"SELECT id,reasons FROM {catalog['facts_table']} FINAL WHERE id IN {sql_list(keys)} FORMAT JSONEachRow")}
        records={k:json.loads(r['raw']) for k,r in raw.items()}
        authors=sorted({earth_rules.tail((a.get('author') or {}).get('id')) for r in records.values() for a in r.get('authorships') or []})
        seeds=set()
        for offset in range(0,len(authors),1000):
            seeds.update(r['id'] for r in b.ch(f"SELECT id FROM {catalog['authors_table']} FINAL WHERE id IN {sql_list(authors[offset:offset+1000])} FORMAT JSONEachRow"))
        journals=[j for j in registry.values() if j['scope'] in ('core','cross')]
        journals+=sorted((j for j in registry.values() if j['scope']=='pending'),key=lambda j:(journal_rank(j),j['works_count'],j['id']),reverse=True)[:100]
        actual_journals=b.details(g,'sources',[j['id'] for j in journals])
    finally:b.close()
    rules=read_json(generation_path(g)/'earth-rules-v2.json');mismatches=[];items=[]
    exported={s['record']['openalex_id'].rsplit('/',1)[-1]:s for s in samples}
    for key in keys:
        record=records[key];classified=earth_rules.classify(record,rules,seeds)
        if classified['owner']!=members[key]['owner'] or classified['reasons']!=sorted(members[key]['reasons']):
            mismatches.append(dict(id=key,kind='classification',python=classified['reasons'],stored=members[key]['reasons']))
        if key in exported:
            s=exported[key];group=s['record']['group'];row=raw[key]
            projected=ec.project(dict(raw=row['raw'],version=row['version'],raw_digest=row['digest'],reasons=members[key]['reasons'] if group==ec.GROUPS[2] else facts[key]['reasons']),registry,collection,'author' if group==ec.GROUPS[2] else 'direct')
            if projected!=s['record']:mismatches.append(dict(id=key,kind='export_projection'))
        else:group='negative_context' if key in excluded else 'negative_random'
        try:text=abstract(record.get('abstract_inverted_index'))
        except ValueError:text=None
        items.append(dict(id=key,group=group,title=record.get('title') or record.get('display_name'),doi=record.get('doi'),
                          year=record.get('publication_year'),language=record.get('language'),abstract=text,
                          journal=(record.get('primary_location') or {}).get('source'),
                          authors=[(a.get('author') or {}).get('display_name') for a in record.get('authorships') or []],
                          source_version=raw[key]['version'],raw_digest=raw[key]['digest'],reasons=members[key]['reasons'],
                          review_label=None,reviewer=None,human_reviewed=False))
    csv_files=collections.defaultdict(dict)
    for s in samples:csv_files[s['file'].replace('.jsonl.gz','.csv')][s['row']]=s['record']
    for file,positions in csv_files.items():
        with (path/file).open(encoding='utf-8',newline='') as f:
            found=0
            for pos,flat in enumerate(itertools.islice(csv.DictReader(f),max(positions)+1)):
                if pos in positions:
                    expected=ec.csv_row(positions[pos]);found+=1
                    if any(flat[k] != ('' if expected[k] is None else str(expected[k])) for k in ec.CSV_FIELDS):
                        mismatches.append(dict(id=positions[pos]['openalex_id'],kind='csv_projection'))
            if found!=len(positions):raise RuntimeError('CSV sample is truncated')
    coverage=[]
    for j in journals:
        actual=actual_journals.get(j['id'],{});identities={actual.get('issn_l'),*(actual.get('issn') or [])}-{None}
        expected={j.get('issn_l'),*(j.get('issn') or [])}-{None}
        identity_ok=bool(actual and actual.get('type')=='journal' and expected and expected & identities)
        coverage.append(dict(id=j['id'],name=j['name'],current_scope=j['scope'],issn_l=j.get('issn_l'),issn=j.get('issn'),
                             works_count=j['works_count'],earth_profile_priority=journal_rank(j),identity_verified=identity_ok,
                             homepage_url=j.get('homepage_url'),existing_evidence=j.get('evidence'),
                             decision='pending',scope_summary=None,evidence_url=None))
    (output/'review-items.jsonl').write_text(''.join(json.dumps(x,ensure_ascii=False)+'\n' for x in items))
    atomic_json(output/'journal-coverage.json',coverage)
    atomic_json(output/'audit.json',dict(generation=g,collection=collection,catalog_signature=catalog['signature'],collection_signature=state['signature'],
                export_blocks=state['blocks'],seed=SEED,sample_count=len(items),strata=dict(collections.Counter(x['group'] for x in items)),
                structural_passed=not mismatches,mismatches=mismatches,topic_review_complete=False,human_reviewed=False,
                journal_identity_issues=[j['id'] for j in coverage if not j['identity_verified']],
                limitations=['Sampling and Python/SQL parity do not establish semantic precision or full-corpus recall.','Random and context-selected controls have different sampling probabilities.']))
    return dict(path=str(output),structural_passed=not mismatches,mismatches=len(mismatches),samples=len(items),journals=len(coverage))


def finalize(output, reviews, journal_reviews):
    output=Path(output);report=read_json(output/'audit.json')
    items=[json.loads(line) for line in (output/'review-items.jsonl').read_text().splitlines()]
    labels=read_json(Path(reviews));decisions=read_json(Path(journal_reviews));coverage=read_json(output/'journal-coverage.json')
    if len(labels)!=len({r['id'] for r in labels}) or {r['id'] for r in labels}!={r['id'] for r in items}:
        raise ValueError('topic review must cover each sampled ID exactly once')
    by_id={r['id']:r for r in labels};strata=collections.defaultdict(collections.Counter)
    for item in items:
        review=by_id[item['id']]
        if review.get('label') not in LABELS or not review.get('evidence') or review.get('reviewer_type') not in ('agent','human'):
            raise ValueError('invalid topic review')
        strata[item['group']][review['label']]+=1
    if len(decisions)!=len({r['id'] for r in decisions}) or {r['id'] for r in decisions}!={r['id'] for r in coverage}:
        raise ValueError('journal review must cover each selected Source ID exactly once')
    known={r['id']:r for r in coverage};draft=read_json(ec.POLICY);additions=[];overrides={}
    for decision in decisions:
        j=known[decision['id']]
        if decision.get('scope') not in ('core','cross','pending','exclude') or not decision.get('rationale'):
            raise ValueError('invalid journal decision')
        changed=decision['scope']!=j['current_scope']
        if changed and decision['scope'] in ('core','cross'):
            if not j['identity_verified'] or not (decision.get('url') or '').startswith('https://'):
                raise ValueError('new journal scope needs stable identity and an official HTTPS reference')
        if changed and j['current_scope']!='pending':
            overrides[j['id']]=dict(name=j['name'],scope=decision['scope'],previous_scope=j['current_scope'],
                                    issn=sorted({j.get('issn_l'),*(j.get('issn') or [])}-{None}),
                                    url=decision.get('url'),rationale=decision['rationale'])
        if changed and j['current_scope']=='pending' and decision['scope'] in ('core','cross'):
            entry=dict(name=j['name'],scope=decision['scope'],issn=sorted({j['issn_l'],*(j.get('issn') or [])}-{None}),
                       url=decision['url'],scope_summary=decision['rationale'],reviewed_date='2026-11-05')
            if not any(ec.name_key(x['name'])==ec.name_key(entry['name']) for x in draft['journals']):
                draft['journals'].append(entry);additions.append(j['id'])
            else:
                overrides[j['id']]=dict(entry,previous_scope='pending')
    draft.update(version=2,base_policy_version=draft['version'],source_scope_overrides=overrides,
                 description='Draft only: reviewed additions and Source-specific corrections; requires a version-2 loader. Never activate with the frozen version-1 exporter.')
    atomic_json(output/'earth-journal-scopes-v2.draft.json',draft)
    atomic_json(output/'content-reviews.json',labels);atomic_json(output/'journal-reviews.json',decisions)
    report.update(topic_review_complete=True,topic_review_counts={k:dict(v) for k,v in strata.items()},
                  human_reviewed=all(x['reviewer_type']=='human' for x in labels),journal_review_count=len(decisions),
                  draft_additions=additions,draft_source_overrides=list(overrides),topic_reviews_signature=digest(labels),journal_reviews_signature=digest(decisions))
    atomic_json(output/'audit.json',report)
    lines=['# 地球科学文献抽查与期刊覆盖审查','',f"文献抽查 {len(items)} 篇；字段/分组一致性：{'通过' if report['structural_passed'] else '失败'}。",'',
           '主题审阅结果按抽样层分别统计，不据此声称全库召回率；agent 审阅不计为人工审阅。','',
           '| 抽样层 | 地学 | 相关交叉 | 无关 | 证据不足 |','|---|---:|---:|---:|---:|']
    for group,c in strata.items():lines.append('| '+group+' | '+' | '.join(str(c[k]) for k in ('earth','related','unrelated','insufficient'))+' |')
    lines+=['',f'审查期刊 {len(decisions)} 个 Source ID；新版草案新增 {len(additions)} 个已消歧来源，记录 {len(overrides)} 个 Source ID 级修正。当前 collection 和规则未修改。','',
            '逐篇证据见 content-reviews.json；期刊判定与参考链接见 journal-reviews.json。',
            '草案 schema version=2，须由未来版本显式实现 source_scope_overrides；当前 v1 导出器会拒绝加载。本轮不启动重导出。']
    (output/'REPORT.md').write_text('\n'.join(lines)+'\n')
    return report
