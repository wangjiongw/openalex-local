"""Preserve cryptographically verified works proofs during entity-scope migration."""
import hashlib,json,os,shutil,sqlite3,time
from pathlib import Path
import xml.etree.ElementTree as ET
from openalex_api import manage,recovery,proofs
from openalex_api.backend import Backend,database,sql_string
from openalex_api.config import ROOT,generation_path,atomic_json
from openalex_api.ingest import connect
from scripts.install_throughput96 import copy_atomic

g='full_20260908';p=generation_path(g);stage=ROOT.parent/'openalex-throughput96'
assert not manage.identity(manage.state_path('recover-'+g))
assert not manage.identity(manage.state_path('build-'+g))
for file,minimum in [('full-tests.xml',115),('scope-tests-final.xml',35)]:
 suites=list(ET.parse(stage/'reports/exact-opt'/file).getroot().iter('testsuite'))
 assert sum(int(s.attrib['tests']) for s in suites)>=minimum
 assert not any(int(s.attrib.get(k,0)) for s in suites for k in ['failures','errors','skipped'])
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
base=json.loads((stage/'reports/exact-opt/tested-code.json').read_text())
changes=[f.name for f in (stage/'openalex_api').glob('*.py') if base.get(f.name)!=sha(f)]
assert set(changes)=={'exact.py','exact_fast.py'},changes
backup=p/'before-exact-scope-fix';backup.mkdir(exist_ok=True)
for rel in ['openalex_api/exact.py','openalex_api/exact_fast.py','scripts/watch_full_build.py','tests/test_exact_fast.py']:
 if not (backup/rel).exists():copy_atomic(ROOT/rel,backup/rel)
 copy_atomic(stage/rel,ROOT/rel)
for name in ['scope-tests-final.xml','scope-tests-final.log']:
 copy_atomic(stage/'reports/exact-opt'/name,ROOT/'reports/exact-opt'/name)
sem=proofs.semantic(g)
with connect(g) as ledger:
 data=[tuple(r) for r in ledger.execute('SELECT relative,metadata_line,metadata_state FROM shards WHERE selected=1 ORDER BY relative')]
 counts=tuple(ledger.execute('SELECT sum(metadata_line),sum(expected),sum(metadata_state="done"),count(*) FROM shards WHERE selected=1').fetchone())
assert counts==(618739513,618739513,2964,2964)
b=Backend();dbname=database(g)
identities=b.ch(f"SELECT name,uuid FROM system.tables WHERE database={sql_string(dbname)} AND name IN ('entities','audit') ORDER BY name FORMAT JSONEachRow")
parts=b.ch(f"SELECT table,name,rows,data_version FROM system.parts WHERE active AND database={sql_string(dbname)} AND table IN ('entities','audit') AND partition='works' ORDER BY table,name FORMAT JSONEachRow")
partition_ids={r['partition_id'] for r in b.ch(f"SELECT DISTINCT partition_id FROM system.parts WHERE active AND database={sql_string(dbname)} AND table IN ('entities','audit') AND partition='works' FORMAT JSONEachRow")}
assert len(partition_ids)==1
partition_id=next(iter(partition_ids))
migrated=[]
for old in ['7b52a06a38dfda5f2834068a','03eedb054458f6bdf7b06d16']:
 lineage=json.loads((p/f'exact-source-lineage-{old}.json').read_text());proof=lineage['proof']
 assert lineage['identities']==identities and lineage['semantic']==sem
 assert [tuple(r) for r in lineage['data_state']]==data
 def token(ps):return hashlib.sha256(json.dumps([identities,ps,proof['fields'],'id','works',data,sem],sort_keys=True).encode()).hexdigest()[:24]
 assert token(lineage['old_parts'])==old
 assert [r for r in lineage['old_parts'] if r['name'].startswith(partition_id+'_')]==parts
 new=token(parts);source=p/'exact-differences'/old;dest=p/'exact-differences'/new
 assert json.loads((source/'result.json').read_text())==proof and proof['missing']==0
 with sqlite3.connect(source/'tasks.sqlite') as db:
  stats=db.execute('SELECT count(*),sum(state="done"),sum(missing) FROM ranges').fetchone()
  assert stats==(1970,1970,0),stats
 if not dest.exists():
  temp=dest.with_name(new+'.migrating');shutil.copytree(source,temp,dirs_exist_ok=True);os.replace(temp,dest)
 atomic_json(dest/'scope-migration.json',dict(old=old,new=new,partition='works',parts=parts,verified=time.time()))
 migrated.append(dict(old=old,new=new,done=1970,missing=0))
recovery.evidence(g,'recovery-tests',passed=True,tests=115,supplemental_tests=35,report=str(ROOT/'reports/exact-opt/full-tests.xml'),supplemental_report=str(ROOT/'reports/exact-opt/scope-tests-final.xml'),source='full regression plus scoped-cache and live recovery supplemental',base_code=base,changed_modules=changes)
atomic_json(p/'exact-scope-install.json',dict(state='installed',time=time.time(),semantic=sem,source_counters=counts,migrated=migrated,code={f.name:sha(f) for f in (ROOT/'openalex_api').glob('*.py')}))
print(json.dumps(dict(migrated=migrated,source_counters=counts)))
