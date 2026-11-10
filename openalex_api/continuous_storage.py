"""One independent, durable storage qualification; immutable prior evidence is retained."""
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import orjson
import psutil
from . import ingest, manage, recovery, resources, storage
from .backend import Backend, database, index, sql_string, sql_list
from .build import mark
from .config import ROOT, atomic_json, config, generation_path
from .storage_finish import require_records
from .storage_repair import exclusive, check_one, commit_manifest, move_once

G = 'full_20260908'
BASE = ROOT / 'reports/continuous-storage-20260915'
SOURCE = ROOT / 'reports/merge-debug-20260914/live-v2/live-default'
EXTENSION = SOURCE.parent / 'extension/progress.json'
SOURCE_INDEX_UUID = "rsrBsssfQt2UQJo_uZ0KHw"
PORT = 19210
MIN_RECORDS = 20_000_000
MIN_SECONDS = 86400
EXPOSURE = 2_028_000
stop_requested = False


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8 * 1024**2), b''):
            h.update(chunk)
    return h.hexdigest()



def expected_capture_options(base):
    return ['--add-exports=java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED',
            '--add-exports=java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED',
            '-javaagent:'+str(base/'merge-capture.jar'),
            '-Doa.merge.capture='+str(base/'merge-failures')]


def qualified_capture_options():
    """Keep the tested observer installed in downstream search; never silently drop it."""
    path=generation_path(G)/'storage-verification.json'
    receipt=json.loads(path.read_text()) if path.exists() else {}
    if not (receipt.get('passed') and receipt.get('merge_capture')):
        return []
    recovery.require_evidence(G,'storage-verification')
    base=Path(receipt['qualification_path']).parent if receipt.get('qualification_path') else BASE
    if receipt.get('qualification_path') and not base.resolve().is_relative_to((ROOT/'reports').resolve()):
        raise RuntimeError('qualification path is outside project reports')
    agent=base/'merge-capture.jar'
    if not agent.exists() or digest(agent)!=receipt.get('capture_agent_sha256'):
        raise RuntimeError('qualified merge capture agent changed')
    return expected_capture_options(base)


def workload_passed(s):
    return (s['unique'] >= MIN_RECORDS and s.get('continuous_seconds', 0) >= MIN_SECONDS
            and s.get('continuous_records', 0) >= EXPOSURE and s.get('updates', 0) > 0)


def require_source(prior, extended):
    if (prior.get('attempt') != '919ccc03c83a40e9a3bcbeb8afe934fe'
            or prior.get('unique') != 3276000 or not prior.get('failure')):
        raise RuntimeError('unexpected failed predecessor')
    if not (extended.get('state') == 'complete' and extended.get('total') == EXPOSURE
            and extended.get('physical_passed') and extended.get('formal_proofs_unchanged')
            and set(extended.get('physical', {})) == {a+'-'+str(i) for a in ('default','c1') for i in range(4)}):
        raise RuntimeError('incomplete paired diagnostic evidence')
    for arm in extended['arms'].values():
        if arm.get('confirmed') != EXPOSURE or arm.get('stop_pending') or arm.get('exit_code') != 143:
            raise RuntimeError('diagnostic shutdown/acknowledgment not confirmed')
    return prior['unique'] + extended['total'], extended['last_id']



def source_shards(source, expected):
    # OpenSearch retains internal indexes alongside the works index.
    paths = sorted((source / 'data/nodes/0/indices' / expected).glob('*/index'))
    if {p.parent.name for p in paths} != {'0','1','2','3'}:
        raise RuntimeError('expected four inherited works shards')
    return paths


def compare_reverse(hits, rows, after):
    """Exact reverse membership AND version check, including bounds checked in SQL."""
    keys = [h['_id'] for h in hits]
    if keys != sorted(set(keys)) or (keys and keys[0] <= after):
        raise RuntimeError('reverse traversal is not strictly ordered')
    if any(h.get('sort', [None])[0] != h['_id'] for h in hits):
        raise RuntimeError('indexed id differs from document identity')
    actual = {h['_id']: int(h['_version']) for h in hits}
    expected = {r['id']: int(r['version']) for r in rows}
    if actual != expected:
        error = RuntimeError('reverse ID/version difference')
        error.backend_context = {'actual': actual, 'expected': expected}
        raise error
    return keys[-1] if keys else after


class Qualification:
    def __init__(self, g=G, base=BASE):
        self.g, self.base = g, base
        self.path = base / 'progress.json'
        self.s = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.proc = None
        self.b = None
        self.name = index('storage_' + g, 'works')
        self.engine = ROOT / 'var/engines/opensearch-3.3.2'

    def save(self, **values):
        self.s.update(values, updated=time.time())
        atomic_json(self.path, self.s)
        summary = {k:self.s.get(k) for k in ('phase','state','attempt','unique','inherited','updates','continuous_seconds','continuous_records','minimum_records','minimum_seconds','startup','error','updated','production_gate_passed')}
        summary.update(progress=str(self.path), copied_files=len(self.s.get('copied',{})),
                       reconciliations=self.s.get('reconciliations',{}), physical_completed=len(self.s.get('physical',{})))
        atomic_json(generation_path(self.g)/'continuous-storage.json', summary)
        mark(self.g, 'storage-continuous-' + self.s['phase'],
             detail_state=self.s.get('state'), progress=str(self.path),
             confirmed=self.s.get('unique', 0), continuous_seconds=self.s.get('continuous_seconds', 0))

    def guard(self, **requirements):
        if stop_requested or (self.base / 'pause-requested').exists():
            raise resources.Paused('user')
        resources.checkpoint(self.g, **requirements)
        if self.proc is not None and self.proc.poll() is not None:
            raise RuntimeError('qualification engine exited unexpectedly')
        storage.assert_storage_logs_clean(self.base / 'logs')

    def prepare(self):
        from .proofs import dependencies
        self.base.mkdir(exist_ok=True)
        metadata = recovery.require_evidence(self.g, 'metadata-complete')
        current = dependencies(self.g, 'storage-verification')
        if self.s:
            if self.s['dependencies'] != current:
                raise RuntimeError('qualification dependencies changed; explicit migration required')
            if self.s.get('state') == 'failed':
                raise RuntimeError('failed qualification requires investigation')
            return
        if storage.runtime_options():
            raise RuntimeError('qualification requires default JVM')
        prior = json.loads((generation_path(self.g) / 'storage-progress.json').read_text())
        extension = json.loads(EXTENSION.read_text())
        count, upper = require_source(prior, extension)
        # All old copies must remain stopped, including a process launched outside manage.
        for p in psutil.process_iter(['name']):
            if p.info['name'] == 'java' and p.environ().get('OPENSEARCH_PATH_CONF') in (
                str(SOURCE / 'config'), str(ROOT / 'var/storage-canary/config'), str(self.base / 'config')):
                raise RuntimeError('source/target engine already running')
        for shard in source_shards(SOURCE, SOURCE_INDEX_UUID):
            if commit_manifest(shard) != extension['physical']['default-' + shard.parent.name]:
                raise RuntimeError('source commit changed after physical checks')
        for name in ('storage-progress.json', 'storage-verification.json', 'metadata-complete.json', 'archive.json'):
            shutil.copy2(generation_path(self.g) / name, self.base / ('inherited-' + name))
        atomic_json(self.base / 'inherited-extension.json', extension)
        size = sum(p.stat().st_size for p in (SOURCE / 'data').rglob('*') if p.is_file())
        # Space for growth plus one full temporary physical-check copy, with global safety margin.
        self.guard(large=True, additional=size * 5, temporary=size * 5)
        self.save(phase='copying', state='running', attempt=uuid.uuid4().hex,
                  dependencies=current, metadata_signature=metadata['signature'],
                  index_uuid=SOURCE_INDEX_UUID, inherited=count, unique=count, last_id=upper, initial_last=upper,
                  updates=0, cycles=0, continuous_seconds=0, continuous_records=0,
                  minimum_records=MIN_RECORDS, minimum_seconds=MIN_SECONDS,
                  capture_agent_sha256=digest(self.base/'merge-capture.jar'),
                  source_bytes=size, copied={}, reconciliations={}, physical={},
                  started=time.time(), production_gate_passed=False)

    def copy(self):
        if self.s['phase'] != 'copying':
            return
        for path in sorted((SOURCE / 'data').rglob('*')):
            if not path.is_file():
                continue
            self.guard()
            rel = str(path.relative_to(SOURCE / 'data'))
            dest = self.base / 'data' / rel
            stamp = [path.stat().st_size, path.stat().st_mtime_ns]
            old = self.s['copied'].get(rel)
            if old and old['stamp'] == stamp and dest.exists() and digest(dest) == old['sha256']:
                continue
            before = digest(path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            temporary = dest.with_name(dest.name + '.copying')
            shutil.copy2(path, temporary)
            if digest(temporary) != before or digest(path) != before:
                raise RuntimeError('source/copy digest mismatch')
            with temporary.open('rb') as f:
                os.fsync(f.fileno())
            temporary.replace(dest)
            self.s['copied'][rel] = {'sha256': before, 'stamp': stamp}
            self.save()
        conf = self.base / 'config'
        conf.mkdir(exist_ok=True)
        (self.base / 'logs').mkdir(exist_ok=True)
        for name in ('jvm.options', 'log4j2.properties'):
            shutil.copy2(SOURCE / 'config' / name, conf / name)
        with (conf / 'log4j2.properties').open('a') as f:
            f.write('\nlogger.merge_capture.name = org.opensearch.index.engine.OpenSearchConcurrentMergeScheduler\nlogger.merge_capture.level = trace\n')
        (conf / 'opensearch.yml').write_text(
            f'cluster.name: oa-continuous-storage\nnode.name: oa-continuous-storage\nnetwork.host: 127.0.0.1\nhttp.port: {PORT}\ntransport.port: {PORT+100}\ndiscovery.type: single-node\npath.data: {self.base}/data\npath.logs: {self.base}/logs\nplugins.security.disabled: true\nnode.processors: 4\nnode.store.allow_mmap: false\n')
        source_shards(self.base, self.s['index_uuid'])
        self.save(phase='inherited-check')

    def start(self):
        # Never adopt an unknown live process or count time across engine restarts.
        if manage.identity(self.base / 'process.json'):
            raise RuntimeError('previous qualification engine remains alive')
        session = uuid.uuid4().hex
        self.save(engine_session=session, state='running')
        log = (self.base / ('console-' + session + '.log')).open('w')
        opts = ('-Xms8g -Xmx8g -XX:ActiveProcessorCount=4 -XX:+UnlockDiagnosticVMOptions '
                '-XX:+LogCompilation -XX:LogFile=' + str(self.base / ('jit-' + session + '.xml')))
        capture = self.base / 'merge-capture.jar'
        if not capture.exists() or digest(capture)!=self.s['capture_agent_sha256']:
            raise RuntimeError('missing tested merge failure capture agent')
        opts += ' ' + ' '.join(expected_capture_options(self.base))
        try:
            self.proc = subprocess.Popen([str(self.engine / 'bin/opensearch')], cwd=ROOT,
                env=os.environ | {'OPENSEARCH_PATH_CONF': str(self.base / 'config'),
                                  'OPENSEARCH_JAVA_HOME': str(self.engine / 'jdk'), 'OPENSEARCH_JAVA_OPTS': opts},
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        finally:
            log.close()
        identity = {'pid': self.proc.pid, 'create_time': psutil.Process(self.proc.pid).create_time(), 'external_diagnostic': True}
        atomic_json(self.base / 'process.json', identity)
        atomic_json(ROOT / 'var/run/verify-continuous-engine.json', identity)
        atomic_json(manage.state_path('verify-'+self.g), {'pid':os.getpid(), 'create_time':psutil.Process().create_time(),
                    'child_pid':self.proc.pid, 'child_create_time':identity['create_time'], 'external_diagnostic':True})
        self.b = Backend(writer=True)
        self.b.cfg = dict(self.b.cfg, os_url=f'http://127.0.0.1:{PORT}')
        storage.guard_canary_bulk(self.b, self.base / 'logs')
        began = last_progress = time.monotonic()
        previous = None
        with httpx.Client(timeout=5, trust_env=False) as client:
            while True:
                self.guard()
                try:
                    reply = client.get(self.b.cfg['os_url'] + '/_cluster/health/' + self.name)
                    reply.raise_for_status()
                    h = reply.json()
                    if h.get('status') == 'green':
                        break
                    reply = client.get(self.b.cfg['os_url'] + '/' + self.name + '/_recovery?active_only=true')
                    reply.raise_for_status()
                    token = [(x.get('id'), x.get('stage'), x.get('index',{}).get('files',{}).get('recovered'),
                              x.get('index',{}).get('size',{}).get('recovered_in_bytes'), x.get('translog',{}).get('recovered'))
                             for value in reply.json().values() for x in value.get('shards',[])]
                    marker = (h.get('active_primary_shards'), h.get('initializing_shards'), token)
                    if marker != previous:
                        previous, last_progress = marker, time.monotonic()
                    self.save(startup={'health': h, 'recovery': token, 'elapsed': time.monotonic()-began})
                except httpx.HTTPError as e:
                    self.save(startup={'error': type(e).__name__, 'elapsed': time.monotonic()-began})
                if time.monotonic()-began > 2700 or time.monotonic()-last_progress > 600:
                    raise resources.Paused('startup-observation-limit')
                time.sleep(5)
        actual = self.b.os('GET', '/' + self.name + '/_settings')[self.name]['settings']['index']
        if (actual['uuid'] != self.s['index_uuid'] or actual['store']['type'] != 'niofs'
                or actual['translog']['durability'] != 'request'):
            raise RuntimeError('engine storage identity mismatch')
        console = (self.base / ('console-' + session + '.log')).read_text()
        if 'MERGE_CAPTURE_INSTALLED' not in console:
            raise RuntimeError('merge capture agent not installed')
        self.save(startup={'status': 'green', 'elapsed': time.monotonic()-began})

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.save(stop_requested_at=time.time())
            os.killpg(self.proc.pid, signal.SIGTERM)
            deadline = time.monotonic()+600
            while self.proc.poll() is None and time.monotonic()<deadline:
                time.sleep(2)
        if self.proc.poll() is None:
            self.save(stop_pending=True)
            raise resources.Paused('engine-stop-pending')
        self.save(exit_code=self.proc.returncode, stop_pending=False)
        self.proc = None
        if self.b:
            self.b.close()
            self.b = None

    def reconcile(self, label, upper, expected):
        cursor = self.s['reconciliations'].setdefault(label, {'forward_after':'', 'forward':0, 'reverse_after':'', 'reverse':0})
        while not cursor.get('forward_done'):
            self.guard()
            rows = self.b.ch(f"SELECT id,version FROM {database(self.g)}.entities FINAL WHERE entity='works' AND id>{sql_string(cursor['forward_after'])} AND id<={sql_string(upper)} AND cityHash64(id)%4=0 ORDER BY id LIMIT 4000 FORMAT JSONEachRow")
            if not rows:
                if cursor['forward'] != expected or cursor['forward_after'] != upper:
                    raise RuntimeError('incomplete acknowledged source prefix')
                cursor['forward_done'] = True
            else:
                require_records(rows, self.b.mget('storage_'+self.g, 'works', [r['id'] for r in rows]))
                cursor.update(forward_after=rows[-1]['id'], forward=cursor['forward']+len(rows))
            self.save()
        if cursor.get('reverse_done'):
            return
        self.b.os('POST', '/' + self.name + '/_refresh')
        pit = self.b.os('POST', '/' + self.name + '/_search/point_in_time?keep_alive=10m')['pit_id']
        try:
            while True:
                self.guard()
                body = {'size':1000, 'pit':{'id':pit,'keep_alive':'10m'}, 'sort':[{'id':'asc'}],
                        '_source':False, 'version':True, 'track_total_hits':False}
                if cursor['reverse_after']:
                    body['search_after'] = [cursor['reverse_after']]
                reply = self.b.os('POST', '/_search', body)
                if reply.get('timed_out') or reply.get('_shards',{}).get('failed'):
                    raise RuntimeError('incomplete reverse search response')
                hits = reply['hits']['hits']
                if not hits:
                    if cursor['reverse'] != expected or cursor['reverse_after'] != upper:
                        raise RuntimeError('incomplete reverse index prefix')
                    cursor['reverse_done'] = True
                    self.save()
                    break
                rows = self.b.ch(f"SELECT id,version FROM {database(self.g)}.entities FINAL WHERE entity='works' AND id IN {sql_list([h['_id'] for h in hits])} AND id<={sql_string(upper)} AND cityHash64(id)%4=0 FORMAT JSONEachRow")
                last = compare_reverse(hits, rows, cursor['reverse_after'])
                cursor.update(reverse_after=last, reverse=cursor['reverse']+len(hits))
                self.save()
        finally:
            try:
                self.b.os('DELETE', '/_search/point_in_time', {'pit_id':[pit]})
            except Exception:
                pass

    def capture(self, label):
        target = self.base / ('capture-' + label + '-' + str(time.time_ns()))
        target.mkdir()
        errors = {}
        if self.b:
            # Bounded timeouts on observability; no document bodies or raw data.
            with httpx.Client(timeout=8, trust_env=False) as client:
                for name, endpoint in [('nodes','/_nodes/stats/jvm,indices,thread_pool,breaker'),
                                       ('segments','/'+self.name+'/_segments'),
                                       ('merges','/'+self.name+'/_stats/merge,segments?level=shards'),
                                       ('hot-threads','/_nodes/hot_threads?threads=30')]:
                    try:
                        r=client.get(self.b.cfg['os_url']+endpoint);r.raise_for_status()
                        (target/(name+'.txt')).write_text(r.text)
                    except Exception as e:
                        errors[name]=repr(e)
        if label == 'failure' and self.proc and self.proc.poll() is None:
            for name, command in [('threads','Thread.print'),('compiler','Compiler.queue')]:
                try:
                    with (target/(name+'.txt')).open('w') as f:
                        subprocess.run([str(self.engine/'jdk/bin/jcmd'),str(self.proc.pid),command],stdout=f,stderr=subprocess.STDOUT,timeout=15)
                except Exception as e:
                    errors[name]=repr(e)
        atomic_json(target/'context.json', {'attempt':self.s.get('attempt'),'phase':self.s.get('phase'),
                    'last_confirmed':self.s.get('last_id'),'unique':self.s.get('unique'), 'errors':errors})
        return str(target)

    def workload(self):
        # Each process restart requires a fresh continuous exposure; records remain reusable.
        self.save(continuous_seconds=0, continuous_records=0, continuous_started=time.time(), state='running')
        last_capture = time.monotonic()
        with ThreadPoolExecutor(max_workers=4) as pool:
            while not workload_passed(self.s):
                start = time.monotonic()
                self.guard()
                updating = self.s['unique'] >= MIN_RECORDS
                upper = ' AND id<='+sql_string(self.s['initial_last']) if updating else ''
                rows = self.b.ch(f"SELECT id,raw,version FROM {database(self.g)}.entities FINAL WHERE entity='works' AND id>{sql_string(self.s['last_id'])}{upper} AND cityHash64(id)%4=0 ORDER BY id LIMIT 4000 FORMAT JSONEachRow")
                if not rows:
                    if not updating:
                        raise RuntimeError('source exhausted before minimum records')
                    self.save(last_id='', cycles=self.s['cycles']+1)
                    continue
                records = [(r['id'],int(r['version']),project_record(r['raw'])) for r in rows]
                # Failure after any sub-bulk leaves the common cursor unchanged for replay.
                jobs = [pool.submit(self.b.bulk,'storage_'+self.g,'works',records[i:i+1000]) for i in range(0,len(records),1000)]
                for job in jobs:
                    job.result()
                require_records(rows,self.b.mget('storage_'+self.g,'works',[r['id'] for r in rows]))
                self.guard()
                values = dict(last_id=rows[-1]['id'], continuous_records=self.s['continuous_records']+len(rows),
                              continuous_seconds=self.s['continuous_seconds']+time.monotonic()-start)
                if updating:
                    values['updates']=self.s['updates']+len(rows)
                else:
                    values['unique']=self.s['unique']+len(rows)
                    values['initial_last']=rows[-1]['id']
                    if values['unique'] >= MIN_RECORDS:
                        values['last_id']=''
                self.save(**values)
                if time.monotonic()-last_capture >= 300:
                    self.capture('periodic');last_capture=time.monotonic()
        self.save(phase='crash-prepared', workload_passed=True)

    def crash(self):
        # Intent is durable before killing only the owned isolated engine; repeat if interrupted.
        if self.s['phase']=='crash-prepared':
            self.guard()
            self.save(crash_target={'pid':self.proc.pid,'create_time':psutil.Process(self.proc.pid).create_time()}, crash_prepared=time.time())
            os.killpg(self.proc.pid,signal.SIGKILL)
            self.proc.wait(timeout=60)
            self.proc=None
            self.b.close();self.b=None
            self.save(phase='crash-issued',crash_issued=time.time())
        self.start()
        self.save(phase='post-recovery',crash_recovered=True)

    def certify(self):
        from .proofs import dependencies
        consoles=[p.read_text() for p in self.base.glob('console-*.log')]
        if (not any('MERGE_CAPTURE_TRANSFORMED' in text for text in consoles)
                or any('MERGE_CAPTURE_TRANSFORM_FAILED' in text for text in consoles)
                or digest(self.base/'merge-capture.jar')!=self.s.get('capture_agent_sha256')):
            raise RuntimeError('merge capture instrumentation was not verified')
        if self.s['dependencies'] != dependencies(self.g,'storage-verification'):
            raise RuntimeError('dependencies changed during qualification')
        c=self.s['reconciliations'].get('post-recovery',{})
        if not (workload_passed(self.s) and self.s.get('crash_recovered') and c.get('forward_done')
                and c.get('reverse_done') and c.get('forward')==c.get('reverse')==self.s['unique']
                and set(self.s['physical'])=={'0','1','2','3'}):
            raise RuntimeError('incomplete qualification; cannot issue proof')
        if manage.identity(self.base/'process.json'):
            raise RuntimeError('qualification engine remains live')
        paths = list((self.base/'data/nodes/0/indices'/self.s['index_uuid']).glob('*/index'))
        if {p.parent.name for p in paths} != {'0','1','2','3'}:
            raise RuntimeError('missing physical shard at certification')
        for shard in paths:
            if self.s['physical'].get(shard.parent.name)!=commit_manifest(shard):
                raise RuntimeError('physical epoch changed')
        # Retain failed canary and old receipt; downstream cleanup must never delete evidence.
        if manage.identity(manage.state_path('canary')) or manage.identity(manage.state_path('canary'),child=True):
            raise RuntimeError('original canary unexpectedly live')
        archive=generation_path(self.g)/'storage-attempts/failed-919ccc03-preserved'
        for key,source in [('canary',ROOT/'var/storage-canary'),('storage-progress.json',generation_path(self.g)/'storage-progress.json'),('storage-verification.json',generation_path(self.g)/'storage-verification.json')]:
            if not self.s.get("moves", {}).get(key, {}).get("complete"):
                move_once(source,archive/key,self.s,key,lambda:self.save())
        atomic_json(generation_path(self.g)/'storage-progress.json',self.s | {'validation_phase':'passed','active_seconds':self.s['continuous_seconds'],'qualification_path':str(self.path)})
        recovery.evidence(self.g,'storage-verification',passed=True,mode='niofs',unique=self.s['unique'],
                          seconds=self.s['continuous_seconds'],crash_recovered=True,physical_check=True,
                          recovery_checked=c['forward'],reverse_checked=c['reverse'],attempt=self.s['attempt'],
                          qualification_path=str(self.path), merge_capture=True,
                          capture_agent_sha256=self.s['capture_agent_sha256'])
        self.save(phase='qualified',state='complete',production_gate_passed=True)

    def run(self):
        self.prepare()
        try:
            self.copy()
            if self.s['phase'] in ('inherited-check','writing','crash-prepared','post-recovery'):
                self.start()
            if self.s['phase']=='inherited-check':
                self.reconcile('inherited',self.s['initial_last'],self.s['inherited'])
                self.save(phase='writing')
            if self.s['phase']=='writing':
                self.workload()
            if self.s['phase'] in ('crash-prepared','crash-issued'):
                self.crash()
            if self.s['phase']=='post-recovery':
                self.reconcile('post-recovery',self.s['initial_last'],self.s['unique'])
                self.b.os('POST','/'+self.name+'/_flush?wait_if_ongoing=true')
                self.stop()
                epoch={p.parent.name:commit_manifest(p) for p in (self.base/'data/nodes/0/indices'/self.s['index_uuid']).glob('*/index')}
                if set(epoch)!={'0','1','2','3'}:raise RuntimeError('missing physical shard')
                self.save(phase='physical',physical_epoch=epoch)
            if self.s['phase']=='physical':
                for shard in sorted((self.base/'data/nodes/0/indices'/self.s['index_uuid']).glob('*/index')):
                    self.guard()
                    key=shard.parent.name
                    if commit_manifest(shard)!=self.s['physical_epoch'][key]:raise RuntimeError('physical epoch changed')
                    if key not in self.s['physical']:
                        self.s['physical'][key]=check_one(self.g,shard,self.base/('physical-'+key+'.log'),[])
                        self.save()
                self.certify()
            if self.s['phase']=='qualified':
                self.guard()
                from .workflow import recover
                self.save(handoff_started=time.time())
                recover(self.g)
                self.save(phase='complete',state='complete')
                mark(self.g,'complete',complete=True)
        except BaseException as e:
            try:
                capture=self.capture('failure')
            except Exception:
                capture=None
            values={'state':'paused' if isinstance(e,resources.Paused) else 'failed','error':repr(e),
                    'backend_context':getattr(e,'backend_context',{}),'capture':capture}
            try:
                self.save(**values)
            except OSError:
                from .diagnostics import incident
                incident('storage-continuous-journal',values)
            raise
        finally:
            self.stop()
            if self.s.get('state') in ('failed','paused'):
                mark(self.g,self.s['state'],reason=self.s.get('error'),progress=str(self.path))


def project_record(raw):
    from .model import project
    return project('works',orjson.loads(raw))


@exclusive
def run(g):
    identity={'pid':os.getpid(),'create_time':psutil.Process().create_time(),'external_diagnostic':True}
    atomic_json(ROOT/'var/run/verify-continuous-controller.json',identity)
    atomic_json(manage.state_path('verify-'+g),identity)
    Qualification(g).run()


def main():
    global stop_requested
    def pause(*args):
        global stop_requested
        stop_requested=True
    signal.signal(signal.SIGTERM,pause)
    signal.signal(signal.SIGINT,pause)
    os.sched_setaffinity(0,config()['cpu_affinity'])
    run(G)


if __name__=='__main__':
    main()
