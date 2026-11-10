from .config import RESOURCE_ROOT
from .config import CODE_ROOT
"""Fresh DTFS qualification, independent of the preserved failed legacy attempts."""
import contextlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

import httpx
import orjson
import psutil
from .config import ROOT, ENTITIES, config, generation_path, atomic_json
from .backend import Backend, database, sql_string
from . import search_projection, search_versions, storage, resources
from .proofs import digest, file_hash
from .continuous_storage import expected_capture_options

def capture_options(base, logs=None):
    options = expected_capture_options(base)
    return [x for x in options if not x.startswith("-Doa.merge.capture=")] + ["-Doa.merge.capture=" + str((logs or base / "logs") / "merge-failures")]

from .storage_repair import check_one

MIN_RECORDS = 20_000_000
MIN_SECONDS = 86400
PORT = 19220


def dependencies():
    c = config()
    return dict(projection=search_versions.signature(),
                code={p: file_hash(CODE_ROOT / "openalex_api" / p) for p in
                      ("search_storage.py", "search_versions.py", "backend.py", "storage.py", "storage_repair.py", "manage.py", "search_scopes.py", "search_incremental.py")},
                engines=file_hash(RESOURCE_ROOT / "engines.lock.json"),
                jdk=file_hash(ROOT / "var/engines/opensearch-3.3.2/jdk/release"),
                jvm_options=file_hash(ROOT / "var/engines/opensearch-3.3.2/config/jvm.options"),
                observer=file_hash(CODE_ROOT / "scripts/MergeFailureCapture.java"),
                checker={p: file_hash(CODE_ROOT / "scripts" / p) for p in ("ReadOnlyIndexCheck.java", "CaptureAgent.java")},
                heap_gib=c["opensearch_heap_gib"],
                processors=min(c["cpus"], c.get("opensearch_processors", 16)),
                mmap=False, store="niofs", translog="request", jit="default",
                filesystem_device=ROOT.stat().st_dev)


def qualification_target(g):
    from .search_scopes import catalog_path, load, EARTH
    if catalog_path(g).exists():
        total = next((r["count"] for r in load(g)["owners"] if r["owner"] == EARTH), 0)
        if not total:
            raise RuntimeError("earth science collection is empty")
        return min(MIN_RECORDS, total)
    return MIN_RECORDS

def require_qualification(g):
    path = generation_path(g) / "search-qualification.json"
    if not path.exists():
        raise RuntimeError("new shared-storage search qualification has not passed")
    receipt = json.loads(path.read_text())
    if (not receipt.get("passed") or receipt.get("dependencies") != dependencies()
            or receipt.get("count", 0) < qualification_target(g) or receipt.get("active_seconds", 0) < MIN_SECONDS
            or not receipt.get("crash_recovered") or not receipt.get("updates")
            or not receipt.get("forward_reverse") or not receipt.get("physical")):
        raise RuntimeError("failed, incomplete or stale search storage qualification")
    from .search_scopes import catalog_path, load
    if catalog_path(g).exists():
        profile = generation_path(g) / "search-profile.json"
        if receipt.get("catalog_signature") != load(g)["signature"] or not profile.exists() or receipt.get("profile_sha256") != file_hash(profile):
            raise RuntimeError("qualification scope catalog or execution profile changed")
    base = Path(receipt["path"]).parent.resolve()
    if not base.is_relative_to((ROOT / "var/search-qualifications").resolve()):
        raise RuntimeError("qualification path outside project shared storage")
    if file_hash(base / "merge-capture.jar") != receipt["observer_sha256"]:
        raise RuntimeError("qualified observer changed")
    if receipt["metadata_signature"] != search_versions.require_metadata(g)["signature"]:
        raise RuntimeError("qualification metadata identity changed")
    return receipt


def qualified_options(g):
    path = generation_path(g) / "search-qualification.json"
    if not path.exists():
        return None
    receipt = require_qualification(g)
    return capture_options(Path(receipt["path"]).parent, ROOT / "var/log/search-v2")


def check_runtime(b):
    expected = dependencies()
    nodes = b.os("GET", "/_nodes/jvm,settings")["nodes"]
    if len(nodes) != 1:
        raise RuntimeError("qualified search profile requires a single owned node")
    node = next(iter(nodes.values()))
    args = node["jvm"]["input_arguments"]
    settings = node["settings"]
    paths = settings["path"]["data"]
    if isinstance(paths, str):
        paths = [paths]
    if len(paths) != 1:
        raise RuntimeError("qualified search profile requires one shared data path")
    data_path = Path(paths[0]).resolve()
    if not data_path.is_relative_to(ROOT / "var") or data_path.stat().st_dev != ROOT.stat().st_dev:
        raise RuntimeError("search engine data must remain on the project shared filesystem")
    if (node["jvm"]["mem"]["heap_max_in_bytes"] != expected["heap_gib"] * 1024**3
            or int(settings["node"]["processors"]) != expected["processors"]
            or str(settings["node"]["store"]["allow_mmap"]).lower() != "false"
            or any("TieredStopAtLevel" in arg for arg in args)
            or not any(arg.startswith("-javaagent:") and arg.endswith("/merge-capture.jar") for arg in args)):
        raise RuntimeError("running search engine differs from the qualified runtime")
    return dict(heap_gib=expected["heap_gib"], processors=expected["processors"],
                jvm_version=node["jvm"]["version"], arguments_sha256=digest(args), mmap=False, data_path=str(data_path))


def check_physical(g, shard, logfile):
    # DTFS can retain a writer lock after close; check a stable committed copy.
    scratch = ROOT / "var/index-check-scratch"
    scratch.mkdir(exist_ok=True)
    size = sum(p.stat().st_size for p in shard.iterdir() if p.is_file())
    resources.checkpoint(g, large=True, temporary=size)
    with storage.index_check_copy(scratch, shard, logfile) as temporary:
        return check_one(g, shard, logfile, [], copy_to=temporary / "index")


class Qualification:
    def __init__(self, g, attempt):
        self.g = g
        self.base = ROOT / "var/search-qualifications" / g / attempt
        search_versions.build_path(g, attempt)  # Validate the CLI identifier before opening paths.
        self.path = self.base / "progress.json"
        self.s = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.engine = ROOT / "var/engines/opensearch-3.3.2"
        self.proc = None
        self.b = None
        self.tables = {}
        from .search_scopes import catalog_path, load
        self.catalog = load(g) if catalog_path(g).exists() else None
        self.manifest = dict(indexes={e: "oa-qualify-" + e for e in ENTITIES})
        if self.s.get("blocks"):
            self.manifest["indexes"]["works"] = ",".join(x["index"] for x in self.s["blocks"])

    def save(self, **values):
        self.s.update(values, updated=time.time())
        critical = {"phase", "state", "engine", "runtime", "calibration", "dependencies", "error", "capture_preflight"}
        if not critical.intersection(values) and self.s.get("phase") == "writing" and (self.s.get("count",0)+self.s.get("updates",0)) % 10000:
            return
        atomic_json(self.path, self.s)
        atomic_json(generation_path(self.g) / "search-qualification-progress.json",
                    {k: self.s.get(k) for k in ("phase", "state", "count", "updates", "active_seconds", "error", "updated")}
                    | dict(path=str(self.path), passed=bool(self.s.get("passed"))))

    def guard(self):
        resources.checkpoint(self.g)
        if self.proc and self.proc.poll() is not None:
            raise RuntimeError("owned qualification engine exited")
        if not hasattr(self, "log_monitor"):
            self.log_monitor = storage.StorageLogMonitor(self.base / "logs")
        self.log_monitor.check()

    def prepare(self):
        proof = search_versions.require_metadata(self.g)
        self.base.mkdir(parents=True, exist_ok=True)
        if self.base.stat().st_dev != ROOT.stat().st_dev:
            raise RuntimeError("qualification must use the project shared filesystem")
        if self.s:
            if self.s.get("state") == "failed":
                raise RuntimeError("failed qualification is immutable; investigate before a new attempt")
            if self.s.get("catalog_signature") != (self.catalog["signature"] if self.catalog else None):
                raise RuntimeError("qualification scope changed")
            if self.s["dependencies"] != dependencies():
                raise RuntimeError("qualification dependencies changed")
            return
        for sub in ("config", "data", "logs", "classes"):
            (self.base / sub).mkdir(exist_ok=True)
        exports = ["--add-exports=java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
                   "--add-exports=java.base/jdk.internal.org.objectweb.asm.commons=ALL-UNNAMED"]
        subprocess.run([str(self.engine / "jdk/bin/javac"), *exports, "-d", str(self.base / "classes"),
                        str(CODE_ROOT / "scripts/MergeFailureCapture.java")], check=True, timeout=120)
        (self.base / "agent.mf").write_text("Premain-Class: MergeFailureCapture\n\n")
        subprocess.run([str(self.engine / "jdk/bin/jar"), "cfm", str(self.base / "merge-capture.jar"),
                        str(self.base / "agent.mf"), "-C", str(self.base / "classes"), "."], check=True, timeout=30)
        for name in ("jvm.options", "log4j2.properties"):
            shutil.copy2(self.engine / "config" / name, self.base / "config" / name)
        (self.base / "config/opensearch.yml").write_text(f"""cluster.name: oa-shared-qualification
node.name: oa-shared-qualification
network.host: 127.0.0.1
http.port: {PORT}
transport.port: {PORT + 100}
discovery.type: single-node
path.data: {self.base}/data
path.logs: {self.base}/logs
path.repo: ["{ROOT}/var/backups/opensearch"]
plugins.security.disabled: true
node.processors: {dependencies()['processors']}
node.store.allow_mmap: false
indices.memory.index_buffer_size: 15%
cluster.routing.allocation.disk.watermark.low: 700gb
cluster.routing.allocation.disk.watermark.high: 500gb
cluster.routing.allocation.disk.watermark.flood_stage: 300gb
""")
        self.save(catalog_signature=self.catalog["signature"] if self.catalog else None, target=qualification_target(self.g), phase="writing", state="running", dependencies=dependencies(),
                  metadata_signature=proof["signature"], after="", count=0, updates=0, active_seconds=0,
                  samples={}, verified={}, physical={}, observer_sha256=file_hash(self.base / "merge-capture.jar"))

    def start(self):
        prior = self.s.get("engine", {})
        if prior.get("pid") and psutil.pid_exists(prior["pid"]):
            existing = psutil.Process(prior["pid"])
            if abs(existing.create_time() - prior["create_time"]) < .01 and existing.status() != psutil.STATUS_ZOMBIE:
                raise RuntimeError("previous owned engine is still alive; inspect its identity before restarting")
        values = dependencies()
        opts = [f"-Xms{values['heap_gib']}g", f"-Xmx{values['heap_gib']}g", f"-XX:ActiveProcessorCount={values['processors']}"]
        with (self.base / ("console-" + str(time.time_ns()) + ".log")).open("w") as log:
            self.proc = subprocess.Popen([str(self.engine / "bin/opensearch")], cwd=ROOT,
                env=os.environ | dict(OPENSEARCH_PATH_CONF=str(self.base / "config"),
                                      OPENSEARCH_JAVA_HOME=str(self.engine / "jdk"),
                                      OPENSEARCH_JAVA_OPTS=" ".join(opts + capture_options(self.base))),
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        identity = dict(pid=self.proc.pid, create_time=psutil.Process(self.proc.pid).create_time(), generation=self.g)
        self.save(engine=identity)
        atomic_json(ROOT / "var/run/verify-search-engine.json", identity)
        self.b = Backend(writer=True)
        self.b.cfg = self.b.cfg | dict(os_url=f"http://127.0.0.1:{PORT}")
        with httpx.Client(timeout=3, trust_env=False) as h:
            for _ in range(300):
                self.guard()
                try:
                    if h.get(self.b.cfg["os_url"] + "/_cluster/health").json().get("status") in ("green", "yellow"):
                        nodes = h.get(self.b.cfg["os_url"] + "/_nodes/_local/process").json()["nodes"]
                        if len(nodes) != 1 or next(iter(nodes.values()))["process"]["id"] != self.proc.pid:
                            raise RuntimeError("qualification endpoint belongs to another process")
                        self.save(runtime=check_runtime(self.b))
                        self.capture_preflight()
                        return
                except (httpx.HTTPError, ValueError):
                    pass
                time.sleep(1)
        raise RuntimeError("qualification startup timed out")

    def capture_preflight(self):
        name = "oa-capture-preflight"
        head = self.b.http.head(self.b.cfg["os_url"]+"/"+name)
        if head.status_code == 200:
            self.b.os("DELETE","/"+name)
        elif head.status_code != 404:
            head.raise_for_status()
        self.b.os("PUT","/"+name,{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"value":{"type":"keyword"}}}})
        for n in range(3):
            self.b.os("PUT","/"+name+"/_doc/"+str(n)+"?refresh=true",{"value":str(n)})
        self.b.os("POST","/"+name+"/_forcemerge?max_num_segments=1")
        probe = self.base / "logs/merge-failures/agent-write-probe.txt"
        self.guard()
        if not probe.exists() or ("pid="+str(self.s["engine"]["pid"])) not in probe.read_text():
            raise RuntimeError("actual engine merge did not prove capture write access")
        self.b.os("DELETE","/"+name)
        self.save(capture_preflight=True)

    def calibrate(self):
        if not self.catalog or self.s.get("calibration"):
            return
        from .search_scopes import source_rows,EARTH
        from concurrent.futures import ThreadPoolExecutor
        profile = generation_path(self.g) / "search-profile.json"
        if profile.exists():
            saved = json.loads(profile.read_text())
            if saved.get("catalog_signature") != self.catalog["signature"]:
                raise RuntimeError("execution profile belongs to another scope catalog")
            self.save(calibration=saved["calibration"],profile_sha256=file_hash(profile),dependencies=dependencies(),profile_reused=True)
            return
        rows = source_rows(self.b,self.g,"works","",1000,dict(owner=EARTH))
        if not rows:
            raise RuntimeError("calibration source is empty")
        results = []
        old = self.b.cfg.get("search_bulk_concurrency",1)
        try:
            for concurrency in (1,2,4):
                name = "oa-calibrate-"+str(concurrency)
                self.b.os("PUT","/"+name,search_projection.mapping(4))
                candidate = dict(indexes={"works":name})
                self.b.cfg["search_bulk_concurrency"] = concurrency
                started = time.monotonic()
                with search_versions.pinned(self.g,candidate):
                    search_versions.write_documents(self.b,self.g,candidate,"works",rows)
                seconds = time.monotonic()-started
                self.b.os("POST","/"+name+"/_refresh")
                def probe(_):
                    start = time.monotonic()
                    reply = self.b.os("POST","/"+name+"/_search",{"query":{"match":{"name":"climate"}},"size":25,"_source":False})
                    self.b.details(self.g,"works",[h["_id"] for h in reply["hits"]["hits"]])
                    return time.monotonic()-start
                with ThreadPoolExecutor(max_workers=10) as pool:
                    latencies = sorted(pool.map(probe,range(30)))
                results.append(dict(concurrency=concurrency,records=len(rows),seconds=seconds,throughput=len(rows)/seconds,p95_seconds=latencies[28]))
                self.b.os("DELETE","/"+name)
        finally:
            self.b.cfg["search_bulk_concurrency"] = old
        eligible = [r for r in results if r["p95_seconds"]<=2]
        if not eligible:
            raise RuntimeError("no calibration profile meets the query latency target")
        chosen = max(eligible,key=lambda r:r["throughput"])["concurrency"]
        atomic_json(profile,dict(bulk_concurrency=chosen,calibration=results,catalog_signature=self.catalog["signature"],diagnostic_only=True))
        self.save(calibration=results,profile_sha256=file_hash(profile),dependencies=dependencies())

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.save(stop_pending=True)
                raise RuntimeError("qualification shutdown pending; owned process retained")
        self.proc = None
        if self.b:
            self.b.close()
            self.b = None

    def rows(self, entity, after, size=1000):
        from .official import source_table
        from .config import read_generation
        if entity not in self.tables:
            self.tables[entity] = source_table(self.b, read_generation(self.g), entity)
        if self.catalog and entity == "works":
            from .search_scopes import source_rows, EARTH
            return source_rows(self.b, self.g, entity, after, size, dict(owner=EARTH))
        table = self.tables[entity]
        return self.b.ch(f"SELECT id,version,raw FROM {table} WHERE entity={sql_string(entity)} AND id>{sql_string(after)} ORDER BY id LIMIT {int(size)} FORMAT JSONEachRow")

    def write(self, entity, rows):
        manifest = self.manifest
        block = None
        if self.catalog and entity == "works":
            from .search_scopes import BLOCK_ROWS
            blocks = self.s.setdefault("blocks", [])
            block = next((x for x in blocks if x["low"] < rows[0]["id"] <= x["high"]), None)
            if block is None:
                if blocks and blocks[-1]["count"] < BLOCK_ROWS:
                    block = blocks[-1]
                else:
                    block = dict(index="oa-qualify-works-" + str(len(blocks)), low=blocks[-1]["high"] if blocks else "", high="", count=0)
                    head = self.b.http.head(self.b.cfg["os_url"]+"/"+block["index"])
                    if head.status_code == 404:
                        self.b.os("PUT", "/" + block["index"], search_projection.mapping(4))
                    elif head.status_code != 200:
                        head.raise_for_status()
                    blocks.append(block)
            manifest = dict(indexes=self.manifest["indexes"] | {"works":block["index"]})
        with search_versions.pinned(self.g, manifest):
            search_versions.write_documents(self.b, self.g, manifest, entity, rows)
        if block and rows[-1]["id"] > block["high"]:
            block.update(high=rows[-1]["id"],count=block["count"] + len(rows))
        if block:
            self.manifest["indexes"]["works"] = ",".join(x["index"] for x in self.s["blocks"])

    def check_documents(self, rows):
        if not self.s.get("blocks"):
            search_versions.checked_documents(rows,search_versions.mget(self.b,self.manifest["indexes"]["works"],[r["id"] for r in rows]),"works")
            return
        checked = 0
        for block in self.s["blocks"]:
            selected = [r for r in rows if block["low"] < r["id"] <= block["high"]]
            if selected:
                checked += len(selected)
                search_versions.checked_documents(selected,search_versions.mget(self.b,block["index"],[r["id"] for r in selected]),"works")


        if checked != len(rows):
            raise RuntimeError("recovery rows fall outside acknowledged qualification blocks")

    def workload(self):
        # Restarting a writer resets continuous exposure; successful writes remain replayable.
        self.save(active_seconds=0, state="running")
        for entity in ENTITIES:
            if self.catalog and entity == "works":
                continue
            name = self.manifest["indexes"][entity]
            head = self.b.http.head(self.b.cfg["os_url"] + "/" + name)
            if head.status_code == 404:
                self.b.os("PUT", "/" + name, search_projection.mapping(4 if entity == "works" else 1))
            else:
                head.raise_for_status()
        while self.s["count"] < self.s.get("target", MIN_RECORDS) or self.s["active_seconds"] < MIN_SECONDS or self.s["updates"] < 10000:
            self.guard()
            started = time.monotonic()
            updating = self.s["count"] >= self.s.get("target", MIN_RECORDS)
            after = self.s.get("update_after", "") if updating else self.s["after"]
            rows = self.rows("works", after, 1000 if updating else min(1000,self.s.get("target", MIN_RECORDS)-self.s["count"]))
            if not rows:
                if not updating:
                    raise RuntimeError("source exhausted before qualification minimum")
                self.save(update_after="")
                continue
            if updating:
                # Updates are drawn only from the known indexed prefix, with identical source versions.
                rows = [r for r in rows if r["id"] <= self.s["after"]]
                if not rows:
                    self.save(update_after="")
                    continue
            self.write("works", rows)
            self.guard()
            if updating:
                self.save(update_after=rows[-1]["id"], updates=self.s["updates"] + len(rows),
                          active_seconds=self.s["active_seconds"] + time.monotonic() - started)
            else:
                self.save(after=rows[-1]["id"], count=self.s["count"] + len(rows),
                          active_seconds=self.s["active_seconds"] + time.monotonic() - started)
            if (self.s["count"] + self.s["updates"]) % 10000 == 0:
                self.b.os("POST", "/" + self.manifest["indexes"]["works"] + "/_refresh")
                self.b.os("POST", "/" + self.manifest["indexes"]["works"] + "/_search",
                          {"size": 25, "query": {"match": {"name": "climate change"}}, "_source": False})
        self.save(phase="crash-prepared")

    def recovery(self):
        if self.s["phase"] == "crash-prepared":
            self.guard()
            self.save(crash_target=self.s["engine"])
            self.proc.kill()
            self.proc.wait(timeout=60)
            self.proc = None
            self.b.close()
            self.b = None
            self.save(phase="crash-issued")
            self.start()
        self.b.os("POST", "/" + self.manifest["indexes"]["works"] + "/_refresh")
        after, seen = "", 0
        while seen < self.s["count"]:
            self.guard()
            rows = self.rows("works", after, min(1000, self.s["count"] - seen))
            if not rows:
                raise RuntimeError("recovery source unexpectedly short")
            self.check_documents(rows)
            after, seen = rows[-1]["id"], seen + len(rows)
        name = self.manifest["indexes"]["works"]
        if self.b.os("GET", "/" + name + "/_count")["count"] != seen:
            raise RuntimeError("recovered membership count differs from acknowledged prefix")
        # Traverse the indexed direction independently; no unbounded source ID list is retained.
        pit = self.b.os("POST", "/" + name + "/_search/point_in_time?keep_alive=10m")["pit_id"]
        after, reverse = "", 0
        try:
            while True:
                self.guard()
                body = dict(size=1000, pit=dict(id=pit, keep_alive="10m"), sort=[{"id": "asc"}],
                            _source=False, version=True, stored_fields=["projection_hash"], track_total_hits=False)
                if after:
                    body["search_after"] = [after]
                result = self.b.os("POST", "/_search", body)
                pit = result.get("pit_id", pit)
                hits = result["hits"]["hits"]
                if not hits:
                    break
                rows = self.rows("works", after, len(hits))
                search_versions.checked_documents(rows, [dict(h, found=True) for h in hits], "works")
                after, reverse = hits[-1]["_id"], reverse + len(hits)
        finally:
            with contextlib.suppress(Exception):
                self.b.os("DELETE", "/_search/point_in_time", {"pit_id": [pit]})
        if reverse != seen or after != self.s["after"]:
            raise RuntimeError("recovery reverse traversal mismatch")
        self.save(crash_recovered=True, forward_reverse=True, phase="sampling")

    def sampling(self):
        meta = search_versions.require_metadata(self.g)
        from .config import read_generation
        expected = read_generation(self.g)["expected"]
        for entity in ENTITIES:
            self.guard()
            if entity != "works":
                after, count = "", 0
                target = min(250000 if entity == "authors" else 10000, expected[entity]["records"])
                while count < target:
                    rows = self.rows(entity, after, min(1000, target - count))
                    if not rows:
                        raise RuntimeError("sample source exhausted")
                    self.write(entity, rows)
                    after, count = rows[-1]["id"], count + len(rows)
            else:
                count = self.s["count"]
            name = self.manifest["indexes"][entity]
            self.b.os("POST", "/" + name + "/_flush?wait_if_ongoing=true")
            stats = self.b.os("GET", "/" + name + "/_stats/store,docs")["_all"]["primaries"]
            self.s["samples"][entity] = dict(count=count, total=expected[entity]["records"],
                bytes=stats["store"]["size_in_bytes"], lucene_docs=stats["docs"]["count"])
            self.save()
        name = self.manifest["indexes"]["works"]
        merges = self.b.os("GET", "/" + name + "/_stats/merge")["_all"]["primaries"]["merges"]
        if not merges.get("total"):
            raise RuntimeError("qualification did not exercise segment merges")
        if self.catalog:
            snapshot = self.g.replace("_", "-") + "-" + self.base.name.replace("_", "-")
            repository = ROOT / "var/backups/opensearch/qualification"
            repository.mkdir(parents=True,exist_ok=True)
            self.b.os("PUT","/_snapshot/oa-qualification",{"type":"fs","settings":{"location":str(repository)}})
            names = [x["index"] for x in self.s["blocks"]]
            self.b.os("PUT","/" + ",".join(names) + "/_settings",{"index.blocks.write":True})
            info = self.b.os("PUT","/_snapshot/oa-qualification/"+snapshot+"?wait_for_completion=true",{"indices":",".join(names),"partial":False,"include_global_state":False})
            if info.get("snapshot",{}).get("state") != "SUCCESS" or info["snapshot"]["shards"]["failed"]:
                raise RuntimeError("qualification snapshot failed")
            self.s["snapshot"] = dict(repository="oa-qualification",name=snapshot)
        self.save(phase="physical", merge_stats=merges)

    def physical(self):
        names = [x["index"] for x in self.s.get("blocks", [])] or [self.manifest["indexes"]["works"]]
        uuids = {name:self.b.os("GET","/"+name+"/_settings")[name]["settings"]["index"]["uuid"] for name in names}
        self.save(index_uuids=uuids)
        self.stop()
        for name, uuid in uuids.items():
            paths = sorted((self.base / "data/nodes/0/indices" / uuid).glob("*/index"))
            if {p.parent.name for p in paths} != {"0", "1", "2", "3"}:
                raise RuntimeError("qualification physical shard set is incomplete")
            for shard in paths:
                self.s["physical"][name + "-" + shard.parent.name] = check_physical(self.g, shard, self.base / ("physical-" + name + "-" + shard.parent.name + ".log"))
                self.save()
        consoles = "\n".join(p.read_text() for p in self.base.glob("console-*.log"))
        if "MERGE_CAPTURE_TRANSFORMED" not in consoles or "MERGE_CAPTURE_TRANSFORM_FAILED" in consoles or "MERGE_CAPTURE_WRITE_PROBE_OK" not in consoles or "MERGE_CAPTURE_WRITE_FAILED" in consoles:
            raise RuntimeError("qualified observer was not exercised correctly")
        if self.s["dependencies"] != dependencies():
            raise RuntimeError("qualification dependencies changed during execution")
        receipt = self.s | dict(passed=True, path=str(self.path), finished=time.time())
        atomic_json(generation_path(self.g) / "search-qualification.json", receipt)
        self.save(phase="complete", state="complete", passed=True)

    def run(self):
        from .ingest import lock
        with lock(ROOT / "var/run/search-qualification-engine.lock"), lock(generation_path(self.g) / "search-qualification.lock"):
            try:
                self.prepare()
                if self.s["phase"] == "complete":
                    return self.s
                self.start()
                self.calibrate()
                with search_versions.pinned(self.g, self.manifest):
                    if self.s["phase"] == "writing":
                        self.workload()
                    if self.s["phase"] in ("crash-prepared", "crash-issued"):
                        self.recovery()
                    if self.s["phase"] == "sampling":
                        self.sampling()
                    if self.s["phase"] == "physical":
                        self.physical()
                return self.s
            except BaseException as error:
                self.save(state="paused" if isinstance(error, resources.Paused) else "failed", error=repr(error),
                          backend_context=getattr(error, "backend_context", None))
                raise
            finally:
                self.stop()
