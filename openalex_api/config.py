from pathlib import Path
import json, os, re, secrets, tempfile, time, logging

CODE_ROOT = Path(__file__).resolve().parents[1]
RESOURCE_ROOT = Path(__file__).resolve().parent / "resources"
ROOT = Path(os.environ.get("OPENALEX_HOME", Path.cwd())).expanduser().resolve()

ENTITIES = (
    "works authors institutions sources publishers funders awards topics keywords concepts domains fields subfields countries continents languages licenses institution-types source-types work-types sdgs"
).split()
DEFAULT = dict(
    snapshot=str(ROOT.parent / "openalex-snapshot"),
    host="0.0.0.0",
    port=8088,
    ch_url="http://127.0.0.1:18123",
    os_url="http://127.0.0.1:19200",
    cpus=min(8, os.cpu_count() or 1),
    memory_gib=16,
    disk_budget_gib=None,
    min_free_gib=1,
    execution_profile="community",
    concurrency=0,  # Zero disables caller concurrency rejection.
    qps=0,  # Zero disables per-key rate limiting.
    burst=10,
    api_workers=1,
    export_workers=2,
    catalog_workers=8,
    catalog_batch_rows=10000,
    export_batch_rows=1000,
    export_shard_rows=100000,
    query_timeout=15,
    cursor_ttl=600,
    batch_size=1000,
    opensearch_heap_gib=16,
    clickhouse_memory_gib=8,
    workers=4,
    search_engine="manticore",
    manticore_workers=4,
    manticore_max_threads_per_query=2,
    manticore_observation_seconds=0,
)


def atomic_json(path, value, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        d = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def config():
    p = ROOT / "config/local.json"
    for attempt in range(4):
        try:
            return DEFAULT | json.loads(p.read_text())
        except FileNotFoundError:
            if attempt == 3:
                raise RuntimeError("Run ./oa init first; config/local.json is unavailable") from None
            # Shared storage can briefly hide the destination of an atomic replacement.
            logging.getLogger(__name__).warning("Config read temporarily missing; retry %d/3", attempt + 1)
            time.sleep((.05, .2, .5)[attempt])


def init():
    for sub in [
        "config",
        "reports",
        "var/log",
        "var/run",
        "var/data",
        "var/generations",
        "var/engines",
        "var/downloads",
    ]:
        (ROOT / sub).mkdir(parents=True, exist_ok=True)
    p = ROOT / "config/local.json"
    if not p.exists():
        atomic_json(
            p,
            DEFAULT
            | dict(
                api_keys=[secrets.token_urlsafe(32)],
                cursor_secret=secrets.token_hex(32),
                ch_admin_password=secrets.token_urlsafe(24),
                ch_reader_password=secrets.token_urlsafe(24),
            ),
        )
    return p


def generation_name(value):
    if not re.fullmatch("[a-z][a-z0-9_]{0,47}", value):
        raise ValueError("generation must match [a-z][a-z0-9_]{0,47}")
    return value


def generation_path(value):
    return ROOT / "var/generations" / generation_name(value)


def read_generation(value):
    return json.loads((generation_path(value) / "generation.json").read_text())


def active():
    p = ROOT / "var/active.json"
    if not p.exists():
        raise RuntimeError(
            "No published generation; import, validate and publish first"
        )
    return read_generation(json.loads(p.read_text())["generation"])


def search_is_active(g):
    pointer = ROOT / "var/active.json"
    return (pointer.exists() and json.loads(pointer.read_text())["generation"] == g
            and not read_generation(g).get("metadata_only", False))
