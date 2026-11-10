"""Each proof declares data, engine or implementation dependencies independently."""

import ast
import hashlib
import json
from .config import ROOT, config, read_generation


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def schema_hash():
    tree = ast.parse((ROOT / "openalex_api/backend.py").read_text())
    method = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "create"
    )
    return digest(ast.dump(method, include_attributes=False))


def semantic(g):
    return digest(
        dict(
            manifest=read_generation(g)["manifest_sha256"],
            model=file_hash(ROOT / "openalex_api/model.py"),
            semantics=file_hash(ROOT / "openalex_api/semantics.py"),
            schema=schema_hash(),
            rules="canonical-json-sorted-sha256;ordinal*1e9+line;external_ids;set-referenced_works;source-position-v1",
        )
    )


def execution():
    c = config()
    return digest(
        dict(
            code={
                p.name: file_hash(p)
                for p in sorted((ROOT / "openalex_api").glob("*.py"))
            },
            tuning={
                k: v
                for k, v in c.items()
                if not any(x in k for x in ("password", "secret", "api_keys"))
            },
        )
    )


def search_epoch(g):
    from .backend import Backend
    from .search_build import index_epoch

    b = Backend(writer=True)
    try:
        return index_epoch(b, g)
    finally:
        b.close()


def data_identity(g):
    from .backend import Backend, database, sql_string

    b = Backend(writer=True)
    try:
        tables = b.ch(
            f"SELECT name,uuid FROM system.tables WHERE database={sql_string(database(g))} "
            "AND name IN ('entities','audit','identifiers','references_in','references_out') "
            "ORDER BY name FORMAT JSONEachRow"
        )
        return {"generation": g, "endpoint": digest(b.cfg["ch_url"]), "tables": tables}
    finally:
        b.close()


def dependencies(g, name):
    result = {"semantic": semantic(g)}
    if name in (
        "metadata-audit", "metadata-complete", "gate-metadata", "gate-search",
        "storage-verification", "search-complete", "search-audit",
        "final-index-check", "api-validation",
    ):
        result["data_identity"] = data_identity(g)
    if name in (
        "storage-verification",
        "offline-index-check",
        "final-index-check",
        "archive",
        "archive-stable",
    ):
        result["engines"] = file_hash(ROOT / "config/engines.lock.json")
        jdk = ROOT / "var/engines/opensearch-3.3.2/jdk/release"
        result["jdk"] = file_hash(jdk) if jdk.exists() else None
        result["checkers"] = {
            x: file_hash(ROOT / "scripts" / x)
            for x in ("ReadOnlyIndexCheck.java", "CaptureAgent.java")
        }
        result["storage_code"] = file_hash(ROOT / "openalex_api/storage.py")
        from .storage import runtime_options

        result["jvm_runtime_options"] = runtime_options()
        if name == "storage-verification":
            result["phase_code"] = {name: file_hash(ROOT / "openalex_api" / name)
                                    for name in ("storage_finish.py", "storage_repair.py", "continuous_storage.py", "manage.py")}
            result["merge_capture_code"] = file_hash(ROOT / "scripts/MergeFailureCapture.java")
            result["tested_storage"] = {
                "allow_mmap": False,
                "store": "niofs",
                "translog": "request",
                "check_threads": 1,
            }
        elif name in ("offline-index-check", "final-index-check"):
            profile = ROOT / "var/storage-profile.json"
            result["storage_profile"] = file_hash(profile) if profile.exists() else None
    if name == "recovery-tests":
        result["code"] = {
            p.name: file_hash(p) for p in sorted((ROOT / "openalex_api").glob("*.py"))
        }
    if name == "preflight":
        result["execution"] = execution()
    if name in ("search-complete", "search-audit", "gate-search"):
        result["search_code"] = file_hash(ROOT / "openalex_api/search_build.py")
    if name in (
        "search-complete",
        "search-audit",
        "final-index-check",
        "api-validation",
    ):
        result["search_epoch"] = search_epoch(g)
    if name == "api-validation":
        result["implementation"] = {
            name: file_hash(ROOT / name)
            for name in (
                "openalex_api/api.py",
                "openalex_api/query.py",
                "openalex_api/official.py",
                "openalex_api/official_text.py",
                "openalex_api/manage.py",
                "scripts/benchmark.py",
            )
        }
        result["qos"] = {
            k: config()[k] for k in ("concurrency", "qps", "burst", "query_timeout", "api_workers")
        }
    return result


def legacy_archive_compatible(g, name, recorded, current):
    """Reuse only a proven unchanged archive implementation; do not re-sign evidence.

    Older archive receipts depended on the entire storage module, including unrelated
    soak code. Reconstruct that exact module from retained source, then compare both
    archive functions and all remaining evidence dependencies.
    """
    if name not in ("archive", "archive-stable"):
        return False
    old = dict(recorded)
    new = dict(current)
    wanted = old.pop("storage_code", None)
    new.pop("storage_code", None)
    if old.get("jvm_runtime_options", []) != new.get("jvm_runtime_options", []):
        return False
    old.setdefault("jvm_runtime_options", [])
    new.setdefault("jvm_runtime_options", [])
    if old != new or not wanted:
        return False
    def functions(path):
        tree = ast.parse(path.read_text())
        selected = {node.name: ast.dump(node, include_attributes=False) for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name in ("archive", "probe_source_open")}
        return selected if len(selected) == 2 else None
    current_functions = functions(ROOT / "openalex_api/storage.py")
    if current_functions is None:
        return False
    for path in (ROOT / "var/generations" / g).glob("before-*/openalex_api/storage.py"):
        if file_hash(path) == wanted and functions(path) == current_functions:
            return True
    return False
