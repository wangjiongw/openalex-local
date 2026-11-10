from .config import CODE_ROOT
"""Dependencies and query protection for immutable Manticore builds."""
import hashlib
import time

import httpx
import orjson

from .config import ROOT, atomic_json, config, generation_path
from .earth_control import read_json
from .search_versions import build_path


def artifact_signature(manifest):
    fields = ("generation", "build", "projection_signature", "metadata_signature", "collection_signature", "segments")
    return hashlib.sha256(orjson.dumps({k: manifest[k] for k in fields}, option=orjson.OPT_SORT_KEYS)).hexdigest()


def parent_ready(g, build, published=False):
    from . import manage
    from .manticore_build import file_hash
    p = build_path(g, build)
    m = read_json(p / "build.json")
    observation = read_json(p / "observation.json")
    if published:
        pointer = read_json(generation_path(g) / "search-active.json")
        receipt = p / "published.json"
        recorded = receipt.exists() and read_json(receipt).get("artifact_signature") == artifact_signature(m)
        return m["state"] == "validated" and observation.get("state") == "complete" and (pointer.get("build") == build or recorded)
    if m["state"] == "validated":
        return observation.get("state") == "complete"
    report = read_json(p / "qualification/acceptance.json")
    if (m["state"] != "sealed" or observation.get("state") != "observing"
            or not report.get("passed") or time.time() - observation.get("updated", 0) > 180):
        return False
    owner = any(manage.identity(manage.state_path(prefix + build)) for prefix in
                ("verify-manticore-", "build-manticore-"))
    engine = read_json(manage.state_path("manticore"))
    return bool(owner and all(engine.get(k) == v for k, v in observation["engine_identity"].items())
                and all(file_hash(CODE_ROOT / "openalex_api" / name) == value for name, value in report["code"].items()))


def wait_parent(g, m, published=False):
    if not m.get("parent"):
        return
    p = build_path(g, m["build"])
    while True:
        parent = read_json(build_path(g, m["parent"]) / "build.json")
        if artifact_signature(parent) != m["parent_artifact_signature"]:
            raise RuntimeError("parent artifact changed")
        if parent_ready(g, m["parent"], published):
            return
        from .manticore_build import save
        save(m)
        atomic_json(p / "progress.json", dict(state="waiting_for_parent", parent=m["parent"],
                    updated=time.time(), entities=m["entities"]))
        time.sleep(30)


def continue_earth(g, m):
    from .manage import start
    from .manticore_build import GROUPS
    count = m["entities"]["works"]["count"]
    if count >= m["earth_expected"]:
        return None
    if not parent_ready(g, m["build"], published=True):
        raise RuntimeError("continuation requires the published parent")
    child = "earth_20m_mc_v1" if count < 20_000_000 else "earth_full_mc_v1"
    if m.get("physical_layout") == "native_v2":
        child = "earth_full_mc_v3" if count >= 20_000_000 else "earth_20m_mc_v3"
    child_path = build_path(g, child)
    if (child_path / "build.json").exists():
        existing = read_json(child_path / "build.json")
        if existing.get("parent") != m["build"]:
            raise RuntimeError("existing child belongs to another parent")
        observation_path = child_path / "observation.json"
        failed_observation = observation_path.exists() and read_json(observation_path).get("state") == "failed"
        if existing["state"] == "failed" or failed_observation:
            raise RuntimeError("child failed; inspect and explicitly resume it")
        if existing["state"] == "validated":
            return child
    start("build-manticore-" + child, ["build", g, child, "--parent", m["build"],
          "--groups", ",".join(GROUPS[1:]), "--qualify-after", "--publish-after", "--continue-earth"])
    if count >= 20_000_000:
        start("verify-manticore-mixed-" + m["build"], ["mixed", g, m["build"], "--child", child])
    return child


_last_guard = {}


def guard(g, m, force=False):
    key = (g, m["build"])
    pause = build_path(g, m["build"]) / "pause.json"
    while pause.exists():
        from .manticore_build import save
        save(m)
        atomic_json(build_path(g, m["build"]) / "progress.json",
                    dict(state="paused_by_mixed_load", updated=time.time(), entities=m["entities"]))
        time.sleep(10)
    if not force and time.monotonic() - _last_guard.get(key, 0) < 30:
        return
    wait_parent(g, m)
    cfg = config()
    probes = [dict(search="climate change", search_field="title")]
    if m.get("parent"):
        fixture = read_json(build_path(g, m["parent"]) / "qualification/cases.json")
        for kind in ("real-topic", "real-year-oa"):
            case = next((c for c in fixture["cases"] if c["name"].startswith(kind + ":")), None)
            if case:
                probes.append(case["params"])
    good_runs, paused = 0, False
    with httpx.Client(headers={"X-API-Key": cfg["api_keys"][0]}, timeout=cfg["query_timeout"]+5,
                      trust_env=False) as client:
        while True:
            wait_parent(g, m)
            targets = [("http://127.0.0.1:" + str(cfg["port"]), None)]
            if m.get("parent"):
                obs = read_json(build_path(g, m["parent"]) / "observation.json")
                if obs.get("state") == "observing":
                    targets.append(("http://127.0.0.1:18088", m["parent"]))
            good, worst = True, 0
            for url, expected in targets:
                for probe in probes:
                    started = time.monotonic()
                    try:
                        r = client.get(url + "/v1/works", params=dict(search_scope="earth_sciences", per_page=25,
                                       select="id,doi,title") | probe)
                        good &= r.status_code == 200
                        if expected and r.status_code == 200:
                            good &= r.json()["meta"]["search_build"] == expected
                    except (httpx.HTTPError, ValueError, KeyError):
                        good = False
                    worst = max(worst, time.monotonic()-started)
            _last_guard[key] = time.monotonic()
            if not paused and good and worst <= 2:
                return
            if not paused:
                from .manticore_build import save
                save(m)
            paused = True
            good_runs = good_runs + 1 if good and worst <= 1.5 else 0
            atomic_json(build_path(g, m["build"]) / "progress.json",
                        dict(state="resuming" if good_runs >= 5 else "paused_for_queries", updated=time.time(),
                             probe_seconds=worst, healthy_probes=good_runs, entities=m["entities"]))
            if good_runs >= 5:
                return
            time.sleep(30)


def mixed_check(g, build, child):
    """Expansion performance is independent of the completed publication gate."""
    import asyncio
    from .earth_delivery import load_cases
    from .ingest import lock
    from .search_versions import read
    path = build_path(g, build)
    child_path = build_path(g, child)
    output = path / 'qualification/mixed-load.json'
    m = read(g, build)
    fixture = read_json(path / 'qualification/cases.json')
    started = time.time()
    atomic_json(output, dict(state='waiting_for_child_writes', started=started, child=child))
    cfg = config()
    url = f"http://127.0.0.1:{cfg['port']}"
    def check():
        from .manticore_build import file_hash
        pointer = generation_path(g) / 'search-active.json'
        if read_json(pointer).get('build') != build:
            raise RuntimeError('mixed test publication was superseded')
        code = {p.name: file_hash(p) for p in (CODE_ROOT / 'openalex_api').glob('*.py')}
        value = asyncio.run(load_cases(url, fixture['cases'], fixture['records'], 10, 1800,
                            {'X-API-Key':cfg['api_keys'][0]}, 100))
        unchanged = code == {p.name: file_hash(p) for p in (CODE_ROOT / 'openalex_api').glob('*.py')}
        value.update(code=code, code_unchanged=unchanged)
        value['passed'] = bool(unchanged and read_json(pointer).get('build') == build
            and not value['errors'] and value['requests'] >= 1000
            and value['minimum_samples_passed'] and value['p95_seconds'] <= 2 and value['p99_seconds'] <= 5
            and all(v['p95_seconds'] <= 2 and v['p99_seconds'] <= 5
                    for k,v in value['by_kind'].items() if k.startswith('real-')))
        return value
    try:
        deadline = time.monotonic() + 7200
        while True:
            progress_path = child_path / 'progress.json'
            if progress_path.exists():
                progress = read_json(progress_path)
                before = progress.get('entities', {}).get('works', {}).get('count', 0)
                if before > m['entities']['works']['count']:
                    break
            if time.monotonic() >= deadline:
                raise RuntimeError('child did not start writing within two hours')
            time.sleep(10)
        atomic_json(output, dict(state='load_testing', started=started, child=child, before_count=before))
        result = check()
        after = read_json(progress_path).get('entities', {}).get('works', {}).get('count', 0)
        result.update(child=child, started=started, build_records_added=after-before, completed=time.time())
        result['passed'] &= after > before
        result['state'] = 'complete' if result['passed'] else 'failed'
        atomic_json(output, result)
        if result['passed']:
            return
        raise RuntimeError('mixed build/query performance gate failed')
    except Exception as error:
        failed = read_json(output)
        failed.update(state='failed', passed=False, error=str(error), updated=time.time())
        atomic_json(output, failed)
        atomic_json(child_path / 'pause.json', dict(reason=str(error), requested=time.time(), parent=build))
        # Wait for the writer to acknowledge the pause before attributing a retest to serving alone.
        deadline = time.monotonic() + 7200
        while True:
            progress = read_json(child_path / 'progress.json') if (child_path / 'progress.json').exists() else {}
            if progress.get('state') == 'paused_by_mixed_load':
                break
            from .manage import identity, state_path
            if not identity(state_path('build-manticore-'+child)) and not identity(state_path('build-manticore-'+child), child=True):
                break
            if time.monotonic() >= deadline:
                atomic_json(path / 'qualification/mixed-recovery.json', dict(state='pause_unconfirmed', error=str(error)))
                raise RuntimeError('writer pause unconfirmed; no rollback attempted') from error
            time.sleep(10)
        try:
            retest = check()
        except Exception as retest_error:
            atomic_json(path / 'qualification/serving-retest.json',
                        dict(state='failed', passed=False, error=str(retest_error), child_paused=True))
            raise
        retest.update(child_paused=True, mixed_error=str(error), completed=time.time())
        atomic_json(path / 'qualification/serving-retest.json', retest)
        if not retest['passed']:
            with lock(generation_path(g) / 'search-controller.lock'):
                pointer = generation_path(g) / 'search-active.json'
                active = read_json(pointer)
                if active.get('build') == build:
                    previous = read_json(path / 'publication.json').get('previous')
                    if not previous or not previous.get('build'):
                        raise RuntimeError('serving failed and no prior publication exists')
                    prior = read(g, previous['build'])
                    if prior['state'] != 'validated':
                        raise RuntimeError('prior publication is no longer validated')
                    atomic_json(pointer, previous)
                    atomic_json(path / 'qualification/mixed-recovery.json', dict(state='rolled_back',
                                previous=previous['build'], error=str(error), time=time.time()))
        else:
            atomic_json(path / 'qualification/mixed-recovery.json', dict(state='serving_retained_expansion_paused',
                        error=str(error), time=time.time()))
