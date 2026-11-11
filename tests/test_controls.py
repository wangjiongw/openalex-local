import asyncio, concurrent.futures, time
import pytest
from fastapi.testclient import TestClient
from openalex_api import api, manage
from openalex_api.config import config


def test_unicode_query_key_is_rejected_without_an_authentication_error():
    with TestClient(api.app) as c:
        response = c.get("/works/W1", params={"api_key": "无效密钥"})
    assert response.status_code == 401


def test_rate_and_docs(monkeypatch):
    cfg = config() | {"qps": 0.001, "burst": 1}
    monkeypatch.setattr(api, "config", lambda: cfg)
    api.limiter.clear()
    with TestClient(api.app, headers={"X-API-Key": cfg["api_keys"][0]}) as c:
        assert c.get("/capabilities").status_code == 200
        assert c.get("/capabilities").status_code == 429
        assert c.get("/docs").status_code == 200
        schema = c.get("/openapi.json").json()
        assert (
            schema["components"]["securitySchemes"]["APIKeyHeader"]["name"]
            == "X-API-Key"
        )
        assert c.get("/healthz").status_code == 200


def test_zero_disables_quotas_without_disabling_authentication(monkeypatch):
    cfg = config() | {"qps":0,"burst":0,"concurrency":0}
    monkeypatch.setattr(api,"config",lambda:cfg)
    api.limiter.clear()
    original = api.app.router.routes[:]

    @api.app.get("/__test_unlimited")
    async def work():
        await asyncio.sleep(0.05)
        return {"ok":True}

    try:
        with TestClient(api.app,headers={"X-API-Key":cfg["api_keys"][0]}) as c:
            assert c.get("/__test_unlimited",headers={"X-API-Key":"wrong"}).status_code==401
            with concurrent.futures.ThreadPoolExecutor(24) as pool:
                responses=list(pool.map(lambda _:c.get("/__test_unlimited"),range(48)))
            assert all(r.status_code==200 for r in responses)
            assert api.limiter=={} and api.inflight==0
    finally:
        api.app.router.routes[:]=original


@pytest.mark.parametrize("validation",[False,True])
def test_api_workers_apply_to_production_and_pinned_validation(monkeypatch,validation):
    cfg=config() | {"qps":0,"concurrency":0,"api_workers":4}
    monkeypatch.setattr(manage,"config",lambda:cfg)
    cmd,env=manage.command("validation-api-full_test" if validation else "api",[])
    assert cmd[cmd.index("--workers")+1]=="4"
    assert cmd[cmd.index("--host")+1]==("127.0.0.1" if validation else cfg["host"])
    assert cmd[cmd.index("--port")+1]==("18088" if validation else str(cfg["port"]))
    if validation:assert env["OPENALEX_GENERATION"]=="full_test"


@pytest.mark.parametrize("settings",[{"qps":5},{"concurrency":10},{"api_workers":0}])
def test_worker_settings_cannot_silently_multiply_caller_limits(monkeypatch,settings):
    cfg=config() | {"qps":0,"concurrency":0,"api_workers":4} | settings
    monkeypatch.setattr(manage,"config",lambda:cfg)
    with pytest.raises(ValueError):manage.command("api",[])


def test_database_quota_is_disabled_and_memory_guards_remain(tmp_path,monkeypatch):
    import xml.etree.ElementTree as ET
    cfg=config()
    monkeypatch.setattr(manage,"ROOT",tmp_path)
    monkeypatch.setattr(manage,"config",lambda:cfg)
    (tmp_path/"config").mkdir()
    manage.engine_configs()
    server=ET.parse(tmp_path/"config/clickhouse.xml").getroot()
    users=ET.parse(tmp_path/"config/clickhouse-users.xml").getroot()
    assert server.findtext("max_concurrent_queries")=="0"
    assert server.findtext("uncompressed_cache_size")=="8589934592"
    assert int(server.findtext("max_server_memory_usage"))==cfg["clickhouse_memory_gib"]*1024**3
    assert users.findtext("profiles/reader/readonly")=="1"
    assert users.findtext("profiles/reader/use_uncompressed_cache")=="1"
    assert int(users.findtext("profiles/reader/max_memory_usage"))>0


def test_total_request_deadline(monkeypatch):
    cfg = config() | {"query_timeout": 0.03, "qps": 10000, "burst": 10000}
    monkeypatch.setattr(api, "config", lambda: cfg)
    api.limiter.clear()
    original = api.app.router.routes[:]

    @api.app.get("/__test_slow")
    async def slow():
        await asyncio.sleep(0.15)
        return {"ok": True}

    try:
        with TestClient(api.app, headers={"X-API-Key": cfg["api_keys"][0]}) as c:
            assert c.get("/__test_slow").status_code == 504
    finally:
        api.app.router.routes[:] = original


def test_concurrency_rejection(monkeypatch):
    cfg = config() | {"concurrency": 1, "qps": 10000, "burst": 10000}
    monkeypatch.setattr(api, "config", lambda: cfg)
    api.limiter.clear()
    original = api.app.router.routes[:]

    @api.app.get("/__test_concurrent")
    async def slow():
        await asyncio.sleep(0.25)
        return {"ok": True}

    try:
        with (
            TestClient(api.app, headers={"X-API-Key": cfg["api_keys"][0]}) as c,
            concurrent.futures.ThreadPoolExecutor(2) as pool,
        ):
            first = pool.submit(c.get, "/__test_concurrent")
            time.sleep(0.05)
            second = pool.submit(c.get, "/__test_concurrent")
            assert sorted(
                [first.result().status_code, second.result().status_code]
            ) == [200, 429]
    finally:
        api.app.router.routes[:] = original


def test_body_limit(monkeypatch):
    cfg = config() | {"qps": 10000, "burst": 10000}
    monkeypatch.setattr(api, "config", lambda: cfg)
    api.limiter.clear()
    with TestClient(api.app, headers={"X-API-Key": cfg["api_keys"][0]}) as c:
        r = c.post("/v1/works/batch", content=b"x" * (1024 * 1024 + 1))
        assert r.status_code == 413
        assert api.inflight == 0


def test_bounded_count_keeps_cursor_at_10000_boundary(monkeypatch):
    cfg = config() | {"qps": 10000, "burst": 10000}
    monkeypatch.setattr(api, "config", lambda: cfg)
    monkeypatch.setattr(
        api, "generation", lambda: {"generation": "test", "complete": False}
    )

    class Fake:
        def os(self, method, path, body=None):
            if "point_in_time?" in path:
                return {"pit_id": "pit"}
            assert body["from"] == 9900 and body["size"] == 100
            return {
                "hits": {
                    "total": {"value": 10000, "relation": "gte"},
                    "hits": [
                        {"_id": f"W{i}", "sort": [f"W{i}"]} for i in range(9900, 10000)
                    ],
                }
            }

        def details(self, g, e, keys):
            return {key: {"id": "https://openalex.org/" + key} for key in keys}

    monkeypatch.setattr(api, "backend", lambda: Fake())
    api.limiter.clear()
    with TestClient(api.app, headers={"X-API-Key": cfg["api_keys"][0]}) as c:
        r = c.get("/v1/works", params={"page": 100, "per_page": 100})
        assert r.status_code == 200, r.text
        assert r.json()["meta"]["next_cursor"] is not None


@pytest.mark.parametrize('required', [True, False])
@pytest.mark.parametrize('headers', [{}, {'X-API-Key':'wrong'}])
def test_explicit_auth_mode_and_openapi(monkeypatch, required, headers):
    cfg = config() | {'auth_required':required, 'qps':0, 'concurrency':0}
    monkeypatch.setattr(api, 'config', lambda:cfg)
    original = api.app.router.routes[:]

    @api.app.post('/__test_auth_mode')
    def work():
        return {'guard_active':api.DEADLINE.get() is not None}

    try:
        with TestClient(api.app) as client:
            response = client.post('/__test_auth_mode', headers=headers, json={})
            assert response.status_code == (401 if required else 200)
            if not required:
                assert response.json()['guard_active']
                assert client.post('/__test_auth_mode', content=b'x'*(1024*1024+1)).status_code == 413
            schema = client.get('/openapi.json').json()
            assert bool(schema.get('components',{}).get('securitySchemes')) == required
    finally:
        api.app.router.routes[:] = original
        api.app.openapi_schema = None


def test_anonymous_mode_preserves_rate_limits(monkeypatch):
    cfg = config() | {'auth_required':False,'qps':0.001,'burst':1,'concurrency':0}
    monkeypatch.setattr(api,'config',lambda:cfg)
    api.limiter.clear()
    original = api.app.router.routes[:]
    @api.app.get('/__test_anonymous_limit')
    def work():return {'ok':True}
    try:
        with TestClient(api.app) as client:
            assert client.get('/__test_anonymous_limit').status_code == 200
            assert client.get('/__test_anonymous_limit', headers={'X-API-Key':'ignored'}).status_code == 429
    finally:
        api.app.router.routes[:] = original
        api.limiter.clear()
