import copy
import importlib
import json
import sqlite3
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from openalex_api import api, ingest, recovery, manage

cfg = importlib.import_module("openalex_api.config")


@pytest.fixture
def release(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "ROOT", tmp_path)
    monkeypatch.setattr(ingest, "config", lambda: {"execution_profile":"test"})
    path = tmp_path / "var/generations/full_test"
    path.mkdir(parents=True)
    source = {"manifest_sha256":"snapshot", "entities":{
        e:{"records":1,"missing":0,"size_mismatch":0} for e in cfg.ENTITIES}}
    meta = dict(generation="full_test",mode="full",pipeline_version=2,
                metadata_frozen=True,validated=False,complete=False,mutable=True,
                snapshot=str(tmp_path / "snapshot"),manifest_sha256="snapshot")
    cfg.atomic_json(path / "generation.json", meta)
    with sqlite3.connect(path / "tasks.sqlite") as db:
        db.execute("CREATE TABLE shards(entity,relative,selected,state,records,expected,metadata_state,metadata_line,error)")
        db.executemany("INSERT INTO shards VALUES(?, ?, 1, 'failed', 0, 1, 'done', 1, 'old error')",
                       [(e,e+"/source") for e in cfg.ENTITIES])
    b = Mock()
    b.ch.return_value = [{"entity":e,"n":1} for e in cfg.ENTITIES]
    b.os.side_effect = AssertionError("metadata release must not use search")
    monkeypatch.setattr(ingest, "Backend", lambda **kw: b)
    monkeypatch.setattr(ingest, "inventory", lambda snapshot: copy.deepcopy(source))
    proof = Mock(return_value={"passed":True,"signature":"semantic"})
    monkeypatch.setattr(recovery,"require_evidence",proof)
    return path, source, meta, b, proof


def test_metadata_publication_does_not_certify_search(release):
    path, _, _, b, proof = release
    result = ingest.publish("full_test", metadata_only=True)
    assert result["metadata_only"] and not result["complete"]
    assert cfg.active()["metadata_only"] and not cfg.active()["validated"]
    assert not cfg.search_is_active("full_test")
    assert proof.call_args_list[0].args == ("full_test","metadata-complete")
    assert proof.call_args_list[1].args == ("full_test","metadata-audit")
    assert json.loads((path/"metadata-release.json").read_text())["entities"]["works"]["indexed_entities"] is None
    b.os.assert_not_called()
    progress = ingest.status("full_test")
    assert all(r["state"] == "done" and r["records"] == 1 for r in progress["progress"])
    assert progress["errors"] == []


@pytest.mark.parametrize("failure",["proof","source","checkpoint","count","frozen"])
def test_metadata_publication_refuses_unverified_data(release, failure):
    path, source, meta, b, proof = release
    if failure == "proof": proof.side_effect = RuntimeError("stale evidence")
    if failure == "source": source["entities"]["works"]["size_mismatch"] = 1
    if failure == "checkpoint":
        with sqlite3.connect(path/"tasks.sqlite") as db: db.execute("UPDATE shards SET metadata_line=0 WHERE entity='works'")
    if failure == "count": b.ch.return_value = [{"entity":"works","n":2}]
    if failure == "frozen": cfg.atomic_json(path/"generation.json",meta|{"metadata_frozen":False})
    with pytest.raises(RuntimeError): ingest.publish("full_test",metadata_only=True)
    assert not (cfg.ROOT/"var/active.json").exists()


def test_metadata_release_cannot_be_normal_publication(release):
    path, _, meta, _, _ = release
    ingest.publish("full_test",metadata_only=True)
    with pytest.raises(RuntimeError,match="validate"): ingest.publish("full_test")
    cfg.atomic_json(path/"generation.json",meta|{"metadata_only":True,"validated":True,"complete":True})
    ingest.publish("full_test")
    assert "metadata_only" not in cfg.active()
    assert cfg.search_is_active("full_test")
    assert json.loads((cfg.ROOT/"var/active.json").read_text())["previous"] is None


def test_promotion_retains_original_rollback_generation(release):
    path,_,meta,_,_=release
    cfg.atomic_json(cfg.ROOT/"var/active.json",dict(generation="pilot_test",published=1,previous=None))
    pilot=cfg.ROOT/"var/generations/pilot_test";pilot.mkdir()
    ingest.publish("full_test",metadata_only=True)
    cfg.atomic_json(path/"generation.json",meta|dict(metadata_only=True,validated=True,complete=True))
    ingest.publish("full_test")
    assert json.loads((cfg.ROOT/"var/active.json").read_text())["previous"]=="pilot_test"
    assert json.loads((path/"published.json").read_text())["previous"]["generation"]=="pilot_test"


def test_active_metadata_cannot_be_cleaned_up(release):
    ingest.publish("full_test",metadata_only=True)
    with pytest.raises(RuntimeError,match="active generation"):
        ingest.cleanup("full_test","full_test")


def test_only_unpublished_search_can_be_closed_on_active_metadata(release, monkeypatch):
    from openalex_api import storage
    path,_,meta,_,_=release
    ingest.publish("full_test",metadata_only=True)
    monkeypatch.setattr(recovery,"signature",lambda _:"semantic")
    def checker(): raise RuntimeError("checker reached")
    monkeypatch.setattr(storage,"prepare_index_checker",checker)
    with pytest.raises(RuntimeError,match="checker reached"):storage.check_indexes("full_test")
    cfg.atomic_json(path/"generation.json",meta|dict(metadata_only=False,validated=True))
    with pytest.raises(RuntimeError,match="cannot close active"):storage.check_indexes("full_test")


@pytest.fixture
def client(tmp_path, monkeypatch):
    meta = dict(generation="full_test",mode="full",complete=False,validated=False,
                metadata_only=True,manifest_sha256="snapshot",snapshot=str(tmp_path/"snapshot"))
    record = {"id":"https://openalex.org/W1","title":"气候变化", "future_field":{"kept":None},
              "authorships":[],"referenced_works":["https://openalex.org/W999"]}
    monkeypatch.setattr(api,"ROOT",tmp_path)
    monkeypatch.setattr(api,"generation",lambda:dict(meta))
    monkeypatch.setattr(api,"config",lambda:dict(api_keys=["test-key"],qps=10000,burst=10000,
                       concurrency=10,query_timeout=15,cursor_ttl=600,cursor_secret="test"))
    monkeypatch.setattr(manage,"health",lambda name:name=="clickhouse")
    path=tmp_path/"var/generations/full_test";path.mkdir(parents=True)
    (path/"metadata-release.json").write_text(json.dumps({"entities":{"works":{"unique_entities":1,"snapshot_records":1}}}))
    b=Mock();b.details.return_value={"W1":record};b.resolve.return_value=["W1"]
    b.os.side_effect=AssertionError("search must not be called")
    monkeypatch.setattr(api,"backend",lambda:b)
    api.limiter.clear()
    with TestClient(api.app,headers={"X-API-Key":"test-key"}) as c:
        yield c,b,meta,record


def test_readiness_and_complete_records_survive_search_outage(client):
    c,b,_,record=client
    response=c.get("/status").json()
    assert response["metadata_ready"] and response["metadata_complete"]
    assert not response["search_ready"] and not response["complete"]
    assert not c.get("/capabilities").json()["search_ready"]
    assert c.get("/v1/works/W1").json()==record
    assert c.get("/v1/works/lookup",params={"scheme":"doi","identifier":"10.1000/x"}).json()==record
    assert c.post("/v1/works/batch",json={"identifiers":["W1"]}).status_code==200
    exact=c.post("/v1/references/match",json={"references":[{"doi":"10.1000/x"}]}).json()
    assert exact["results"][0]["status"]=="matched"
    b.os.assert_not_called()


def test_search_requires_explicit_readiness(client):
    c,b,_,_=client
    response=c.get("/v1/works",params={"search":"climate"})
    assert response.status_code==503 and response.json()["detail"]["code"]=="search_not_ready"
    assert c.post("/v1/references/match",json={"references":[{"title":"climate"}]}).status_code==503
    b.os.assert_not_called()


def test_mixed_batch_resolves_dois_once_and_preserves_duplicates_and_ambiguity(client):
    c,b,_,record=client
    docs={"W1":record,"W2":{"id":"https://openalex.org/W2","doi":None},"W3":{"id":"https://openalex.org/W3"}}
    b.resolve_many.return_value={"10.1000/mixed":["W2","W3"]}
    b.details.side_effect=lambda g,e,keys:{key:docs[key] for key in keys if key in docs}
    values=["W1","10.1000/MIXED","https://doi.org/10.1000/mixed","10.1000/missing","W999"]
    response=c.post("/v1/works/batch",json={"identifiers":values})
    assert response.status_code==200
    rows=response.json()["results"]
    assert [row["input"] for row in rows]==values
    assert [row["status"] for row in rows]==["found","ambiguous","ambiguous","not_found","not_found"]
    assert rows[1]["results"]==rows[2]["results"]==[docs["W2"],docs["W3"]]
    b.resolve_many.assert_called_once_with("full_test","works","doi",["10.1000/mixed","10.1000/mixed","10.1000/missing"])
    b.resolve.assert_not_called()
    b.os.assert_not_called()
    compact=c.post("/v1/works/batch",params={"select":"doi,title"},json={"identifiers":["W2"]})
    assert compact.status_code==200
    assert compact.json()["results"][0]["results"]==[{"doi":None,"title":None}]


def test_related_works_returns_metadata_in_snapshot_order_without_search(client):
    c,b,_,record=client
    record["related_works"]=["https://openalex.org/W3","https://openalex.org/W2","https://openalex.org/W999"]
    docs={"W1":record,"W2":{"id":"https://openalex.org/W2","title":"Second","doi":None},
          "W3":{"id":"https://openalex.org/W3","title":"First","doi":"https://doi.org/10.1000/first","future_field":{"kept":True}}}
    b.details.side_effect=lambda g,e,keys:{key:docs[key] for key in keys if key in docs}
    response=c.get("/v1/works/W1/related-works")
    assert response.status_code==200
    data=response.json()
    assert data["meta"]["method"]=="snapshot_related_works"
    assert data["meta"]["count"]==3
    assert [r["id"] for r in data["results"]]==record["related_works"]
    assert data["results"][0]["work"]==docs["W3"]
    assert data["results"][2]=={"id":"https://openalex.org/W999","resolved":False,"work":None}
    selected=c.get("/v1/works/W1/related-works",params={"select":"doi,title,publication_year"}).json()
    assert selected["results"][0]["work"]=={"doi":"https://doi.org/10.1000/first","title":"First","publication_year":None}
    assert selected["results"][1]["work"]["doi"] is None
    b.os.assert_not_called()


def test_related_works_handles_empty_missing_and_requires_auth(client):
    c,b,_,record=client
    record["related_works"]=None
    b.details.side_effect=lambda g,e,keys:{"W1":record} if keys==["W1"] else {}
    assert c.get("/v1/works/W1/related-works").json()["results"]==[]
    assert c.get("/v1/works/W99/related-works").status_code==404
    assert c.get("/v1/works/W1/related-works",headers={"X-API-Key":""}).status_code==401
    assert c.get("/v1/works/W1/related-works",params={"search":"x"}).status_code==422
    b.os.assert_not_called()


def test_web_assets_are_public_but_data_is_authenticated(client):
    c,_,_,_=client
    assert c.get("/",headers={"X-API-Key":""}).status_code==200
    response=c.get("/assets/app.js",headers={"X-API-Key":""})
    assert response.status_code==200 and "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
    assert "test-key" not in response.text
    assert c.get("/status",headers={"X-API-Key":""}).status_code==401


def test_validation_instance_can_test_search_before_promotion(release, monkeypatch):
    path,_,meta,_,_=release
    cfg.atomic_json(path/"generation.json",meta|{"metadata_only":True,"validated":True})
    monkeypatch.setenv("OPENALEX_GENERATION","full_test")
    assert not api.generation()["metadata_only"]
    monkeypatch.delenv("OPENALEX_GENERATION")
    cfg.atomic_json(cfg.ROOT/"var/active.json",{"generation":"full_test"})
    assert api.generation()["metadata_only"]
