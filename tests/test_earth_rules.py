import copy
import json
import os
import uuid

import pytest

from openalex_api import earth_rules as er, earth_catalog as ec, search_scopes as scopes
from openalex_api.backend import Backend, database, sql_string
from openalex_api.config import ENTITIES, atomic_json, generation_path


def topic(n, field="19", subfield="1907", name="Geology"):
    return dict(id="https://openalex.org/T"+str(n), display_name=name, score=.9,
                domain=dict(id="https://openalex.org/domains/3"),
                field=dict(id="https://openalex.org/fields/"+field),
                subfield=dict(id="https://openalex.org/subfields/"+subfield))


def rules():
    return er.compile_rules(er.policy(), dict(topics=[topic(1), topic(2,"17","1702","Reservoir computing"),
        topic(3,"33","3312","Climate Change and Migration")]),
        [dict(id="C1",name="Geology",ancestors=[]),dict(id="C2",name="Stratigraphy",ancestors=["C1"])],
        [dict(id="S1",display_name="Geophysical Research Letters",type="journal"),
         dict(id="S2",display_name="Geophysical Research Letters",type="repository")])


def test_domains_are_context_not_inclusion_and_cross_domain_topics_work():
    r=rules()
    assert not er.classify(dict(topics=[topic(2,"17","1702","Reservoir computing")]),r)["earth_related"]
    assert er.classify(dict(topics=[topic(3,"33","3312","Climate Change and Migration")]),r)["earth_related"]
    assert er.classify(dict(topics=[topic(2,"17","1702","Reservoir computing")],primary_topic=topic(1)),r)["earth_related"]
    for sf in ["1111","1107","2212","3305"]:
        assert er.classify(dict(topics=[topic(9,"11",sf,"unknown")]),r)["earth_related"]
    extra=er.compile_rules(er.policy(),dict(topics=[topic(99,"27","2740","Occupational and environmental lung diseases")]),[],[])
    assert er.classify(dict(topics=[topic(99,"27","2740","Occupational and environmental lung diseases")]),extra)["earth_related"]
    assert not er.classify(dict(topics=[topic(90,"27","2730","Cancer treatment")]),extra)["earth_related"]


def test_missing_concept_ancestors_have_explicit_name_fallback():
    r=er.compile_rules(er.policy(),{},[dict(id="C1",name="Catchment hydrology",ancestors=None),
                                     dict(id="C2",name="Neural reservoir computing",ancestors=None)],[])
    assert [c["id"] for c in r["concepts"]]==["C1"]


def test_journal_identity_and_publication_location_and_concept_threshold():
    r=rules()
    assert [j["id"] for j in r["journals"]]==["S1"]
    source=dict(id="https://openalex.org/S1")
    assert er.classify(dict(primary_location=dict(source=source)),r)["reasons"]==["journal:S1"]
    assert not er.classify(dict(locations=[dict(source=source,version="submittedVersion")]),r)["earth_related"]
    assert er.classify(dict(locations=[dict(source=source,is_published=True)]),r)["earth_related"]
    assert not er.classify(dict(concepts=[dict(id="C2",score=.49)]),r)["earth_related"]
    assert er.classify(dict(concepts=[dict(id="C2",score=.5)]),r)["reasons"]==["concept:C2"]


def test_specific_text_abstract_tokens_and_nonrecursive_author_evidence():
    r=rules()
    assert er.classify(dict(title="A study of climate change"),r)["reasons"]==["text:title"]
    assert er.classify(dict(abstract_inverted_index={"tectonic":[0],"seismic":[30]}),r)["reasons"]==["text:abstract"]
    assert not er.classify(dict(abstract_inverted_index={"seismic":[0,1,2]}),r)["earth_related"]
    assert not er.classify(dict(abstract_inverted_index={"climate":[0],"change":[99]}),r)["earth_related"]
    author=dict(author=dict(id="https://openalex.org/A1",display_name="Famous Scientist"))
    record=dict(title="Energy and water management",authorships=[author,author])
    result=er.classify(record,r,{"A1"})
    assert result["reasons"]==["author:A1"] and result["author_seed"]==0 and result["author_ids"]==["A1"]
    assert not er.classify(dict(title="An unrelated algorithm",authorships=[author]),r,{"A1"})["earth_related"]
    assert not er.classify(dict(title="Appendix: Quantitative Sources",keywords=[dict(display_name="Geology",score=.14)]),r)["earth_related"]
    assert not er.classify(dict(keywords=[dict(display_name="soil",score=.9),dict(display_name="erosion",score=.9)]),r)["earth_related"]
    assert er.classify(dict(topics=[dict(topic(1),score=.01)]),r)["author_seed"]==0


def test_journal_auto_selection_needs_specialist_name_and_core_profile():
    journals=[dict(id="S1",display_name="Nature",type="journal",topics=[dict(topic(1),count=999)]),
              dict(id="S2",display_name="Geological Studies",type="journal",topics=[dict(topic(1),count=50)]),
              dict(id="S3",display_name="Geological Education",type="journal",topics=[dict(topic(1,"33"),count=50)]),
              dict(id="S4",display_name="Geological Studies Small",type="journal",topics=[dict(topic(1),count=1)])]
    assert [j["id"] for j in er.compile_rules(er.policy(),{},[],journals)["journals"]]==["S2"]


@pytest.mark.skipif(os.environ.get("OA_LIVE_TESTS") != "1", reason="requires real ClickHouse")
def test_sql_matches_independent_python_on_real_clickhouse():
    b=Backend(writer=True)
    r=rules()
    records=[{},dict(topics=[topic(1)]),dict(primary_topic=topic(1)),
             dict(topics=[topic(2,"17","1702","Reservoir computing")]),
             dict(primary_location=dict(source=dict(id="S1"))),
             dict(locations=[dict(source=dict(id="S1"),is_published=True)]),
             dict(locations=[dict(source=dict(id="S1"),version="submittedVersion")]),
             dict(concepts=[dict(id="C2",score=.5)]),dict(concepts=[dict(id="C2",score=.49)]),
             dict(title="A study of climate change"),dict(keywords=[dict(display_name="groundwater",score=.8)]),
             dict(title="Appendix",keywords=[dict(display_name="Geology",score=.14)]),
             dict(keywords=[dict(display_name="soil",score=.9),dict(display_name="erosion",score=.9)]),
             dict(topics=[dict(topic(1),score=.01)]),
             dict(abstract_inverted_index={"tectonic":[0],"Seismic":[10]}),
             dict(abstract_inverted_index={"seismic":[0,1,2]}),
             dict(title="Math",abstract_inverted_index={"yapılandırmacı":[0]}),
             dict(title="élandé",abstract_inverted_index={"seismic/tectonic":[0]}),
             dict(abstract_inverted_index={"seismicü":[0],"seismicö":[1]}),
             dict(title="élandé"),
             dict(title="Water management",authorships=[dict(author=dict(id="A1"))]),
             dict(topics=[topic(1)],authorships=[dict(author=dict(id="https://openalex.org/A1"))])]
    try:
        for record in records:
            q=er.evidence_sql(r)+" FROM (SELECT 'W1' id,toUInt64(1) version,repeat('a',64) digest,"+sql_string(json.dumps(record))+" raw) FORMAT JSONEachRow"
            got=b.ch(q)[0]; expected=er.classify(record,r)
            for k in ["field_ids","reasons","author_ids","author_seed","author_candidate"]:
                assert got[k]==expected[k],(record,k,got,expected)
    finally: b.close()


@pytest.mark.skipif(os.environ.get("OA_LIVE_TESTS") != "1", reason="requires real ClickHouse")
def test_unicode_upgrade_clones_old_facts_and_repairs_before_continuing(monkeypatch):
    import time
    from openalex_api import search_versions,resources,api
    from openalex_api.proofs import digest
    from openalex_api.resources import Paused
    from openalex_api.config import config
    from fastapi.testclient import TestClient
    g="test_unicode_"+uuid.uuid4().hex[:10];b=Backend(writer=True);r=rules()
    monkeypatch.setattr(search_versions,"require_metadata",lambda g:dict(signature="fixture"))
    monkeypatch.setattr(ec,"REPAIR_BATCH_ROWS",2)
    records=[dict(id="https://openalex.org/W1",title="Math",abstract_inverted_index={"yapılandırmacı":[0]},authorships=[dict(author=dict(id="A1"))]),
             dict(id="https://openalex.org/W2",title="Geology",topics=[topic(1)]),
             dict(id="https://openalex.org/W3",title="A generic algorithm")]
    expected={e:dict(records=3 if e=="works" else 0) for e in ENTITIES}
    meta=dict(generation=g,metadata_frozen=True,metadata_only=True,mode="full",manifest_sha256="fixture",expected=expected)
    atomic_json(generation_path(g)/"generation.json",meta)
    atomic_json(generation_path(g)/"earth-rules-v2.json",r)
    paused=[]
    def checkpoint(g):
        state=json.loads(scopes.catalog_path(g).read_text())
        if state.get("phase")=="unicode_repair" and state.get("repair_checked",0) and not paused:
            paused.append(True);raise Paused("fixture repair interruption")
    monkeypatch.setattr(resources,"checkpoint",checkpoint)
    try:
        b.create(g,{},search=False)
        b.insert(g,"entities",[dict(entity="works",id="W"+str(n+1),version=1,digest=str(n)*64,raw=json.dumps(record)) for n,record in enumerate(records)])
        old=database(g)+".old_facts"
        b.ch(f"CREATE TABLE {old} (id String,version UInt64,raw_digest FixedString(64),field_ids Array(UInt16),reasons Array(String),author_ids Array(String),author_seed UInt8,author_candidate UInt8) ENGINE=ReplacingMergeTree(version) ORDER BY id")
        data=[]
        for n,record in enumerate(records):
            e=er.classify(record,r)
            if n==0:e.update(author_candidate=1,author_ids=["A1"])
            data.append(dict(id="W"+str(n+1),version=1,raw_digest=str(n)*64,**{k:e[k] for k in ['field_ids','reasons','author_ids','author_seed','author_candidate']}))
        b.ch('INSERT INTO '+old+' FORMAT JSONEachRow',data=('\n'.join(json.dumps(x) for x in data)+'\n').encode())
        prior=dict(state="paused",phase="evidence",rule_version=2,metadata_signature="fixture",signature="old",rules_signature=digest(r),implementation_signature=ec.ASCII_IMPLEMENTATION,
                   facts_table=old,table=database(g)+".old_membership",authors_table=database(g)+".old_authors",count=3,after="W3",finalized_count=0,finalized_after="",started=time.time())
        atomic_json(scopes.catalog_path(g),prior)
        with pytest.raises(Paused):scopes.build_catalog(g)
        repaired=json.loads(scopes.catalog_path(g).read_text())
        assert repaired['phase']=='unicode_repair' and repaired['repair_checked']==2 and repaired['count']==3
        assert repaired['facts_table']!=old and repaired['signature']!='old'
        old_row=b.ch("SELECT author_candidate FROM "+old+" FINAL WHERE id='W1' FORMAT JSONEachRow")[0]
        new_row=b.ch("SELECT author_candidate,author_ids FROM "+repaired['facts_table']+" FINAL WHERE id='W1' FORMAT JSONEachRow")[0]
        assert old_row['author_candidate']==1 and new_row==dict(author_candidate=0,author_ids=[])
        monkeypatch.setattr(api,"generation",lambda:meta);monkeypatch.setattr(api,"backend",lambda:b)
        with TestClient(api.app,headers={"X-API-Key":config()['api_keys'][0]}) as h:
            v=h.get('/v1/works/classification',params=dict(identifier='W2')).json()['results'][0]['classification']
            assert v['state']=='repairing' and v['earth_related'] is None
        final=scopes.build_catalog(g)
        assert final['state']=='complete' and final['count']==3 and final['repair_completed']
        assert b.ch("SELECT earth_related FROM "+final['table']+" FINAL WHERE id='W1' FORMAT JSONEachRow")[0]['earth_related']==0
    finally:b.ch('DROP DATABASE IF EXISTS '+database(g));b.close()


@pytest.mark.skipif(os.environ.get("OA_LIVE_TESTS") != "1", reason="requires real ClickHouse")
def test_real_catalog_preserves_legacy_and_seeds_authors_without_recursion(monkeypatch):
    from openalex_api import search_versions, resources
    from openalex_api.resources import Paused
    from openalex_api import api
    from fastapi.testclient import TestClient
    from openalex_api.config import config
    g="test_earth_"+uuid.uuid4().hex[:12]
    b=Backend(writer=True)
    p=copy.deepcopy(er.policy());p["author_min_core_works"]=2
    monkeypatch.setattr(er,"policy",lambda:p)
    monkeypatch.setattr(search_versions,"require_metadata",lambda g:dict(signature="fixture"))
    monkeypatch.setattr(ec,"BATCH_ROWS",2)
    checks=[]
    def checkpoint(g):
        state=json.loads(scopes.catalog_path(g).read_text())
        if state["count"]==2 and not checks:
            checks.append(True);raise Paused("fixture interruption")
    monkeypatch.setattr(resources,"checkpoint",checkpoint)
    author=lambda n:dict(author=dict(id="https://openalex.org/A"+str(n)))
    records=[dict(id="https://openalex.org/W1",title="Study one",topics=[topic(1)],authorships=[author(1)]),
             dict(id="https://openalex.org/W2",title="Study two",topics=[topic(1)],authorships=[author(1)]),
             dict(id="https://openalex.org/W3",title="Energy and water management",authorships=[author(1),author(2)]),
             dict(id="https://openalex.org/W4",title="Water management",authorships=[author(2)]),
             dict(id="https://openalex.org/W5",title="An unrelated algorithm",authorships=[author(1)]),
             dict(id="https://openalex.org/W6",title="Legacy topic",topics=[topic(9,"17","1702","Legacy topic")])]
    entities=[dict(entity="works",id="W"+str(n+1),version=1,digest=str(n)*64,raw=json.dumps(r)) for n,r in enumerate(records)]
    entities += [dict(entity="topics",id="T1",version=1,raw=json.dumps(topic(1))),dict(entity="topics",id="T9",version=1,raw=json.dumps(topic(9,"17","1702","Legacy topic")))]
    expected={e:dict(records=6 if e=="works" else 2 if e=="topics" else 0) for e in ENTITIES}
    meta=dict(generation=g,metadata_frozen=True,metadata_only=True,mode="full",manifest_sha256="fixture",expected=expected)
    atomic_json(generation_path(g)/"generation.json",meta)
    try:
        b.create(g,{},search=False);b.insert(g,"entities",entities)
        baseline=database(g)+".legacy"
        b.ch(f"CREATE TABLE {baseline} (id String,owner String) ENGINE=ReplacingMergeTree ORDER BY id")
        b.ch(f"INSERT INTO {baseline} VALUES ('W6','earth_sciences')")
        atomic_json(scopes.catalog_path(g),dict(rule_version=1,state="building",table=baseline,signature="legacy",metadata_signature="fixture",count=1,after="W6",rules=[dict(id="T9",name="Legacy topic")]))
        with pytest.raises(Paused):scopes.build_catalog(g)
        state=json.loads(scopes.catalog_path(g).read_text())
        assert state["count"]==2 and (generation_path(g)/"search-catalog-v1.json").exists()
        catalog=scopes.build_catalog(g)
        assert catalog["state"]=="complete" and catalog["count"]==6 and catalog["finalized_count"]==6
        assert scopes.build_catalog(g)["signature"]==catalog["signature"]
        rows={r["id"]:r for r in b.ch("SELECT id,earth_related,reasons FROM "+catalog["table"]+" FINAL ORDER BY id FORMAT JSONEachRow")}
        assert [k for k,v in rows.items() if v["earth_related"]]==["W1","W2","W3","W6"]
        assert rows["W3"]["reasons"]==["author:A1"]
        seeds=b.ch("SELECT id,core_works FROM "+catalog["authors_table"]+" FINAL ORDER BY id FORMAT JSONEachRow")
        assert seeds==[dict(id="A1",core_works=2)]
        monkeypatch.setattr(api,"generation",lambda:meta)
        monkeypatch.setattr(api,"backend",lambda:b)
        with TestClient(api.app,headers={"X-API-Key":config()["api_keys"][0]}) as h:
            r=h.get("/v1/works/classification",params=dict(identifier="W3"))
            assert r.status_code==200,r.text
            assert r.json()["results"][0]["classification"]==dict(label="earth_sciences",earth_related=True,state="complete",reasons=["author:A1"])
            assert h.get("/v1/works/classification",params=dict(identifier="W5")).json()["results"][0]["classification"]["earth_related"] is False
            # Missing facts and author-only candidates remain unknown during extraction.
            unfinished=dict(catalog,state="building",phase="evidence")
            atomic_json(scopes.catalog_path(g),unfinished)
            assert h.get("/v1/works/classification",params=dict(identifier="W3")).json()["results"][0]["classification"]["earth_related"] is None
            assert h.get("/v1/works/classification",params=dict(identifier="W1")).json()["results"][0]["classification"]["earth_related"] is True
    finally:
        b.ch("DROP DATABASE IF EXISTS "+database(g));b.close()
