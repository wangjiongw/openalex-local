import copy
import json
import os
import uuid
from pathlib import Path

import pytest

from openalex_api import earth_collections as ec
from openalex_api.backend import Backend, database
from openalex_api.config import ENTITIES, atomic_json, generation_path
from openalex_api.proofs import digest


def registry():
    return {"S1": dict(id="S1", name="Geology", issn_l="0091-7613", scope="core", evidence={}),
            "S2": dict(id="S2", name="Nature", scope="cross", evidence={})}


def row(record, reasons=()):
    return dict(raw=json.dumps(dict(id="https://openalex.org/W1", **record)), version=1,
                raw_digest="a" * 64, reasons=list(reasons))


def test_groups_publication_identity_and_author_priority():
    source = dict(id="https://openalex.org/S1")
    assert ec.project(row(dict(primary_location=dict(source=source))), registry(), "v1")["group"] == ec.GROUPS[0]
    assert ec.project(row(dict(locations=[dict(source=source, version="submittedVersion")])), registry(), "v1") is None
    assert ec.project(row(dict(locations=[dict(source=source, version="publishedVersion")])), registry(), "v1")["group"] == ec.GROUPS[0]
    assert ec.project(row({}, ["field:19", "author:A1"]), registry(), "v1")["group"] == ec.GROUPS[1]
    assert ec.project(row({}, ["author:A1"]), registry(), "v1") is None
    assert ec.project(row({}, ["author:A1"]), registry(), "v1", "author")["group"] == ec.GROUPS[2]
    assert ec.project(row(dict(primary_location=dict(source=source)), ["author:A1"]), registry(), "v1", "author") is None


def test_projection_keeps_missingness_author_order_and_csv_round_trip():
    r = ec.project(row(dict(title='气候,"研究"\n第二行', doi="https://doi.org/10.1234/AB(C)",
                           abstract_inverted_index={"世界": [1], "你好": [0, 2]},
                           authorships=[dict(author=dict(id="A2", display_name="乙")), dict(author=dict(id="A1", display_name="甲"))],
                           open_access=dict(is_oa=False, oa_status="closed")), ["text:title"]), registry(), "v1")
    assert r["doi"] == "10.1234/ab(c)" and r["abstract"] == "你好 世界 你好"
    assert [a["name"] for a in r["authors"]] == ["乙", "甲"]
    assert ec.csv_row(r)["is_oa"] is False
    invalid = ec.project(row(dict(abstract_inverted_index={"bad": [-1]}), ["field:19"]), registry(), "v1")
    assert invalid["abstract"] is None and invalid["abstract_status"] == "invalid"
    assert invalid["open_access"].get("is_oa") is None and invalid["field_errors"]
    with pytest.raises(ValueError):ec.collection_path("full", "../escape")


def test_half_written_block_has_no_commit_receipt(tmp_path):
    state = dict(blocks=0, phase="direct", after="")
    record = ec.project(row({}, ["field:19"]), registry(), "v1")
    def broken():
        yield record
        raise RuntimeError("interrupted stream")
    with pytest.raises(RuntimeError, match="interrupted"):
        ec.write_block(tmp_path, state, "W1", 1, broken())
    assert not (tmp_path / "blocks/00000000.json").exists()
    receipt = ec.write_block(tmp_path, state, "W1", 1, iter([record]))
    ec.validate_receipt(tmp_path, receipt)
    assert receipt["counts"][ec.GROUPS[1]] == 1


def test_export_service_identity_command_and_memory_priority(monkeypatch):
    from openalex_api import manage
    name = 'export-earth-full_20260908'
    assert manage.state_path(name).name == name + '.json'
    command, _ = manage.command(name, ['--collection', 'v1', '--follow'])
    assert command[2:6] == ['openalex_api.cli', 'earth-export', 'full_20260908', '--collection']
    assert command[-1] == '--follow'
    monkeypatch.setattr(manage, 'identity', lambda path: object())
    assert manage.health(name)
    with pytest.raises(ValueError):manage.state_path('export-earth-../escape')


@pytest.mark.skipif(os.environ.get("OA_LIVE_TESTS") != "1", reason="requires real ClickHouse")
def test_real_export_resume_independent_journal_inclusion_and_final_authors(monkeypatch, tmp_path):
    from openalex_api import official, recovery, resources, earth_catalog
    g = "test_collection_" + uuid.uuid4().hex[:10]
    b = Backend(writer=True)
    rules = {"fixture": True, "journals": []}
    p = generation_path(g)
    monkeypatch.setattr(ec, "ROOT", tmp_path)
    monkeypatch.setattr(ec, "PAGE_ROWS", 2)
    monkeypatch.setattr(ec, "SHARD_ROWS", 1)
    monkeypatch.setattr(recovery, "require_evidence", lambda *args: dict(signature="fixture", passed=True))
    monkeypatch.setattr(official, "source_table", lambda b, meta, entity: database(g) + ".entities FINAL")
    policy = tmp_path / "policy.json"
    atomic_json(policy, dict(version=1, journals=[dict(name="Geology", scope="core", issn=["0091-7613"],
                                                    url="https://example.org/geology", scope_summary="Geology")]))
    monkeypatch.setattr(ec, "POLICY", policy)
    records = [dict(primary_location=dict(source=dict(id="https://openalex.org/S1"))), {}, {},
               dict(locations=[dict(source=dict(id="S1"), is_published=True)]), {},
               dict(abstract_inverted_index={"bad": [-1]}), dict(title='带逗号, 和换行\n题名')]
    reasons = [[], ["field:19"], [], ["field:19"], [], ["text:title"], ["text:title"]]
    atomic_json(p / "generation.json", dict(generation=g, expected={e:dict(records=7 if e=="works" else 0) for e in ENTITIES}))
    atomic_json(p / "earth-rules-v2.json", rules)
    catalog = dict(rule_version=2, state="building", phase="evidence", signature="fixture",
                   metadata_signature="fixture", implementation_signature=earth_catalog.implementation_signature(),
                   rules_signature=digest(rules), count=7, after="W7", facts_table=database(g)+".facts", table=database(g)+".membership")
    atomic_json(p / "search-catalog.json", catalog)
    try:
        b.create(g, {}, search=False)
        values = [dict(entity="works", id="W"+str(i+1), raw=json.dumps(dict(id="https://openalex.org/W"+str(i+1), **r)), version=1, digest="a"*64) for i,r in enumerate(records)]
        values += [dict(entity="sources", id="S1", raw=json.dumps(dict(id="https://openalex.org/S1", display_name="Geology", type="journal", works_count=10, issn_l="0091-7613"))),
                   dict(entity="sources", id="S2", raw=json.dumps(dict(id="https://openalex.org/S2", display_name="Geology", type="journal", works_count=4, issn_l="0000-0000")))]
        b.insert(g, "entities", values)
        b.ch(f"CREATE TABLE {catalog['facts_table']} (id String,version UInt64,raw_digest FixedString(64),reasons Array(String)) ENGINE=ReplacingMergeTree(version) ORDER BY id")
        b.ch(f"CREATE TABLE {catalog['table']} (id String,version UInt64,raw_digest FixedString(64),owner String,reasons Array(String)) ENGINE=ReplacingMergeTree(version) ORDER BY id")
        data=[dict(id="W"+str(i+1),version=1,raw_digest="a"*64,reasons=r) for i,r in enumerate(reasons)]
        b.ch(f"INSERT INTO {catalog['facts_table']} FORMAT JSONEachRow", data=('\n'.join(json.dumps(r) for r in data)+'\n').encode())
        member=dict(id="W3",version=1,raw_digest="a"*64,owner="earth_sciences",reasons=["author:A1"])
        b.ch(f"INSERT INTO {catalog['table']} FORMAT JSONEachRow",data=(json.dumps(member)+'\n').encode())
        original=ec.apply_receipt
        calls=[]
        def fail_before_manifest(state, receipt):
            if not calls:
                calls.append(True);raise RuntimeError("crash after committed files")
            return original(state,receipt)
        monkeypatch.setattr(ec,"apply_receipt",fail_before_manifest)
        with pytest.raises(RuntimeError,match="crash after"):
            ec.run(g,"v1",max_blocks=1)
        monkeypatch.setattr(ec,"apply_receipt",original)
        partial=ec.run(g,"v1")
        assert partial['state']=='waiting_for_scan' and partial['scanned']==7
        assert partial['counts']==dict(zip(ec.GROUPS,[2,3,0]))
        check=ec.verify(g,"v1");assert check['passed'] and not check['complete']
        frozen=json.loads((ec.collection_path(g,"v1")/'journals.json').read_text())
        assert frozen['journals']['S1']['scope']=='core' and frozen['journals']['S2']['scope']=='pending'
        atomic_json(p/'search-catalog.json',dict(catalog,state='complete',phase='complete'))
        final=ec.run(g,"v1")
        assert final['state']=='complete' and final['counts']==dict(zip(ec.GROUPS,[2,3,1]))
        assert ec.verify(g,"v1")['complete']
        assert ec.run(g,"v1")['counts']==final['counts']
        from openalex_api import earth_control
        committed_once=[]
        def transient_after_commit(state, receipt):
            if not committed_once:
                committed_once.append(True)
                raise FileNotFoundError(2,'transient catalog read',str(p/'search-catalog.json'))
            return original(state,receipt)
        monkeypatch.setattr(ec,'apply_receipt',transient_after_commit)
        recovered=earth_control.run(g,'controlled',max_blocks=1)
        assert recovered['blocks']==1 and recovered['state']=='partial'
        monkeypatch.setattr(ec,'apply_receipt',original)
        assert earth_control.run(g,'controlled')['counts']==final['counts']
        assert ec.verify(g,'controlled')['complete']
        b.ch(f"INSERT INTO {catalog['facts_table']} VALUES ('W2',2,repeat('b',64),['field:19'])")
        with pytest.raises(RuntimeError,match='coverage or identity mismatch'):ec.run(g,"bad")
        policy.write_text(json.dumps(dict(version=1,journals=[])))
        with pytest.raises(RuntimeError,match='inputs changed'):ec.run(g,"v1")
        f=next((ec.collection_path(g,"v1")/ec.GROUPS[0]).glob('*.csv'));f.write_text('corrupted')
        with pytest.raises(RuntimeError,match='changed'):ec.verify(g,"v1")
    finally:
        b.ch('DROP DATABASE IF EXISTS '+database(g));b.close()
