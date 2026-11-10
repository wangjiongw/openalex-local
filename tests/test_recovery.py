import json
from unittest.mock import Mock
import httpx
import pytest
from openalex_api import backend, recovery, ingest, build


def bulk_client(items):
    b = object.__new__(backend.Backend)
    b.os = Mock(return_value={"items": items})
    return b


def test_bulk_requires_complete_response():
    b = bulk_client([])
    with pytest.raises(backend.BackendError, match="count mismatch"):
        b.bulk("test_bulk", "works", [("W1", 3, {})])


def test_bulk_conflict_not_silently_accepted():
    b = bulk_client(
        [{"index": {"_id": "W1", "_index": "oa-test-bulk-works", "status": 409}}]
    )
    b.mget = Mock(return_value=[{"_id": "W1", "found": True, "_version": 4}])
    with pytest.raises(backend.BackendError, match="conflicting"):
        b.bulk("test_bulk", "works", [("W1", 3, {})])


def test_storage_error_no_retry():
    b = bulk_client(
        [
            {
                "index": {
                    "_id": "W1",
                    "_index": "oa-test-bulk-works",
                    "status": 500,
                    "error": {"type": "e_o_f_exception"},
                }
            }
        ]
    )
    with pytest.raises(backend.BackendError, match="Fatal"):
        b.bulk("test_bulk", "works", [("W1", 3, {})])
    assert b.os.call_count == 1


def test_http_retries_bounded(monkeypatch):
    monkeypatch.setattr(backend.time, "sleep", lambda _: None)
    call = Mock(
        return_value=httpx.Response(
            503, text="unavailable", request=httpx.Request("GET", "http://example.test")
        )
    )
    with pytest.raises(backend.BackendError):
        backend.retry_http(call)
    assert call.call_count == 6


def test_stale_gate_never_releases(tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "generation_path", lambda _: tmp_path)
    monkeypatch.setattr(recovery, "signature", lambda _: "new")
    (tmp_path / "gate-search.json").write_text(
        json.dumps({"passed": True, "signature": "old"})
    )
    with pytest.raises(RuntimeError, match="stale"):
        recovery.require_evidence("test", "gate-search")


def test_incomplete_benchmark_rejected():
    with pytest.raises(RuntimeError, match="missing required"):
        build.assert_benchmark({"summary": {}})


def test_cancel_before_write(monkeypatch):
    event = Mock()
    event.is_set.return_value = True
    monkeypatch.setattr(ingest, "_cancel_event", event)
    with pytest.raises(ingest.ImportCancelled):
        ingest.check_cancel()


@pytest.mark.parametrize("cause", ["array_index_out_of_bounds_exception", "merge_exception", "this writer hit an unrecoverable error; cannot merge"])
def test_merge_failure_is_not_transient_even_with_503(cause):
    b = bulk_client([{"index": {"_id": "W1", "_index": "oa-test-bulk-works", "status": 503,
        "error": {"type": "unavailable_shards_exception", "caused_by": {"type": cause}}}}])
    with pytest.raises(backend.BackendError, match="Fatal index storage"):
        b.bulk("test_bulk", "works", [("W1", 3, {})])
    assert b.os.call_count == 1
