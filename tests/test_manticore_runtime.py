import pytest

from openalex_api import manticore_runtime as runtime


@pytest.mark.parametrize("settings,expected", [({}, 0), ({"manticore_max_threads_per_query": 4}, 4)])
def test_query_threads_are_applied_to_the_engine(monkeypatch, settings, expected):
    monkeypatch.setattr(runtime, "config", lambda: settings)
    calls = []
    client = type("Client", (), {"sql": lambda self, sql: calls.append(sql)})()
    runtime.configure_queries(client)
    assert calls == ["SET GLOBAL max_threads_per_query=" + str(expected)]


@pytest.mark.parametrize("value", [-1, True, "4", 4.5, None])
def test_invalid_query_threads_do_not_change_engine(monkeypatch, value):
    monkeypatch.setattr(runtime, "config", lambda: {"manticore_max_threads_per_query": value})
    calls = []
    client = type("Client", (), {"sql": lambda self, sql: calls.append(sql)})()
    with pytest.raises(ValueError, match="nonnegative integer"):
        runtime.configure_queries(client)
    assert not calls


def test_runtime_worker_pool_preserves_other_engine_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime, "config", lambda: {"cpus": 96, "manticore_workers": 96})
    path = tmp_path / "engine.conf"
    path.write_text("searchd {\n threads = 32\n binlog_flush = 1\n}\n")
    runtime.configure_workers(path)
    assert path.read_text() == "searchd {\n threads = 96\n binlog_flush = 1\n}\n"
    with pytest.raises(RuntimeError, match="missing or ambiguous"):
        runtime.configure_workers(path)


@pytest.mark.parametrize("value", [0, -1, True, "96", 4.5, None])
def test_invalid_worker_pool_does_not_rewrite_config(monkeypatch, tmp_path, value):
    monkeypatch.setattr(runtime, "config", lambda: {"cpus": 96, "manticore_workers": value})
    path = tmp_path / "engine.conf"
    path.write_text(" threads = 32\n")
    with pytest.raises(ValueError, match="positive integer"):
        runtime.configure_workers(path)
    assert path.read_text() == " threads = 32\n"
