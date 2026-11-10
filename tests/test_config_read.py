import importlib
from pathlib import Path
from unittest.mock import Mock
import pytest

cfg = importlib.import_module('openalex_api.config')


def test_config_recovers_missing_read_without_falling_back_to_defaults(monkeypatch):
    read = Mock(side_effect=[FileNotFoundError(), '{"port": 8089, "api_keys": ["fixture"]}'])
    sleep = Mock()
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(cfg.time, 'sleep', sleep)
    value = cfg.config()
    assert value['port'] == 8089 and value['api_keys'] == ['fixture']
    assert read.call_count == 2
    sleep.assert_called_once_with(.05)


def test_persistently_missing_config_fails_after_bounded_retries(monkeypatch):
    read = Mock(side_effect=FileNotFoundError())
    sleep = Mock()
    monkeypatch.setattr(Path, 'read_text', read)
    monkeypatch.setattr(cfg.time, 'sleep', sleep)
    with pytest.raises(RuntimeError, match='unavailable'):
        cfg.config()
    assert read.call_count == 4 and sleep.call_count == 3


def test_malformed_config_is_not_retried(monkeypatch):
    read = Mock(return_value='{')
    monkeypatch.setattr(Path, 'read_text', read)
    with pytest.raises(ValueError):
        cfg.config()
    assert read.call_count == 1
