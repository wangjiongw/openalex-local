import gzip
import json

import pytest

from openalex_api import snapshot
from openalex_api.demo import make_snapshot


@pytest.mark.parametrize('legacy', [True, False])
def test_layout_and_full_content_verification(tmp_path, legacy):
    root = tmp_path / 'snapshot'
    make_snapshot(root, legacy)
    value = snapshot.inventory(root)
    assert value['entities']['works']['records'] == 12
    assert value['entities']['keywords']['records'] == 0
    assert value['entities']['institutions']['records'] == 1
    assert len(value['files']) == 8
    assert snapshot.verify(root, deep=True)['passed']
    assert value['complete'] is legacy
    with gzip.open(next(v['path'] for v in value['files'] if v['entity'] == 'works'), 'rt') as stream:
        assert json.loads(next(stream))['unknown_future_field'] == {'preserved': 'yes'}


def test_changed_and_truncated_shards_fail(tmp_path):
    make_snapshot(tmp_path)
    item = snapshot.inventory(tmp_path)['files'][0]
    from pathlib import Path
    Path(item['path']).write_bytes(b'bad')
    with pytest.raises(ValueError, match='wrong-size'):
        snapshot.verify(tmp_path)


@pytest.mark.parametrize('url', ['s3://openalex/data/jsonl/works/../secret.gz',
                                  's3://openalex/data/jsonl/works/%2e%2e/secret.gz',
                                  's3://openalex/data/jsonl/authors/a.gz',
                                  'https://example.org/works/a.gz?token=secret'])
def test_object_boundary(url):
    with pytest.raises(ValueError):
        snapshot.relative_object(url, 'works')


def test_parquet_is_detected_without_loading_it(tmp_path):
    path = tmp_path / 'data/parquet'
    path.mkdir(parents=True)
    (path / 'manifest.json').write_text('{}')
    with pytest.raises(ValueError, match='Parquet'):
        snapshot.inventory(tmp_path)


def test_duplicate_shard_rejected(tmp_path):
    make_snapshot(tmp_path)
    path = tmp_path / 'data/jsonl/manifest.json'
    value = json.loads(path.read_text())
    value['entities'][0]['files'] *= 2
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='duplicate'):
        snapshot.inventory(tmp_path)
