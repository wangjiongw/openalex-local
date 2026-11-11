"""Read-only arXiv adapters and a shared, loss-aware normalized source record."""
import gzip
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from .model import doi
from .semantics import digest

VERSION = 1


def identifier(value):
    value = str(value).strip()
    value = re.sub(r'^https?://arxiv.org/(abs|pdf)/', '', value, flags=re.I)
    value = re.sub(r'^arxiv:', '', value, flags=re.I)
    value = re.sub(r'\.pdf$', '', value)
    value = re.sub(r'v[1-9][0-9]*$', '', value)
    if not re.fullmatch(r'(?:[0-9]{4}\.[0-9]{4,5}|[a-z][a-z.\-]+/[0-9]{7})', value):
        raise ValueError('invalid arXiv identifier')
    return value


def date(value):
    try:
        parsed = parsedate_to_datetime(value) if ',' in value else datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError, AttributeError) as error:
        raise ValueError('invalid arXiv source date') from error


def first_submission(raw, adapter):
    identifier(raw['arxiv_id'] if adapter == 'duckdb' else raw['id'])
    if adapter == 'duckdb':
        first = date(raw['first_created'])
        if raw['first_version'] != 'v1' or int(raw['first_created_year']) != int(first[:4]):
            raise ValueError('derived first submission does not agree with v1/year')
        return first
    versions = raw.get('versions') or []
    if not isinstance(versions,list) or not versions or versions[0].get('version') != 'v1':
        raise ValueError('arXiv versions must start with v1')
    return date(versions[0]['created'])


def normalize(raw, adapter):
    derived = adapter == 'duckdb'
    key = identifier(raw['arxiv_id'] if derived else raw['id'])
    versions = raw.get('versions') or []
    if derived:
        first, latest = date(raw['first_created']), date(raw['latest_created'])
        first_version, latest_version = raw['first_version'], raw['latest_version']
        if first_version != 'v1' or int(raw['first_created_year']) != int(first[:4]):
            raise ValueError('derived first submission does not agree with v1/year')
    else:
        if not isinstance(versions, list) or not versions or versions[0].get('version') != 'v1':
            raise ValueError('arXiv versions must start with v1')
        first, latest = date(versions[0]['created']), date(versions[-1]['created'])
        first_version, latest_version = 'v1', versions[-1]['version']
    if latest < first or not re.fullmatch('v[1-9][0-9]*', latest_version):
        raise ValueError('invalid arXiv version chronology')
    for field in ('title', 'abstract', 'authors'):
        if not isinstance(raw.get(field), str) or not raw[field].strip():
            raise ValueError('missing required arXiv field: ' + field)
    categories = raw.get('categories') or ''
    if not isinstance(categories, str) or not categories.split():
        raise ValueError('missing arXiv categories')
    publication_doi = doi(raw.get('doi'))
    return dict(id='arxiv:' + key, arxiv_id=key, title=raw['title'], abstract=raw['abstract'],
                authors=raw['authors'], authors_parsed=raw.get('authors_parsed') if not derived else None,
                categories=categories.split(), first_submitted=first, latest_submitted=latest,
                publication_year=int(first[:4]), first_version=first_version, latest_version=latest_version,
                doi=publication_doi, doi_status='valid' if publication_doi else 'invalid_source_value' if raw.get('doi') else 'missing', journal_ref=raw.get('journal_ref') if derived else raw.get('journal-ref'), comments=raw.get('comments'),
                license=raw.get('license'), update_date=raw.get('update_date'),
                report_no=raw.get('report_no') if derived else raw.get('report-no'),
                versions=versions if not derived else None,
                source='arxiv', type='preprint', url='https://arxiv.org/abs/' + key,
                references_status='not_provided_by_source',
                provenance=dict(adapter=adapter, record_kind='derived_record' if derived else 'original_record',
                                raw_available=not derived, authors_parsed_available=not derived and 'authors_parsed' in raw,
                                version_dates_available=not derived, historical_version_content_available=False,
                                source_hash=digest(raw)), raw_source=raw)


def records(path, adapter):
    path = Path(path)
    if adapter == 'jsonl':
        opener = gzip.open if path.suffix == '.gz' else open
        with opener(path, 'rt', encoding='utf-8') as stream:
            for line, value in enumerate(stream, 1):
                if value.strip():
                    raw = json.loads(value)
                    if not isinstance(raw, dict):
                        raise ValueError(f'arXiv JSONL line {line} must be an object')
                    yield line, raw
    elif adapter == 'duckdb':
        try:
            import duckdb
        except ImportError as error:
            raise ValueError('install openalex-local-api[arxiv-duckdb] for the optional derived adapter') from error
        with duckdb.connect(str(path), read_only=True) as connection:
            connection.execute("SET threads=4")
            connection.execute("SET memory_limit='2GB'")
            result = connection.execute('SELECT * FROM arxiv_metadata ORDER BY arxiv_id')
            columns = [c[0] for c in result.description]
            line = 0
            while batch := result.fetchmany(5000):
                for row in batch:
                    line += 1
                    yield line, dict(zip(columns, row))
    else:
        raise ValueError('input format must be jsonl or duckdb')
