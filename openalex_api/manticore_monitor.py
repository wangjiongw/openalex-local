"""Lightweight, read-only monitoring of one build through publication."""
import argparse
from collections import deque
import json
import logging
import time

import httpx
from .config import ROOT, atomic_json, config
from .earth_control import read_json
from .search_versions import build_path
from . import manage


def summarize(samples):
    last = samples[-1]
    result = dict(last, rates={})
    for minutes in (5, 10, 30):
        window = [s for s in samples if s['at'] >= last['at'] - minutes * 60]
        first = window[0]
        elapsed = last['at'] - first['at']
        delta = last['count'] - first['count']
        result['rates'][str(minutes)] = dict(
            rows_per_second=delta / elapsed if elapsed > 0 and delta >= 0 else None,
            observed_seconds=elapsed,
            includes_nonwriting_phase=any(s['phase'] != 'building' for s in window))
    result['alerts'] = []
    if not last['builder_alive'] and last['phase'] not in ('published', 'failed', 'rolled_back'):
        result['alerts'].append('builder_not_running')
    recent = [s for s in samples if s['at'] >= last['at'] - 600]
    if (last['at'] - recent[0]['at'] >= 540 and all(s['phase'] == 'building' for s in recent)
            and all(s['count'] == last['count'] for s in recent)):
        result['alerts'].append('no_writing_progress_for_9_minutes')
    if last['phase'] in ('failed', 'rolled_back'):
        result['alerts'].append(last['phase'])
    return result


def snapshot(path, build):
    manifest = read_json(path / 'build.json')
    progress = read_json(path / 'progress.json')
    phase = 'failed' if manifest['state'] == 'failed' else progress['state']
    error = manifest.get('error')
    if phase == 'sealed' or manifest['state'] == 'validated':
        for filename in ('observation.json', 'publication.json'):
            try:
                stage = read_json(path / filename)
            except FileNotFoundError:
                continue
            phase = stage['state']
            error = stage.get('error') or error
    return dict(at=time.time(), count=progress['entities']['works']['count'], phase=phase,
                error=error, progress_updated=progress.get('updated'),
                batch_timings=progress.get('batch_timings'),
                builder_alive=bool(manage.identity(manage.state_path('build-manticore-' + build))))


def run(generation, build):
    path = build_path(generation, build)
    report_path = path / 'speed-monitor.json'
    samples = deque(maxlen=32)
    cfg = config()
    logging.basicConfig(level=logging.INFO)
    with httpx.Client(base_url=f"http://127.0.0.1:{cfg['port']}",
                      headers={'X-API-Key': cfg['api_keys'][0]}, timeout=15, trust_env=False) as client:
        while True:
            try:
                samples.append(snapshot(path, build))
                report = summarize(list(samples))
                started = time.monotonic()
                try:
                    response = client.get('/v1/works', params=dict(search='climate change',
                        search_field='title', search_scope='earth_sciences', per_page=1, select='id,doi,title'))
                    response.raise_for_status()
                    payload = response.json()
                    report['probe'] = dict(seconds=time.monotonic()-started,
                                           search_build=payload['meta'].get('search_build'),
                                           results=len(payload['results']))
                except Exception as error:
                    report['probe'] = dict(error=type(error).__name__)
                    report['alerts'].append('query_probe_failed')
            except Exception as error:
                report = dict(at=time.time(), alerts=['monitor_read_failed'], error=str(error))
            try:
                atomic_json(report_path, report)
            except OSError:
                logging.exception('Monitor report write failed; will retry next minute')
            print(json.dumps(report), flush=True)
            if report.get('phase') in ('published', 'failed', 'rolled_back'):
                return
            time.sleep(60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('generation')
    parser.add_argument('build')
    args = parser.parse_args()
    run(args.generation, args.build)
