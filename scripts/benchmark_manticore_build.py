"""Benchmark identical frozen rows on an isolated engine; never writes production tables."""
import argparse
import itertools
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from openalex_api.backend import Backend, sql_string
from openalex_api.config import ROOT, atomic_json
from openalex_api.earth_control import read_json
from openalex_api.manticore_build import members, write_batch, seal, signature
from openalex_api.manticore_engine import Client, create


def activate_tuning(report):
    selected = report['selected']
    sustained = report.get('sustained') or []
    if (not report.get('passed') or report.get('projection_signature') != signature()
            or report.get('sustained_seconds', 0) < 1800 or not sustained
            or not sustained[0].get('restored')
            or any(r['rows'] != 100000 or r['seconds'] <= 0
                   or any(r[k] != selected[k] for k in ('batch_rows', 'projection_workers')) for r in sustained)):
        raise RuntimeError('tuning requires sustained verified writes and snapshot restoration')
    throughput = sum(r['rows'] for r in sustained) / sum(r['seconds'] for r in sustained)
    baseline = report['matrix'][0]['rows_per_second']
    improved = throughput >= baseline * 1.1
    profile = report | dict(passed=improved, sustained_rows_per_second=throughput,
                            tuning_status='qualified' if improved else 'retain_default')
    atomic_json(ROOT/'var/generations/full_20260908/manticore-tuning.json', profile)
    return profile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:19318')
    parser.add_argument('--output', type=Path, default=ROOT/'reports/manticore-20m-20261108/benchmark')
    parser.add_argument('--finalize-existing', action='store_true')
    args = parser.parse_args()
    if args.url != 'http://127.0.0.1:19318':
        parser.error('benchmark requires the isolated local test engine on 19318')
    out = args.output; out.mkdir(parents=True, exist_ok=True)
    if args.finalize_existing:
        profile = activate_tuning(read_json(out/'result.json'))
        atomic_json(out/'tuning.json', profile)
        print(profile['tuning_status'], flush=True)
        return
    collection = ROOT/'exports/earth_collections/full_20260908/earth_scope_v1'
    build = read_json(ROOT/'var/generations/full_20260908/searches/earth_20m_mc_v1/build.json')
    group = '02_broad_interdisciplinary'
    skip = sum(s['count'] for s in build['segments'] if s.get('group') == group)
    selected = list(itertools.islice(members(collection, read_json(collection/'manifest.json'), group, skip), 100000))
    if len(selected) != 100000:
        raise RuntimeError('benchmark needs 100,000 subsequent members')
    b = Backend(writer=True); rows = []
    for offset in range(0, len(selected), 1000):
        ids = ','.join(sql_string(r['id']) for r in selected[offset:offset+1000])
        rows.extend(b.ch("SELECT id,raw,version,digest FROM oa_full_20260908.entities WHERE entity='works' AND id IN ("+ids+") ORDER BY id FORMAT JSONEachRow"))
        if offset % 10000 == 0: print('loaded', len(rows), flush=True)
    b.close()
    assert len(rows) == len({r['id'] for r in rows}) == len(selected)
    membership = {r['id']: r for r in selected}
    c = Client(args.url, timeout=300)
    measurements = []
    def run(batch, workers, label, snapshot=False):
        name = 'oa_bench_' + label
        path = out/label; path.mkdir(exist_ok=True)
        create(c, name)
        segment = dict(index=name, entity='works', group=group, count=0, batches=[])
        pool = ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context('spawn')) if workers > 1 else None
        started = time.monotonic()
        try:
            for offset in range(0, len(rows), batch):
                chunk = rows[offset:offset+batch]
                write_batch(c, None, path, segment, chunk, [membership[r['id']] for r in chunk], pool)
            elapsed = time.monotonic()-started
            assert int(c.rows('SELECT count(*) n FROM '+name)[0]['n']) == len(rows)
            if snapshot: seal(c, path, segment)
            return dict(batch_rows=batch, projection_workers=workers, rows=len(rows), seconds=elapsed,
                        rows_per_second=len(rows)/elapsed, restored=segment.get('restore_verified', False))
        finally:
            if pool: pool.shutdown()
            c.sql('DROP TABLE '+name)
    for workers in (1, 2, 4):
        for batch in (1000, 2000, 5000):
            result = run(batch, workers, f'w{workers}_b{batch}')
            measurements.append(result); print(result, flush=True)
            atomic_json(out/'matrix.json', measurements)
    best = max(measurements, key=lambda r: r['rows_per_second'])
    baseline = measurements[0]
    if best['rows_per_second'] < baseline['rows_per_second']*1.1: best = baseline
    sustained=[]; started=time.monotonic()
    while time.monotonic()-started < 1800:
        result=run(best['batch_rows'],best['projection_workers'], 'sustained_'+str(len(sustained)),snapshot=not sustained)
        sustained.append(result)
        atomic_json(out/'sustained-progress.json', sustained)
    report=dict(passed=True, projection_signature=signature(), matrix=measurements, selected=best,
                sustained=sustained, sustained_seconds=time.monotonic()-started,
                limitations=['Independent engine; production mixed-load qualification remains mandatory.',
                             'Source reads measured separately; identical cached metadata used for matrix.'])
    atomic_json(out/'result.json',report)
    atomic_json(out/'tuning.json', activate_tuning(report))
    print(report['selected'],flush=True);c.close()


if __name__ == '__main__':
    main()
