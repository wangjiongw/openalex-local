"""Read-only resource and merge history for the representative batch benchmark."""

import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import psutil
from openalex_api.backend import Backend, database
from openalex_api.config import ROOT, atomic_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    owner = json.loads((args.report / "owner.json").read_text())
    benchmark = psutil.Process(owner["pid"])
    birth = benchmark.create_time()
    b = Backend(writer=True)
    peak = 0
    max_parts = 0
    samples = 0
    try:
        with (args.report / "resources.jsonl").open("a") as out:
            while (
                benchmark.is_running() and abs(benchmark.create_time() - birth) < 0.01
            ):
                processes = []
                for proc in psutil.process_iter(["pid", "name"]):
                    try:
                        if Path(proc.cwd()) in (ROOT, ROOT.parent / "openalex-api"):
                            processes.append(
                                dict(
                                    pid=proc.pid,
                                    name=proc.name(),
                                    created=proc.create_time(),
                                    rss=proc.memory_info().rss,
                                    cpu=sum(proc.cpu_times()[:2]),
                                    affinity=proc.cpu_affinity(),
                                )
                            )
                    except (psutil.Error, OSError):
                        pass
                rss = sum(p["rss"] for p in processes)
                parts = b.ch(
                    f"SELECT max(n) n FROM (SELECT count() n FROM system.parts WHERE active AND database='{database(owner['generation'])}' GROUP BY table,partition_id) FORMAT JSONEachRow"
                )[0]["n"]
                merges = b.ch(
                    f"SELECT count() n,sum(total_size_bytes_compressed) bytes FROM system.merges WHERE database='{database(owner['generation'])}' FORMAT JSONEachRow"
                )[0]
                peak = max(peak, rss)
                max_parts = max(max_parts, int(parts))
                samples += 1
                row = dict(
                    time=time.time(),
                    rss=rss,
                    parts=int(parts),
                    merges=merges,
                    free=psutil.disk_usage(ROOT).free,
                    processes=processes,
                )
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
                out.flush()
                atomic_json(
                    args.report / "resource-summary.json",
                    dict(
                        samples=samples,
                        peak_rss=peak,
                        max_active_parts_per_partition=max_parts,
                        updated=time.time(),
                    ),
                )
                time.sleep(30)
    finally:
        b.close()


if __name__ == "__main__":
    main()
