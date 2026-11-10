"""One measured disk-usage cache shared by all project workers."""

import time
import logging
import threading
from . import diagnostics
from .config import ROOT, atomic_json
from .ingest import project_usage_bytes


def run():
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    threading.Thread(target=diagnostics.sample_loop, args=(stop,), daemon=True).start()
    while True:
        start = time.time()
        usage = project_usage_bytes()
        atomic_json(
            ROOT / "var/run/disk-usage.json",
            {"time": time.time(), "bytes": usage, "scan_seconds": time.time() - start},
        )
        logging.info("Measured project usage: %.2f GiB", usage / 1024**3)
        time.sleep(300)


if __name__ == "__main__":
    run()
