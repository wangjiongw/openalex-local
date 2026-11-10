#!/usr/bin/env python3
"""Restore the pinned compatibility build; never silently follow a moving CI URL."""

import urllib.request, pathlib, hashlib, json, subprocess, shutil, sys

root = pathlib.Path(__file__).resolve().parents[1]
lock = root / "config/engines.lock.json"
data = json.loads(lock.read_text())
entry = data.get("clickhouse-compat")
if not entry:
    raise SystemExit("No approved compatibility build in engines.lock.json")
dest = root / "var/engines/clickhouse-compat/clickhouse"
dest.parent.mkdir(parents=True, exist_ok=True)


def sha(path):
    h = hashlib.sha512()
    with path.open("rb") as f:
        while b := f.read(8 * 1024 * 1024):
            h.update(b)
    return h.hexdigest()


if dest.exists():
    if sha(dest) != entry["runtime_sha512"]:
        raise SystemExit("Installed runtime checksum mismatch; restore pinned backup")
    print(entry["version"])
    sys.exit(0)
backup = root / "var/downloads/clickhouse-compat-runtime"
if backup.exists():
    if sha(backup) != entry["runtime_sha512"]:
        raise SystemExit("Backup runtime checksum mismatch")
    shutil.copy2(backup, dest)
    dest.chmod(0o755)
    print("Restored pinned runtime")
    sys.exit(0)
archive = dest.with_suffix(".download")
with (
    urllib.request.urlopen(
        urllib.request.Request(entry["url"], headers={"User-Agent": "curl/8.0"}),
        timeout=120,
    ) as r,
    archive.open("wb") as out,
):
    while b := r.read(8 * 1024 * 1024):
        out.write(b)
if sha(archive) != entry["sha512"]:
    archive.unlink()
    raise SystemExit(
        "Official CI URL has changed. Supply the retained pinned runtime; refusing a silent upgrade."
    )
archive.rename(dest)
dest.chmod(0o755)
subprocess.run([str(dest), "--version"], check=True)
if sha(dest) != entry["runtime_sha512"]:
    raise SystemExit("Decompressed runtime checksum mismatch")
shutil.copy2(dest, backup)
