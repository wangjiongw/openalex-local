#!/usr/bin/env python3
"""Download fixed official ARM64 builds, record/check SHA512 before extraction."""

import hashlib, json, pathlib, urllib.request, tarfile, concurrent.futures

ROOT = pathlib.Path(__file__).resolve().parents[1]
ITEMS = [
    (
        "clickhouse-common-static-25.8.12.129-arm64.tgz",
        "https://packages.clickhouse.com/tgz/lts/",
        None,
    ),
    (
        "opensearch-3.3.2-linux-arm64.tar.gz",
        "https://artifacts.opensearch.org/releases/bundle/opensearch/3.3.2/",
        "ab055a9192b04be020272a7769801f041c28ad9e12849d54c5ed96b844692b8fcf2ab66ae91f761cc052d3f19777a4a49c43fa81a7ffb037cb2afbe62c4a8dec",
    ),
]


def fetch(item):
    name, base, expected = item
    dest = ROOT / "var/downloads" / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    lock = ROOT / "config/engines.lock.json"
    known = json.loads(lock.read_text()) if lock.exists() else {}
    expected = known.get(name, {}).get("sha512", expected)
    if not dest.exists():
        tmp = dest.with_suffix(".partial")
        print("Downloading", name, flush=True)
        with (
            urllib.request.urlopen(
                urllib.request.Request(base + name, headers={"User-Agent": "curl/8.0"}),
                timeout=120,
            ) as r,
            tmp.open("wb") as out,
        ):
            while chunk := r.read(8 * 1024 * 1024):
                out.write(chunk)
        tmp.rename(dest)
    digest = (
        hashlib.file_digest(dest.open("rb"), "sha512").hexdigest()
        if hasattr(hashlib, "file_digest")
        else sha(dest)
    )
    if expected and digest != expected:
        raise RuntimeError("SHA512 mismatch: " + name)
    target = ROOT / "var/engines"
    installed = target / (
        "opensearch-3.3.2"
        if name.startswith("opensearch")
        else "clickhouse-common-static-25.8.12.129"
    )
    if installed.exists():
        print("Already installed", name, flush=True)
        return name, known.get(
            name,
            dict(
                url=base + name,
                sha512=digest,
                checksum_source="official checksum"
                if item[2]
                else "locally pinned SHA512",
            ),
        )
    with tarfile.open(dest) as t:
        for m in t.getmembers():
            if m.name.startswith("/") or ".." in pathlib.PurePosixPath(m.name).parts:
                raise RuntimeError("Unsafe archive member")
        t.extractall(target)
    print("Installed", name, digest, flush=True)
    return name, dict(
        url=base + name,
        sha512=digest,
        checksum_source="official checksum"
        if item[2]
        else "TLS download, locally pinned SHA512",
    )


def sha(path):
    h = hashlib.sha512()
    with path.open("rb") as f:
        while b := f.read(8 * 1024 * 1024):
            h.update(b)
    return h.hexdigest()


if __name__ == "__main__":
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        result = dict(pool.map(fetch, ITEMS))
    lock = ROOT / "config/engines.lock.json"
    existing = json.loads(lock.read_text()) if lock.exists() else {}
    lock.write_text(json.dumps(existing | result, indent=2) + "\n")
