"""Install hash-pinned user-space engines without modifying the host OS."""
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

import httpx

from .config import ROOT, RESOURCE_ROOT, atomic_json
from .ingest import lock


def checksum(path, algorithm='sha256'):
    h = hashlib.new(algorithm)
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 * 1024**2), b''):
            h.update(chunk)
    return h.hexdigest()


def architecture():
    if platform.system() != 'Linux':
        raise RuntimeError('native engines currently require Linux/glibc')
    value = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64', 'amd64': 'amd64'}.get(platform.machine())
    if not value:
        raise RuntimeError('unsupported engine architecture: ' + platform.machine())
    return value


def ensure(offline=False, cache=None, profile='standard', search=False):
    from .config import config
    if config().get("adopted_from"):
        raise ValueError("adopted workspaces cannot replace shared engines")
    arch = architecture()
    entries = json.loads((RESOURCE_ROOT / 'platform-engines.json').read_text())
    if profile == 'armv8-compat' and arch != 'arm64':
        raise ValueError('armv8-compat is an ARM64-only profile')
    selected = {name: item for name, item in entries[arch].items() if name == 'clickhouse' or search}
    if profile == 'armv8-compat':
        selected['clickhouse'] = entries['armv8-compat']['clickhouse']
    cache = Path(cache).expanduser().resolve() if cache else ROOT / 'var/downloads'
    destination = ROOT / 'var/engines'
    destination.mkdir(parents=True, exist_ok=True)
    results = {}
    with lock(ROOT / 'var/run/engines.lock'):
        for engine, item in selected.items():
            target = destination / item['directory']
            receipt = target / 'installation.json'
            identity = hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()
            if target.exists():
                if not receipt.exists():
                    raise RuntimeError('unmanaged engine installation; preserve it and use a new workspace: ' + str(target))
                proof = json.loads(receipt.read_text())
                if proof['identity'] != identity or any(checksum(target / name) != digest for name, digest in proof['runtime'].items()):
                    raise RuntimeError('engine profile/checksum changed; preserve installation and use a new workspace')
                results[engine] = proof
                continue
            candidates = [cache / item['filename'], cache / 'manticore' / item['filename']]
            candidates += [cache / alias for alias in item.get('cache_aliases', [])]
            archive = next((p for p in candidates if p.is_file()), None)
            if archive is None:
                if offline:
                    raise RuntimeError('pinned engine is absent from offline cache: ' + item['filename'])
                archive = ROOT / 'var/downloads' / item['filename']
                archive.parent.mkdir(parents=True, exist_ok=True)
                temp = archive.with_suffix('.partial')
                with httpx.stream('GET', item['url'], timeout=120, follow_redirects=True, trust_env=False) as response:
                    response.raise_for_status()
                    with temp.open('wb') as output:
                        for chunk in response.iter_bytes(8 * 1024**2):
                            output.write(chunk)
                temp.replace(archive)
            if checksum(archive, item['algorithm']) != item['digest']:
                raise RuntimeError('pinned engine archive checksum mismatch: ' + item['filename'])
            with tempfile.TemporaryDirectory(prefix='install-', dir=destination) as directory:
                stage = Path(directory) / 'engine'
                stage.mkdir()
                if item['kind'] == 'binary':
                    shutil.copyfile(archive, stage / item['binary'])
                elif item['kind'] == 'tgz':
                    with tarfile.open(archive) as stream:
                        members = [m for m in stream if m.isfile() and m.name.endswith('/usr/bin/clickhouse')]
                        if len(members) != 1:
                            raise RuntimeError('expected one ClickHouse executable in pinned archive')
                        with stream.extractfile(members[0]) as source, (stage / item['binary']).open('wb') as output:
                            shutil.copyfileobj(source, output)
                elif item['kind'] == 'deb':
                    dpkg = shutil.which('dpkg-deb')
                    if not dpkg:
                        raise RuntimeError('dpkg-deb is required to unpack the pinned Manticore package')
                    subprocess.run([dpkg, '-x', str(archive), str(stage)], check=True, timeout=300)
                else:
                    raise ValueError('unsupported engine package format')
                binary = stage / item['binary']
                binary.chmod(0o755)
                env = os.environ.copy()
                if engine == 'manticore':
                    env['MANTICORE_MODULES'] = str(stage / 'usr/share/manticore/modules')
                try:
                    version = subprocess.check_output([str(binary), '--version'], env=env, text=True, stderr=subprocess.STDOUT, timeout=60)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                    raise RuntimeError('engine cannot run on this CPU/system; ARMv8 users may need --profile armv8-compat') from error
                if item['version'] not in version:
                    raise RuntimeError('installed engine version differs from lock')
                required = item['runtime_files']
                if engine == 'manticore' and ('(secondary ' not in version or '(columnar ' not in version):
                    raise RuntimeError('Manticore columnar/secondary modules did not load')
                proof = dict(engine=engine, architecture=arch, profile=profile, identity=identity,
                             version=version.strip(), runtime={name: checksum(stage / name) for name in required})
                atomic_json(stage / 'installation.json', proof)
                stage.rename(target)
                results[engine] = proof
    return dict(passed=True, architecture=arch, engines=results)
