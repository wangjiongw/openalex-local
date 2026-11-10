"""Fail closed when a shared workspace is still assigned to another host."""
import json
import socket

from .config import ROOT, atomic_json
from .ingest import lock


def claim():
    path = ROOT / 'var/run/workspace-owner.json'
    with lock(ROOT / 'var/run/workspace-owner.lock'):
        if path.exists():
            owner = json.loads(path.read_text())
            if owner['host'] != socket.gethostname():
                raise RuntimeError('workspace belongs to another host; stop its services before explicitly releasing ownership')
        else:
            atomic_json(path, dict(host=socket.gethostname(), workspace=str(ROOT)))
    return str(path)
