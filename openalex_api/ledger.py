"""Synchronous transaction RPC: only the coordinator commits import checkpoints."""

import os
import time
import uuid

_requests = _responses = _parent = None


def install(requests, responses, parent):
    global _requests, _responses, _parent
    _requests, _responses, _parent = requests, responses, parent


class WorkerLedger:
    def __init__(self):
        self.pending = []

    def execute(self, statement, params=()):
        self.pending.append((statement, params))

    def commit(self):
        token = uuid.uuid4().hex
        _requests.put((token, self.pending))
        self.pending = []
        started = time.monotonic()
        while token not in _responses:
            if time.monotonic() - started > 300:
                raise TimeoutError("checkpoint coordinator unresponsive")
            if os.getppid() != _parent:
                raise RuntimeError("ledger coordinator exited")
            time.sleep(0.01)
        error = _responses.pop(token)
        if error:
            raise RuntimeError("checkpoint commit failed: " + error)

    def close(self):
        self.pending = []


def coordinate(g, requests, responses):
    from .ingest import connect

    db = connect(g)
    db.execute("PRAGMA synchronous=FULL")
    try:
        while True:
            item = requests.get()
            if item is None:
                return
            token, statements = item
            try:
                with db:
                    for sql, params in statements:
                        db.execute(sql, params)
                responses[token] = None
            except Exception as error:
                responses[token] = type(error).__name__ + ": " + str(error)[:500]
    finally:
        db.close()
