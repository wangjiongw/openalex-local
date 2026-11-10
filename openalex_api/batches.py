"""Bounded sink batches; an oversized individual row is retained in its own batch."""

import orjson


def bounded(rows, max_rows=100000, max_bytes=32 * 1024**2, encode=orjson.dumps):
    batch, size = [], 0
    for row in rows:
        n = len(encode(row)) + 1
        if batch and (len(batch) >= max_rows or size + n > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(row)
        size += n
        if size >= max_bytes or len(batch) >= max_rows:
            yield batch
            batch, size = [], 0
    if batch:
        yield batch


def insert(b, g, table, rows):
    for batch in bounded(rows):
        b.insert(g, table, batch)
