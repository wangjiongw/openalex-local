"""Explicit projection compatibility; deployment-only changes do not rewrite receipts."""
import ast
import hashlib
import json
from pathlib import Path

from .config import CODE_ROOT, RESOURCE_ROOT


def engine_semantics(path):
    tree = ast.parse(Path(path).read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.Assign, ast.AnnAssign))
             and not (isinstance(n, ast.FunctionDef) and n.name == 'engine_config')]
    return hashlib.sha256(ast.dump(ast.Module(body=nodes, type_ignores=[]), include_attributes=False).encode()).hexdigest()


def supports(manifest):
    if manifest.get('engine') != 'manticore' or manifest.get('engine_version') != '29.9.0':
        return False
    entries = json.loads((RESOURCE_ROOT / 'projection-compatibility.json').read_text())
    entry = entries.get(manifest.get('projection_signature'))
    if not entry or entry['layout'] != manifest.get('physical_layout', 'legacy'):
        return False
    package = CODE_ROOT / 'openalex_api'
    return (engine_semantics(package / 'manticore_engine.py') == entry['engine_semantics']
            and all(hashlib.sha256((package / name).read_bytes()).hexdigest() == digest
                    for name, digest in entry['files'].items()))
