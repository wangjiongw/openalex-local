"""Compatibility entrypoint for the managed storage incident controller."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openalex_api.storage_repair import run
from openalex_api import workflow

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("generation")
    parser.add_argument("--until", default=None)
    args = parser.parse_args()
    from openalex_api.config import generation_path
    import json
    path = generation_path(args.generation) / "execution-run.json"
    until = args.until or (json.loads(path.read_text()).get("until", "complete") if path.exists() else "complete")
    workflow.recover(args.generation, until=until)
