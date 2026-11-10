"""Compatibility launcher for the packaged restore command."""
import argparse
import json
from pathlib import Path
from openalex_api.operations import restore

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generation")
    parser.add_argument("build")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(restore(args.generation, args.build, args.output)))
