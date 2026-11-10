"""Public lifecycle entrypoints; historical commands remain compatibility paths."""
from pathlib import Path
import json

from .config import ROOT, config, generation_path


def execute(args):
    if args.command == 'adopt':
        from .adoption import prepare
        return prepare(Path(args.source_workspace), args.generation, args.build, args.port)
    if args.command == 'snapshot':
        from . import snapshot
        return snapshot.verify(args.target or config()['snapshot'], args.deep)
    if args.command == 'engines':
        from .engines import ensure
        return ensure(args.offline, args.cache, args.profile, search=args.search)
    if args.command == 'pipeline':
        from . import community_pipeline as pipeline
        if args.action == 'plan':
            return pipeline.plan(args.generation, args.profile)
        if args.action == 'status':
            return json.loads(pipeline.path(args.generation).read_text())
        return pipeline.run(args.generation, args.profile, args.until)
    from .search_versions import active, build_path
    manifest = active(args.generation) if not args.build else None
    build = args.build or (manifest or {}).get('build')
    if not build:
        raise ValueError('provide --build or publish a search version first')
    if args.command == 'verify':
        from .acceptance import verify_remote
        return verify_remote(args.generation, build, Path(args.output), args.base_url, args.remote_client)
    if args.command == 'restore':
        from .operations import restore
        return restore(args.generation, build, Path(args.output))
    raise ValueError('unsupported lifecycle command')
