"""P1-only configuration and storage commands; Python standard library only."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys

from freewaypulse.config import Settings


def initialize_storage(settings: Settings) -> dict:
    settings.validate()
    directories = [settings.sample_dir, settings.runs_dir]
    for path in directories:
        path.mkdir(parents=True, exist_ok=True)
    return {'status': 'passed', 'action': 'init-storage',
            'directories': [str(path) for path in directories]}


def check(settings: Settings, require_docker: bool = False) -> dict:
    settings.validate()
    checks = {
        'python_3_11': sys.version_info[:2] == (3, 11),
        'source_observations_present': settings.observations_csv.is_file(),
        'source_stations_present': settings.stations_csv.is_file(),
        'storage_paths_valid': True,
    }
    if require_docker:
        checks['docker_cli'] = shutil.which('docker') is not None
        if checks['docker_cli']:
            for name, args in [('docker_engine', ['docker', 'info']),
                               ('compose_v2', ['docker', 'compose', 'version'])]:
                try:
                    result = subprocess.run(args, capture_output=True, timeout=15)
                    checks[name] = result.returncode == 0
                except (OSError, subprocess.TimeoutExpired):
                    checks[name] = False
    return {'task': 'P1', 'status': 'passed' if all(checks.values()) else 'failed',
            'action': 'check', 'checks': checks,
            'runtime_services_checked': require_docker,
            'storage': {'sample_dir': str(settings.sample_dir), 'runs_dir': str(settings.runs_dir)}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['config', 'check', 'init-storage'], default='check', nargs='?')
    parser.add_argument('--run-id', default='example-run', help='Show paths only; does not start or create a run')
    parser.add_argument('--docker', action='store_true', help='Also require installed Docker engine and Compose')
    args = parser.parse_args(argv)
    settings = Settings.read()
    if args.action == 'config':
        report = {'task': 'P1', 'status': 'passed', 'action': 'config',
                  'configuration': settings.as_dict(args.run_id)}
    elif args.action == 'init-storage':
        report = initialize_storage(settings)
    else:
        report = check(settings, args.docker)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as error:
        print(f'P1 configuration error: {error}', file=sys.stderr)
        raise SystemExit(1)
