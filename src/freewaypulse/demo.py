"""P2/P3: real Kafka -> teammate Spark/analytics -> verified Parquet demo."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from uuid import uuid4

from freewaypulse.config import ROOT, Settings
from freewaypulse.platform import check


def compose(*args: str, log: Path | None = None):
    command = ['docker', 'compose', *map(str, args)]
    print('+ ' + ' '.join(command), flush=True)
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open('w', encoding='utf-8') as output:
            result = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        if result.returncode:
            print(log.read_text(encoding='utf-8', errors='replace')[-8000:], file=sys.stderr)
            raise subprocess.CalledProcessError(result.returncode, command)
        return
    subprocess.run(command, cwd=ROOT, check=True)


def _container_path(path: Path) -> str:
    return '/workspace/' + path.resolve().relative_to(ROOT).as_posix()


def run_demo(settings: Settings, run_id: str, station_limit: int, event_limit: int) -> Path:
    readiness = check(settings, require_docker=True)
    if readiness['status'] != 'passed':
        raise ValueError('Docker/Compose/Linux containers are required: ' + json.dumps(readiness['checks']))
    paths = settings.for_run(run_id).as_dict()
    run_dir = Path(paths['root'])
    if run_dir.exists():
        raise ValueError(f'Run already exists: {run_id}. Use verify or choose a new ID.')
    topic = settings.topic + '-' + run_id.lower().replace('_', '-')
    if len(topic) > 249:
        raise ValueError('Combined Kafka topic is too long')
    reports = Path(paths['reports'])
    reports.mkdir(parents=True)
    thresholds = {
        'severe_speed_mph': float(os.environ.get('SEVERE_SPEED_MPH', '20')),
        'congested_speed_mph': float(os.environ.get('CONGESTED_SPEED_MPH', '40')),
        'moderate_speed_mph': float(os.environ.get('MODERATE_SPEED_MPH', '55')),
    }
    runtime = {'run_id': run_id, 'topic': topic, 'bootstrap_servers': settings.bootstrap_servers,
               'source': _container_path(settings.observations_csv),
               'station_limit': station_limit, 'event_limit': event_limit,
               'watermark': settings.watermark, 'speed_thresholds_mph': thresholds,
               'mode': 'finite_checkpoint_replay', 'window_15_slide': None,
               'started_at': datetime.now(timezone.utc).isoformat()}
    (reports / 'runtime.json').write_text(json.dumps(runtime, indent=2) + '\n', encoding='utf-8')
    try:
        compose('up', '-d', '--wait', '--wait-timeout', '180', 'kafka')
        compose('build', 'worker')
        # One partition preserves global event order through bounded watermark batches.
        compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', 'kafka:9092', '--create', '--topic', topic,
                '--partitions', '1', '--replication-factor', '1')
        base = ['run', '--rm', '-T', 'worker', 'python', '-m']
        container_run = _container_path(run_dir)
        compose(*base, 'freewaypulse.producer.replay', '--input', runtime['source'],
                '--bootstrap', settings.bootstrap_servers, '--topic', topic,
                '--station-limit', str(station_limit), '--limit', str(event_limit),
                '--rate', str(settings.replay_rate),
                '--report', container_run + '/reports/producer.json', log=reports / 'producer.log')
        stream_args = [*base, 'freewaypulse.streaming.cli', '--run-id', run_id,
                       '--bootstrap-servers', settings.bootstrap_servers, '--topic', topic,
                       '--trigger', 'available_now', '--finite-demo', '--slide-15min', 'none',
                       '--max-offsets-per-trigger', '5000', '--timeout', '900']
        compose(*stream_args, log=reports / 'spark.log')
        verifier = [*base, 'freewaypulse.verification', '--run-dir', container_run,
                    '--source', runtime['source']]
        compose(*verifier, log=reports / 'verify.log')
        first = json.loads((reports / 'verification.json').read_text(encoding='utf-8'))
        shutil.copy2(reports / 'streaming.json', reports / 'streaming-first.json')
        # Resume identical offsets/state; no producer replay, so outputs must stay stable.
        compose(*stream_args, log=reports / 'restart.log')
        compose(*verifier, log=reports / 'verify-restart.log')
        second = json.loads((reports / 'verification.json').read_text(encoding='utf-8'))
        for key in ('input_events', 'metrics_5min', 'metrics_15min', 'summary_rows', 'traffic_states'):
            if first[key] != second[key]:
                raise ValueError(f'Checkpoint restart changed {key}')
        (reports / 'restart.json').write_text(json.dumps(
            {'status': 'passed', 'same_checkpoints': True, 'producer_replayed': False,
             'input_events': second['input_events'], 'outputs_unchanged': True}, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(second, indent=2, ensure_ascii=False))
        print(f'Demo and restart passed: {reports}')
        return run_dir
    except Exception as error:
        (reports / 'failure.json').write_text(json.dumps(
            {'status': 'failed', 'error': str(error)}, indent=2) + '\n', encoding='utf-8')
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run', 'verify', 'doctor', 'down'], nargs='?', default='run')
    parser.add_argument('--run-id')
    parser.add_argument('--station-limit', type=int)
    parser.add_argument('--event-limit', type=int)
    args = parser.parse_args(argv)
    settings = Settings.read()
    if args.action == 'doctor':
        report = check(settings, require_docker=True)
        print(json.dumps(report, indent=2))
        return 0 if report['status'] == 'passed' else 1
    if args.action == 'down':
        compose('down')
        return 0
    if args.action == 'verify':
        if not args.run_id:
            parser.error('verify requires --run-id')
        from freewaypulse.verification import verify
        print(json.dumps(verify(Path(settings.for_run(args.run_id).root), settings.observations_csv), indent=2))
        return 0
    stations = settings.station_limit if args.station_limit is None else args.station_limit
    events = (settings.event_limit or 0) if args.event_limit is None else args.event_limit
    if stations <= 0 or events < 0:
        parser.error('station-limit must be >0; event-limit must be >=0')
    run_id = args.run_id or datetime.now(timezone.utc).strftime('demo-%Y%m%dT%H%M%S-') + uuid4().hex[:6]
    run_demo(settings, run_id, stations, events)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f'Demo failed: {error}', file=sys.stderr)
        raise SystemExit(1)
