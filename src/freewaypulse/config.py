"""One configuration shared by host tools and container modules."""
from dataclasses import dataclass
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def load_env(path: Path = ROOT / '.env') -> None:
    """Load simple KEY=value defaults without overriding process environment."""
    if not path.exists():
        return
    for number, line in enumerate(path.read_text(encoding='utf-8-sig').splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, sep, value = line.partition('=')
        if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key.strip()):
            raise ValueError(f'{path}:{number}: expected KEY=value')
        value = value.strip()
        if value.startswith(('\"', "'")):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f'{path}:{number}: unmatched quote')
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


def _path(name: str, default: str) -> Path:
    path = Path(os.environ.get(name, default))
    return (path if path.is_absolute() else ROOT / path).resolve()


def _positive(name: str, default: int, allow_zero: bool = False) -> int:
    value = int(os.environ.get(name, str(default)))
    if value < (0 if allow_zero else 1):
        raise ValueError(f'{name} must be {">=0" if allow_zero else ">0"}')
    return value


@dataclass(frozen=True)
class Settings:
    dataset_dir: Path
    sample_dir: Path
    runs_dir: Path
    station_limit: int
    event_limit: int | None
    replay_rate: int
    bootstrap_servers: str
    topic: str
    watermark: str

    @property
    def observations_csv(self) -> Path:
        return self.dataset_dir / 'processed' / 'observations.csv.gz'

    @property
    def stations_csv(self) -> Path:
        return self.dataset_dir / 'processed' / 'stations.csv'

    def for_run(self, run_id: str) -> 'RunPaths':
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', run_id):
            raise ValueError('run_id must contain 1..64 letters, digits, underscores or hyphens')
        return RunPaths(self.runs_dir / run_id)

    def validate(self) -> None:
        """Keep generated storage separate from the source and repository files."""
        for name, path in [('DATASET_DIR', self.dataset_dir), ('SAMPLE_DIR', self.sample_dir),
                           ('RUNS_DIR', self.runs_dir)]:
            if not path.is_relative_to(ROOT) or path == ROOT:
                raise ValueError(f'{name} must be a directory within the repository')
            if path.exists() and not path.is_dir():
                raise ValueError(f'{name} must point to a directory')
        for name, output in [('SAMPLE_DIR', self.sample_dir), ('RUNS_DIR', self.runs_dir)]:
            if output.is_relative_to(self.dataset_dir) or self.dataset_dir.is_relative_to(output):
                raise ValueError(f'{name} must not overlap source DATASET_DIR')
            if output.parts[len(ROOT.parts)] in {'.git', '.venv', '.tools', 'src', 'tests', 'docs', 'scripts', '.github'}:
                raise ValueError(f'{name} must not use repository code or tool directories')
        if self.sample_dir.is_relative_to(self.runs_dir) or self.runs_dir.is_relative_to(self.sample_dir):
            raise ValueError('SAMPLE_DIR and RUNS_DIR must be separate directories')
        if not self.bootstrap_servers.strip() or not self.topic.strip():
            raise ValueError('Kafka address and topic must not be empty')
        if not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', self.topic) or self.topic in {'.', '..'}:
            raise ValueError('KAFKA_TOPIC must contain 1..128 letters, digits, dots, underscores or hyphens')
        if not re.fullmatch(r'[1-9][0-9]*\s+(seconds?|minutes?|hours?)', self.watermark):
            raise ValueError('SPARK_WATERMARK must be a positive duration, e.g. 10 minutes')

    def as_dict(self, run_id: str = 'example-run') -> dict:
        return {
            'dataset_dir': str(self.dataset_dir), 'observations_csv': str(self.observations_csv),
            'stations_csv': str(self.stations_csv), 'sample_dir': str(self.sample_dir),
            'sample_parquet': str(self.sample_dir / 'traffic_7d.parquet'),
            'sample_events': str(self.sample_dir / 'traffic_events.jsonl'),
            'sample_profile': str(self.sample_dir / 'profile.json'), 'runs_dir': str(self.runs_dir),
            'station_limit': self.station_limit, 'event_limit': self.event_limit,
            'replay_rate': self.replay_rate, 'bootstrap_servers': self.bootstrap_servers,
            'kafka_host_address': 'localhost:29092', 'topic': self.topic, 'watermark': self.watermark,
            'example_run': self.for_run(run_id).as_dict(),
        }

    @classmethod
    def read(cls):
        load_env()
        limit = _positive('DEMO_EVENT_LIMIT', 0, allow_zero=True)
        settings = cls(_path('DATASET_DIR', 'data/d07-1week-mainline'),
                   _path('SAMPLE_DIR', 'data/sample'),
                   _path('RUNS_DIR', 'data/runs'),
                   _positive('STATION_LIMIT', 100), limit or None,
                   _positive('REPLAY_EVENTS_PER_SECOND', 5000),
                   os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                   os.environ.get('KAFKA_TOPIC', 'traffic-events'),
                   os.environ.get('SPARK_WATERMARK', '10 minutes'))
        settings.validate()
        return settings


@dataclass(frozen=True)
class RunPaths:
    """Path contract only: obtaining paths never creates files or runs services."""
    root: Path

    def as_dict(self) -> dict[str, str]:
        names = {
            'root': '', 'input': 'input', 'valid_events': 'valid_events',
            'quarantine': 'quarantine', 'ingest_checkpoint': 'checkpoints/ingest',
            'quarantine_checkpoint': 'checkpoints/quarantine',
            'window_5_checkpoint': 'checkpoints/window_5',
            'window_15_checkpoint': 'checkpoints/window_15',
            'metrics_5min': 'traffic_metrics/5min', 'metrics_15min': 'traffic_metrics/15min',
            'traffic_summary': 'traffic_summary', 'reports': 'reports',
        }
        return {key: str(self.root / suffix) for key, suffix in names.items()}
