import contextlib
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from freewaypulse.config import ROOT, Settings
from freewaypulse.platform import check, initialize_storage, main


class PlatformP1Test(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temporary = tempfile.TemporaryDirectory(prefix='.p1-test-', dir=ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.source = self.base / 'source'
        (self.source / 'processed').mkdir(parents=True)
        (self.source / 'processed/observations.csv.gz').write_bytes(b'source-owned-by-data-team')
        (self.source / 'processed/stations.csv').write_bytes(b'station_id\n1\n')
        self.settings = Settings(self.source, self.base / 'sample', self.base / 'runs',
                                 100, None, 5000, 'kafka:9092', 'traffic-events', '10 minutes')

    def test_storage_initialization_is_repeatable_and_preserves_existing_source_and_sample(self):
        self.settings.sample_dir.mkdir()
        supplied_sample = self.settings.sample_dir / 'traffic_7d.parquet'
        supplied_sample.write_bytes(b'completed-data-handoff')
        before = {p: p.read_bytes() for p in self.source.rglob('*') if p.is_file()}
        initialize_storage(self.settings)
        initialize_storage(self.settings)
        self.assertTrue(self.settings.runs_dir.is_dir())
        self.assertEqual(supplied_sample.read_bytes(), b'completed-data-handoff')
        self.assertEqual(before, {p: p.read_bytes() for p in self.source.rglob('*') if p.is_file()})

    def test_unsafe_storage_is_rejected_before_creating_directories(self):
        for output in [self.source, self.source / 'new-output', self.base, ROOT,
                       ROOT / 'src/new-output', ROOT.parent / 'outside-project']:
            with self.subTest(output=output), self.assertRaises(ValueError):
                initialize_storage(replace(self.settings, runs_dir=output))
        self.assertFalse((self.source / 'new-output').exists())
        self.assertFalse(self.settings.sample_dir.exists())

    def test_run_paths_have_separate_checkpoints_and_prevent_path_traversal(self):
        paths = self.settings.for_run('p1-example').as_dict()
        checkpoints = [value for name, value in paths.items() if name.endswith('checkpoint')]
        self.assertEqual(len(checkpoints), len(set(checkpoints)))
        self.assertTrue(all(Path(p).is_relative_to(self.settings.runs_dir) for p in paths.values()))
        self.assertFalse(self.settings.runs_dir.exists())
        for run_id in ['../escape', 'nested/run', 'C:\\escape', '', '.' * 2]:
            with self.subTest(run_id=run_id), self.assertRaises(ValueError):
                self.settings.for_run(run_id)

    def test_default_check_never_calls_docker_or_creates_output(self):
        with patch('freewaypulse.platform.subprocess.run') as command:
            report = check(self.settings)
        command.assert_not_called()
        self.assertFalse(report['runtime_services_checked'])
        self.assertTrue(report['checks']['source_observations_present'])
        self.assertFalse(self.settings.sample_dir.exists())

    def test_optional_docker_readiness_fails_clearly_if_docker_is_absent(self):
        with patch('freewaypulse.platform.shutil.which', return_value=None), \
                patch('freewaypulse.platform.subprocess.run') as command:
            report = check(self.settings, require_docker=True)
        command.assert_not_called()
        self.assertEqual(report['status'], 'failed')
        self.assertFalse(report['checks']['docker_cli'])

    def test_missing_source_is_reported_without_regenerating_data(self):
        self.settings.observations_csv.unlink()
        report = check(self.settings)
        self.assertEqual(report['status'], 'failed')
        self.assertFalse(report['checks']['source_observations_present'])
        self.assertFalse(self.settings.observations_csv.exists())

    def test_config_runs_without_site_packages_from_another_directory(self):
        env = os.environ.copy()
        env['PYTHONPATH'] = str(ROOT / 'src')
        result = subprocess.run([sys.executable, '-S', '-m', 'freewaypulse.platform', 'config',
                                 '--run-id', 'p1-test'], cwd=self.base, env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report['configuration']['dataset_dir'], str(ROOT / 'data/d07-1week-mainline'))
        self.assertEqual(report['configuration']['example_run']['root'], str(ROOT / 'data/runs/p1-test'))

    def test_config_entrypoint_does_not_create_a_run(self):
        with patch('freewaypulse.platform.Settings.read', return_value=self.settings), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            status = main(['config', '--run-id', 'config-only'])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue())['task'], 'P1')
        self.assertFalse(self.settings.runs_dir.exists())


if __name__ == '__main__':
    unittest.main()
