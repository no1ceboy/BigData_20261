from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
from unittest.mock import patch

import pytest

from freewaypulse.config import ROOT, Settings
from freewaypulse.demo import run_demo


@pytest.fixture
def isolated_settings():
    with tempfile.TemporaryDirectory(prefix='.demo-test-', dir=ROOT) as directory:
        yield replace(Settings.read(), runs_dir=Path(directory) / 'runs')


def test_missing_docker_stops_before_creating_run(isolated_settings):
    with patch('freewaypulse.demo.check', return_value={'status': 'failed', 'checks': {'docker_cli': False}}), \
            patch('freewaypulse.demo.compose') as stage:
        with pytest.raises(ValueError, match='Docker'):
            run_demo(isolated_settings, 'not-started', 100, 0)
    stage.assert_not_called()
    assert not isolated_settings.runs_dir.exists()


def test_existing_run_is_preserved(isolated_settings):
    run = isolated_settings.for_run('existing').root
    run.mkdir(parents=True)
    marker = run / 'keep.txt'
    marker.write_text('previous results', encoding='utf-8')
    with patch('freewaypulse.demo.check', return_value={'status': 'passed'}), \
            patch('freewaypulse.demo.compose') as stage:
        with pytest.raises(ValueError, match='already exists'):
            run_demo(isolated_settings, 'existing', 100, 0)
    stage.assert_not_called()
    assert marker.read_text(encoding='utf-8') == 'previous results'


def test_failed_stage_aborts_and_leaves_failure_report(isolated_settings):
    error = subprocess.CalledProcessError(7, ['docker', 'compose', 'up'])
    with patch('freewaypulse.demo.check', return_value={'status': 'passed'}), \
            patch('freewaypulse.demo.compose', side_effect=error) as stage:
        with pytest.raises(subprocess.CalledProcessError):
            run_demo(isolated_settings, 'failed-run', 100, 0)
    stage.assert_called_once()
    report = json.loads((isolated_settings.for_run('failed-run').root / 'reports/failure.json').read_text())
    assert report['status'] == 'failed'


def test_one_command_uses_same_topic_and_run_and_restarts_without_replay(isolated_settings):
    calls = []
    reports = isolated_settings.for_run('contract-run').root / 'reports'

    def fake_compose(*args, log=None):
        calls.append(args)
        if 'freewaypulse.streaming.cli' in args:
            (reports / 'streaming.json').write_text('{"unit_test": true}', encoding='utf-8')
        if 'freewaypulse.verification' in args:
            verification = {'input_events': 3, 'metrics_5min': 3, 'metrics_15min': 1,
                            'summary_rows': 4, 'traffic_states': {'FREE': 4}}
            (reports / 'verification.json').write_text(json.dumps(verification), encoding='utf-8')

    with patch('freewaypulse.demo.check', return_value={'status': 'passed'}), \
            patch('freewaypulse.demo.compose', side_effect=fake_compose):
        run_demo(isolated_settings, 'contract-run', 2, 3)
    producer = [args for args in calls if 'freewaypulse.producer.replay' in args]
    spark = [args for args in calls if 'freewaypulse.streaming.cli' in args]
    assert len(producer) == 1
    assert len(spark) == 2
    assert spark[0] == spark[1]
    assert producer[0][producer[0].index('--topic') + 1] == spark[0][spark[0].index('--topic') + 1]
    assert producer[0][producer[0].index('--input') + 1].endswith('/processed/observations.csv.gz')
    assert '--finite-demo' in spark[0]
    assert json.loads((reports / 'restart.json').read_text())['outputs_unchanged'] is True
