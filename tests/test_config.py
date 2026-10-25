import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from freewaypulse.config import ROOT, Settings, load_env


class RuntimeConfigurationTest(unittest.TestCase):
    def test_process_overrides_dotenv_and_paths_do_not_depend_on_shell_directory(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {}, clear=True):
            env_file = Path(temporary) / '.env'
            env_file.write_text('STATION_LIMIT=2\nSAMPLE_DIR="data/example sample"\n', encoding='utf-8')
            os.environ['STATION_LIMIT'] = '3'
            load_env(env_file)
            before = Path.cwd()
            try:
                os.chdir(temporary)
                settings = Settings.read()
            finally:
                os.chdir(before)
            self.assertEqual(settings.station_limit, 3)
            self.assertEqual(settings.sample_dir, ROOT / 'data/example sample')
            self.assertEqual(settings.event_limit, None)

    def test_bad_limits_fail_before_creating_outputs(self):
        with patch.dict(os.environ, {'STATION_LIMIT': '0'}):
            with self.assertRaises(ValueError):
                Settings.read()

    def test_dotenv_reports_line_number_for_malformed_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / '.env'
            env_file.write_text('# valid comment\nno-equals-sign\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, ':2:'):
                load_env(env_file)

    def test_source_storage_overlap_is_rejected(self):
        with patch.dict(os.environ, {'RUNS_DIR': 'data/d07-1week-mainline/outputs'}):
            with self.assertRaisesRegex(ValueError, 'overlap'):
                Settings.read()

    def test_dotenv_rejects_invalid_names_and_unclosed_quotes(self):
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / '.env'
            for line in ['1INVALID=value', 'SAMPLE_DIR="unfinished']:
                env_file.write_text(line + '\n', encoding='utf-8')
                with self.subTest(line=line), self.assertRaises(ValueError):
                    load_env(env_file)


if __name__ == '__main__':
    unittest.main()
