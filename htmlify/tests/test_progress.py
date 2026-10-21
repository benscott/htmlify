import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from htmlify.progress import (
    clear_progress,
    configure_progress,
    format_duration,
    get_batch_failures,
    initialise_batch_progress,
    initialise_site_progress,
    record_batch_completion,
    record_page_completion,
)


class TestProgress(unittest.TestCase):

    def tearDown(self):
        clear_progress()

    def test_formats_long_durations_compactly(self):
        self.assertEqual(format_duration(9.6), '10s')
        self.assertEqual(format_duration(3723), '1h 2m 3s')
        self.assertEqual(format_duration(93784), '1d 2h 3m 4s')

    def test_reports_elapsed_rate_and_eta_without_counting_cached_pages_as_work(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'progress.json'
            output = io.StringIO()
            with redirect_stdout(output):
                configure_progress(state_path, interval=0)
                initialise_site_progress('example.org', 10, now=100)
                record_page_completion(
                    'example.org', processed=False, now=100,
                )
                record_page_completion(
                    'example.org', processed=False, now=100,
                )
                record_page_completion(
                    'example.org', processed=True, now=110,
                )
                record_page_completion(
                    'example.org', processed=True, now=110,
                )

            final_line = output.getvalue().splitlines()[-1]
            self.assertIn('4/10 pages (40.0%)', final_line)
            self.assertIn('elapsed 10s', final_line)
            self.assertIn('ETA 30s', final_line)
            self.assertIn('0.20 pages/s', final_line)
            state = json.loads(state_path.read_text(encoding='utf-8'))
            self.assertEqual(state['sites']['example.org']['completed'], 4)
            self.assertEqual(state['sites']['example.org']['processed'], 2)

    def test_reinitialising_a_dynamic_task_does_not_reset_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'progress.json'
            with redirect_stdout(io.StringIO()):
                configure_progress(state_path, interval=60)
                initialise_site_progress('example.org', 10, now=100)
                record_page_completion('example.org', processed=True, now=101)
                initialise_site_progress('example.org', 12, now=102)

            state = json.loads(state_path.read_text(encoding='utf-8'))
            site = state['sites']['example.org']
            self.assertEqual(site['total'], 12)
            self.assertEqual(site['completed'], 1)

    def test_initial_progress_counts_pages_already_on_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'progress.json'
            with redirect_stdout(io.StringIO()):
                configure_progress(state_path, interval=60)
                initialise_site_progress(
                    'example.org', 10, completed=7, now=100,
                )

            site = json.loads(
                state_path.read_text(encoding='utf-8')
            )['sites']['example.org']
            self.assertEqual(site['total'], 10)
            self.assertEqual(site['completed'], 7)

    def test_batch_progress_reports_successes_failures_and_eta(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / 'progress.json'
            output = io.StringIO()
            with redirect_stdout(output):
                configure_progress(state_path, interval=0)
                initialise_batch_progress('Sitemaps', 10, now=100)
                record_batch_completion(
                    'Sitemaps', processed=False, failed=False, now=100,
                )
                record_batch_completion(
                    'Sitemaps', processed=True, failed=False, now=110,
                )
                record_batch_completion(
                    'Sitemaps',
                    processed=True,
                    failed=True,
                    item='broken.example.org',
                    error='database missing',
                    now=110,
                )

            final_line = output.getvalue().splitlines()[-1]
            self.assertIn('3/10 sites (30.0%)', final_line)
            self.assertIn('2 succeeded', final_line)
            self.assertIn('1 failed', final_line)
            self.assertIn('elapsed 10s', final_line)
            self.assertIn('ETA 35s', final_line)
            self.assertEqual(get_batch_failures('Sitemaps'), [{
                'item': 'broken.example.org',
                'error': 'database missing',
            }])


if __name__ == '__main__':
    unittest.main()
