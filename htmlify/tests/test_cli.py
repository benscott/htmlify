import json
import unittest
from types import SimpleNamespace

from htmlify.cli import (
    _concise_luigi_error,
    _sitemap_failures_from_result,
)
from htmlify.tasks.sitemap import SiteMapTask


class Scheduler:
    def __init__(self, errors):
        self.errors = errors

    def fetch_error(self, task_id):
        return {'error': self.errors.get(task_id)}


class TestCliFailureReporting(unittest.TestCase):

    def test_concise_luigi_error_uses_final_traceback_line(self):
        error = json.dumps(
            'Runtime error:\nTraceback (most recent call last):\n'
            'mysql.connector.errors.ProgrammingError: database missing',
        )
        self.assertEqual(
            _concise_luigi_error(error),
            'mysql.connector.errors.ProgrammingError: database missing',
        )

    def test_reads_unrecovered_failures_from_parent_luigi_worker(self):
        failed = SiteMapTask(domain='failed.example.org')
        recovered = SiteMapTask(domain='recovered.example.org')
        history = [
            (failed, 'PENDING', True),
            (recovered, 'PENDING', True),
            (failed, 'FAILED', True),
            (recovered, 'FAILED', True),
            (recovered, 'DONE', True),
        ]
        scheduler = Scheduler({
            failed.task_id: json.dumps(
                'Runtime error:\nValueError: failed database query',
            ),
        })
        result = SimpleNamespace(worker=SimpleNamespace(
            _add_task_history=history,
            _scheduler=scheduler,
        ))

        self.assertEqual(_sitemap_failures_from_result(result), [{
            'item': 'failed.example.org',
            'error': 'ValueError: failed database query',
        }])


if __name__ == '__main__':
    unittest.main()
