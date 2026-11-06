import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import Mock, patch

import pandas as pd
from luigi.execution_summary import LuigiStatusCode
from typer.testing import CliRunner

from htmlify.cli import cli
from htmlify.tasks.migrate import MigrateTask


class TestMigrationOrdering(unittest.TestCase):

    @staticmethod
    def sites():
        return pd.DataFrame({
            'domain': [
                'charlie.myspecies.info',
                'alpha.myspecies.info',
                'bravo.myspecies.info',
            ],
            'resolved_domain': [
                'charlie.myspecies.info',
                'alpha.myspecies.info',
                'bravo.myspecies.info',
            ],
        })

    def test_orders_sites_by_name_in_either_direction(self):
        ascending = MigrateTask(
            domain='', platform_path='', sort_by='name', sort_order='asc',
        )
        descending = MigrateTask(
            domain='', platform_path='', sort_by='name', sort_order='desc',
        )

        self.assertEqual(
            ascending.order_sites(self.sites()).resolved_domain.tolist(),
            [
                'alpha.myspecies.info',
                'bravo.myspecies.info',
                'charlie.myspecies.info',
            ],
        )
        self.assertEqual(
            descending.order_sites(self.sites()).resolved_domain.tolist(),
            [
                'charlie.myspecies.info',
                'bravo.myspecies.info',
                'alpha.myspecies.info',
            ],
        )

    def test_orders_sites_by_existing_sitemap_url_count(self):
        with tempfile.TemporaryDirectory() as directory:
            processing = Path(directory)
            sitemap_dir = processing / 'sitemaps'
            sitemap_dir.mkdir()
            counts = {
                'alpha.myspecies.info': 20,
                'bravo.myspecies.info': 5,
                'charlie.myspecies.info': 10,
            }
            for domain, count in counts.items():
                (sitemap_dir / f'{domain}.yaml').write_text(
                    '# htmlify-sitemap-version: test\n'
                    + ''.join(f'- node/{number}\n' for number in range(count)),
                    encoding='utf-8',
                )

            with patch(
                'htmlify.tasks.migrate.config.PROCESSING_DATA_DIR',
                processing,
            ):
                ascending = MigrateTask(
                    domain='',
                    platform_path='',
                    sort_by='url-count',
                    sort_order='asc',
                ).order_sites(self.sites())
                descending = MigrateTask(
                    domain='',
                    platform_path='',
                    sort_by='url-count',
                    sort_order='desc',
                ).order_sites(self.sites())

        self.assertEqual(
            ascending.resolved_domain.tolist(),
            [
                'bravo.myspecies.info',
                'charlie.myspecies.info',
                'alpha.myspecies.info',
            ],
        )
        self.assertEqual(ascending['_sitemap_urls'].tolist(), [5, 10, 20])
        self.assertEqual(
            descending.resolved_domain.tolist(),
            [
                'alpha.myspecies.info',
                'charlie.myspecies.info',
                'bravo.myspecies.info',
            ],
        )

    def test_url_count_order_explains_missing_sitemaps(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                'htmlify.tasks.migrate.config.PROCESSING_DATA_DIR',
                Path(directory),
            ):
                task = MigrateTask(
                    domain='',
                    platform_path='',
                    sort_by='url-count',
                    sort_order='asc',
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    r'Run `uv run htmlify sitemaps` first',
                ):
                    task.order_sites(self.sites())

    def test_batch_excludes_complete_sites_before_applying_limit(self):
        sites = self.sites()
        sites['platform_path'] = '/var/aegir/platforms/scratchpads'
        task = MigrateTask(
            domain=None,
            limit=2,
            offset=0,
            platform_path=None,
            sort_by='name',
            sort_order='asc',
        )

        with patch.object(
            task,
            'archive_is_complete',
            side_effect=lambda domain: domain == 'alpha.myspecies.info',
        ):
            selected = task.select_sites(sites)

        self.assertEqual(
            selected.resolved_domain.tolist(),
            ['bravo.myspecies.info', 'charlie.myspecies.info'],
        )

    def test_non_scratchpads_do_not_require_sitemaps_for_url_ordering(self):
        sites = self.sites()
        sites['platform_path'] = '/var/aegir/platforms/scratchpads'
        sites.loc[len(sites)] = {
            'domain': 'stats.myspecies.info',
            'resolved_domain': 'stats.myspecies.info',
            'platform_path': '/var/aegir/platforms/stats',
        }

        with tempfile.TemporaryDirectory() as directory:
            processing = Path(directory)
            sitemap_dir = processing / 'sitemaps'
            sitemap_dir.mkdir()
            for domain in self.sites().resolved_domain:
                (sitemap_dir / f'{domain}.yaml').write_text(
                    '- node/1\n',
                    encoding='utf-8',
                )

            task = MigrateTask(
                domain=None,
                platform_path=None,
                sort_by='url-count',
                sort_order='asc',
            )
            with (
                patch(
                    'htmlify.tasks.migrate.config.PROCESSING_DATA_DIR',
                    processing,
                ),
                patch.object(task, 'archive_is_complete', return_value=False),
            ):
                selected = task.select_sites(sites)

        self.assertNotIn(
            'stats.myspecies.info',
            selected.resolved_domain.tolist(),
        )


class TestMigrationCommand(unittest.TestCase):

    def test_limit_and_offset_are_passed_to_migrate_task(self):
        runner = CliRunner()
        build_result = Mock(status=LuigiStatusCode.SUCCESS)

        with (
            tempfile.TemporaryDirectory() as directory,
            patch('htmlify.cli._require_database_credentials'),
            patch(
                'htmlify.cli._selected_migration_domains',
                return_value=['bravo.myspecies.info'],
            ),
            patch('htmlify.cli.config.PROCESSING_DATA_DIR', Path(directory)),
            patch('htmlify.cli.luigi.build', return_value=build_result) as build,
        ):
            result = runner.invoke(
                cli,
                [
                    'migrate',
                    '--sort', 'name',
                    '--order', 'asc',
                    '--limit', '2',
                    '--offset', '3',
                ],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        task = build.call_args.args[0][0]
        self.assertEqual(task.limit, 2)
        self.assertEqual(task.offset, 3)

    def test_multiple_sites_run_in_separate_child_processes(self):
        runner = CliRunner()
        domains = [
            'alpha.myspecies.info',
            'bravo.myspecies.info',
        ]

        with (
            patch('htmlify.cli._require_database_credentials'),
            patch(
                'htmlify.cli._selected_migration_domains',
                return_value=domains,
            ),
            patch(
                'htmlify.cli.subprocess.run',
                side_effect=[CompletedProcess([], 0), CompletedProcess([], 0)],
            ) as run,
        ):
            result = runner.invoke(
                cli,
                ['migrate', '--limit', '2', '--workers', '3'],
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(run.call_count, 2)
        commands = [item.args[0] for item in run.call_args_list]
        self.assertEqual(
            [command[command.index('--domain') + 1] for command in commands],
            domains,
        )
        self.assertTrue(all(
            command[command.index('--workers') + 1] == '3'
            for command in commands
        ))

    def test_isolated_batch_continues_but_exits_nonzero_after_failure(self):
        runner = CliRunner()

        with (
            patch('htmlify.cli._require_database_credentials'),
            patch(
                'htmlify.cli._selected_migration_domains',
                return_value=[
                    'alpha.myspecies.info',
                    'bravo.myspecies.info',
                ],
            ),
            patch(
                'htmlify.cli.subprocess.run',
                side_effect=[CompletedProcess([], 1), CompletedProcess([], 0)],
            ) as run,
        ):
            result = runner.invoke(cli, ['migrate', '--limit', '2'])

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertEqual(run.call_count, 2)
        self.assertIn('alpha.myspecies.info (exit code 1)', result.output)


if __name__ == '__main__':
    unittest.main()
