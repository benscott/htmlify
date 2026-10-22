import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

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


if __name__ == '__main__':
    unittest.main()
