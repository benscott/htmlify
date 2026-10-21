import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from htmlify.inventory import build_sitemap_inventory, sitemap_url_count


class TestSitemapInventory(unittest.TestCase):

    def test_counts_urls_and_reports_complete_stale_partial_and_unrun_sites(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sitemap_dir = root / 'sitemaps'
            sites_dir = root / 'published'
            processing_dir = root / 'processing'
            sitemap_dir.mkdir()

            domains = [
                'complete.example.org',
                'stale.example.org',
                'partial.example.org',
                'unrun.example.org',
            ]
            for domain in domains:
                (sitemap_dir / f'{domain}.yaml').write_text(
                    yaml.safe_dump(['', 'node/1', 'taxonomy/term/2']),
                    encoding='utf-8',
                )

            for domain in domains[:3]:
                homepage = sites_dir / domain / 'index.html'
                homepage.parent.mkdir(parents=True)
                homepage.write_text('home', encoding='utf-8')

            complete_marker = (
                processing_dir / 'sites' / 'complete.example.org.complete'
            )
            complete_marker.parent.mkdir(parents=True)
            complete_marker.write_text('14\n', encoding='utf-8')
            sitemap_mtime = (
                sitemap_dir / 'complete.example.org.yaml'
            ).stat().st_mtime
            os.utime(complete_marker, (sitemap_mtime + 1, sitemap_mtime + 1))

            stale_marker = (
                processing_dir / 'sites' / 'stale.example.org.complete'
            )
            stale_marker.write_text('13\n', encoding='utf-8')

            partial_marker = (
                processing_dir / 'sites' / 'partial'
                / 'partial.example.org' / 'selection.complete'
            )
            partial_marker.parent.mkdir(parents=True)
            partial_marker.write_text('14\n', encoding='utf-8')

            inventory = build_sitemap_inventory(
                domains,
                sitemap_dir,
                sites_dir,
                processing_dir,
                '14',
            )

            statuses = {site.domain: site.status for site in inventory.sites}
            self.assertEqual(statuses, {
                'complete.example.org': 'complete',
                'partial.example.org': 'partial',
                'stale.example.org': 'stale',
                'unrun.example.org': 'not run',
            })
            self.assertEqual(inventory.summary, {
                'sites': 4,
                'sitemaps': 4,
                'urls': 12,
                'statuses': {
                    'complete': 1,
                    'not run': 1,
                    'partial': 1,
                    'stale': 1,
                },
            })

    def test_missing_sitemap_has_no_url_count_and_json_report_preserves_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inventory = build_sitemap_inventory(
                ['missing.example.org'],
                root / 'sitemaps',
                root / 'sites',
                root / 'processing',
                '14',
            )
            report = root / 'report.json'
            inventory.write_json(report)
            payload = json.loads(report.read_text(encoding='utf-8'))

            self.assertIsNone(inventory.sites[0].sitemap_urls)
            self.assertFalse(inventory.sites[0].has_run)
            self.assertIsNone(payload['sites'][0]['sitemap_urls'])

    def test_rejects_a_non_list_sitemap(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.yaml'
            path.write_text('node: 12\n', encoding='utf-8')

            with self.assertRaisesRegex(ValueError, 'not a URL list'):
                sitemap_url_count(path)

    def test_counts_standard_sitemap_without_loading_it_as_yaml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'large.yaml'
            path.write_text(
                '# htmlify-sitemap-version: 3\n'
                "- ''\n"
                '- node/1\n'
                '- taxonomy/term/2\n',
                encoding='utf-8',
            )

            with patch(
                'htmlify.inventory.yaml.safe_load',
                side_effect=AssertionError('full YAML parser was used'),
            ):
                self.assertEqual(sitemap_url_count(path), 3)

    def test_reports_inventory_scan_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sitemap_dir = root / 'sitemaps'
            sitemap_dir.mkdir()
            domains = ['b.example.org', 'a.example.org']
            for domain in domains:
                (sitemap_dir / f'{domain}.yaml').write_text(
                    '- node/1\n', encoding='utf-8',
                )
            progress = []

            build_sitemap_inventory(
                domains,
                sitemap_dir,
                root / 'sites',
                root / 'processing',
                '14',
                progress=lambda completed, total, domain: progress.append(
                    (completed, total, domain)
                ),
            )

            self.assertEqual(progress, [
                (1, 2, 'a.example.org'),
                (2, 2, 'b.example.org'),
            ])


if __name__ == '__main__':
    unittest.main()
