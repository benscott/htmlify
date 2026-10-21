import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from htmlify.tasks.site import SiteTask
from htmlify.tasks.sitemap import SiteMapTask


class TestSiteTask(unittest.TestCase):

    def test_requires_only_the_database_sitemap_and_does_not_propagate_force(
        self,
    ):
        task = SiteTask(
            domain='gadus.myspecies.info',
            platform_path='/tmp/scratchpads',
            force=True,
        )

        dependency = task.requires()

        self.assertIsInstance(dependency, SiteMapTask)
        self.assertFalse(dependency.force)

    def test_converts_sitemap_paths_to_canonical_urls(self):
        task = SiteTask(
            domain='gadus.myspecies.info',
            platform_path='/tmp/scratchpads',
        )

        self.assertEqual(
            task.path_to_url('specimens'),
            'https://gadus.myspecies.info/specimens',
        )
        self.assertEqual(
            task.path_to_url(''),
            'https://gadus.myspecies.info',
        )

    def test_selects_one_exact_page(self):
        task = SiteTask(
            domain='gadus.myspecies.info',
            platform_path='/tmp/scratchpads',
            page_path='/biblio?unused=1',
        )

        self.assertEqual(
            task.select_links(['', 'biblio', 'biblio/import/form']),
            ['', 'biblio'],
        )
        self.assertTrue(task.is_filtered_run)
        self.assertIn(
            'sites/partial/gadus.myspecies.info',
            task.completion_output().path,
        )

    def test_selects_a_limited_page_prefix_batch(self):
        task = SiteTask(
            domain='gadus.myspecies.info',
            platform_path='/tmp/scratchpads',
            page_prefix='/taxonomy/term/',
            page_offset=1,
            page_limit=2,
        )

        self.assertEqual(
            task.select_links([
                'taxonomy/term',
                'taxonomy/term/8',
                'taxonomy/term/9',
                'taxonomy/term/10',
                'simpletaxonomy/term/11',
            ]),
            ['', 'taxonomy/term/8', 'taxonomy/term/9'],
        )

    def test_prefers_an_existing_local_platform_sites_directory(
        self,
    ):
        with TemporaryDirectory() as temporary_directory:
            platform = Path(temporary_directory) / 'scratchpads2'
            expected = platform / 'sites'
            expected.mkdir(parents=True)

            with patch(
                'htmlify.tasks.site.PLATFORMS_ROOT_PATH',
                Path(temporary_directory) / 'production-platforms',
            ):
                task = SiteTask(
                    domain='weevil.myspecies.info',
                    platform_path=platform,
                )

                self.assertEqual(task.get_platform_sites_path(), expected)

    def test_nested_aliases_use_an_index_symlink_for_the_parent(
        self,
    ):
        with TemporaryDirectory() as temporary_directory:
            sites_root = Path(temporary_directory)
            site_root = sites_root / 'weevil.myspecies.info'
            for node_id in ('1921', '1924'):
                page = site_root / 'node' / node_id / 'index.html'
                page.parent.mkdir(parents=True)
                page.write_text(node_id, encoding='utf-8')

            aliases = [
                ('weevil-phylogeny-phd', 'node/1921'),
                (
                    'weevil-phylogeny-phd/initial-project-description',
                    'node/1924',
                ),
            ]
            with (
                patch('htmlify.tasks.site.SITES_DIR', sites_root),
                patch('htmlify.tasks.site.get_site_aliases', return_value=aliases),
            ):
                task = SiteTask(
                    domain='weevil.myspecies.info',
                    platform_path='/tmp/scratchpads',
                )
                task.create_site_aliases()

                parent = site_root / 'weevil-phylogeny-phd'
                child = parent / 'initial-project-description'
                self.assertTrue(parent.is_dir())
                self.assertFalse(parent.is_symlink())
                self.assertTrue((parent / 'index.html').is_symlink())
                self.assertEqual(
                    (parent / 'index.html').resolve(),
                    (site_root / 'node' / '1921' / 'index.html').resolve(),
                )
                self.assertTrue(child.is_symlink())
                self.assertEqual(
                    child.resolve(),
                    (site_root / 'node' / '1924').resolve(),
                )
                self.assertTrue(task.site_aliases_complete())

    def test_copy_across_assets_preserves_generated_facet_indexes(
        self,
    ):
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_assets = root / 'source-assets'
            sites_dir = root / 'sites'
            source_assets.mkdir()
            (source_assets / 'facet-search.js').write_text(
                'current asset',
                encoding='utf-8',
            )

            destination_assets = (
                sites_dir / 'gadus.myspecies.info' / 'assets'
            )
            facet_index = destination_assets / 'facets' / 'gallery.json'
            facet_index.parent.mkdir(parents=True)
            facet_index.write_text('{"records": []}', encoding='utf-8')
            taxonomy_index = destination_assets / 'taxonomy' / '4.json'
            taxonomy_index.parent.mkdir(parents=True)
            taxonomy_index.write_text('{"nodes": []}', encoding='utf-8')
            (destination_assets / 'stale.js').write_text(
                'stale asset',
                encoding='utf-8',
            )

            with (
                patch('htmlify.tasks.site.ASSETS_DIR', source_assets),
                patch('htmlify.tasks.site.SITES_DIR', sites_dir),
            ):
                task = SiteTask(
                    domain='gadus.myspecies.info',
                    platform_path='/tmp/scratchpads',
                )
                task.copy_across_assets()

            self.assertEqual(
                facet_index.read_text(encoding='utf-8'),
                '{"records": []}',
            )
            self.assertEqual(
                taxonomy_index.read_text(encoding='utf-8'),
                '{"nodes": []}',
            )
            self.assertEqual(
                (destination_assets / 'facet-search.js').read_text(
                    encoding='utf-8',
                ),
                'current asset',
            )
            self.assertFalse((destination_assets / 'stale.js').exists())


if __name__ == '__main__':
    unittest.main()
