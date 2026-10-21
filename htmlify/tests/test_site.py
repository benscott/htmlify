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

    def test_alias_to_unpublished_node_is_not_created_or_required(self):
        with TemporaryDirectory() as temporary_directory:
            sites_root = Path(temporary_directory)
            site_root = sites_root / 'aba.myspecies.info'
            published = site_root / 'node' / '22' / 'index.html'
            published.parent.mkdir(parents=True)
            published.write_text('published', encoding='utf-8')
            stale_alias = site_root / 'old-unpublished-page'
            stale_alias.symlink_to(site_root / 'node' / '21')
            aliases = [
                ('old-unpublished-page', 'node/21'),
                ('current-page', 'node/22'),
            ]

            with (
                patch('htmlify.tasks.site.SITES_DIR', sites_root),
                patch('htmlify.tasks.site.get_site_aliases', return_value=aliases),
            ):
                task = SiteTask(
                    domain='aba.myspecies.info',
                    platform_path='/tmp/scratchpads',
                )
                sitemap_paths = ['', 'node/22']
                task.create_site_aliases(sitemap_paths)

                self.assertFalse(stale_alias.exists())
                self.assertFalse(stale_alias.is_symlink())
                self.assertTrue((site_root / 'current-page').is_symlink())
                self.assertTrue(task.site_aliases_complete(sitemap_paths))

    def test_legacy_colorbox_pages_contain_only_the_primary_media(self):
        with TemporaryDirectory() as temporary_directory:
            sites_root = Path(temporary_directory)
            site_root = sites_root / 'aba.myspecies.info'
            canonical_page = site_root / 'file' / '267'
            canonical_page.mkdir(parents=True)
            (canonical_page / 'index.html').write_text(
                '''
                <html><body>
                  <header><img src="/logo.png"></header>
                  <div id="block-system-main">
                    <div class="file"><div class="content">
                      <span class="file-styles">
                        <img src="/styles/large/specimen.jpg" title="Specimen">
                      </span>
                      <img src="/licence.png" alt="Licence">
                    </div></div>
                  </div>
                  <footer>Site footer</footer>
                </body></html>
                ''',
                encoding='utf-8',
            )
            compatibility_dir = site_root / 'file-colorboxed'
            compatibility_dir.parent.mkdir(parents=True, exist_ok=True)
            compatibility_dir.symlink_to(site_root / 'file')

            with patch('htmlify.tasks.site.SITES_DIR', sites_root):
                task = SiteTask(
                    domain='aba.myspecies.info',
                    platform_path='/tmp/scratchpads',
                )
                task.create_file_colorbox_aliases()

                compatibility_page = site_root / 'file-colorboxed' / '267'
                self.assertFalse(compatibility_page.is_symlink())
                popup = (compatibility_page / 'index.html').read_text()
                self.assertIn('/styles/large/specimen.jpg', popup)
                self.assertIn('data-htmlify-lightbox-version="1"', popup)
                self.assertNotIn('/logo.png', popup)
                self.assertNotIn('/licence.png', popup)
                self.assertNotIn('Site footer', popup)
                self.assertTrue(task.file_colorbox_aliases_complete())

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
            taxonomy_index.chmod(0o600)
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
                self.assertFalse(task.generated_assets_are_web_readable())
                task.copy_across_assets()
                generated_assets_are_readable = (
                    task.generated_assets_are_web_readable()
                )

            self.assertEqual(
                facet_index.read_text(encoding='utf-8'),
                '{"records": []}',
            )
            self.assertEqual(
                taxonomy_index.read_text(encoding='utf-8'),
                '{"nodes": []}',
            )
            self.assertEqual(taxonomy_index.stat().st_mode & 0o777, 0o644)
            self.assertTrue(generated_assets_are_readable)
            self.assertEqual(
                (destination_assets / 'facet-search.js').read_text(
                    encoding='utf-8',
                ),
                'current asset',
            )
            self.assertFalse((destination_assets / 'stale.js').exists())


if __name__ == '__main__':
    unittest.main()
