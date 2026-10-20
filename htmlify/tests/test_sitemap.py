import unittest
from unittest.mock import patch

from htmlify.tasks.sitemap import SiteMapTask


class TestSiteMapRoutes(unittest.TestCase):

    def test_public_html_route_filter(self):
        accepted = [
            'specimens',
            'tweets',
            'about/project',
        ]
        rejected = [
            '',
            '<front>',
            '<nolink>',
            'admin/content',
            'devel/php',
            'biblio/import',
            'taxonomy/term/%/csv',
            'dwca-export/reference/csv',
            'search/ajax/results',
            'news/feed',
        ]

        for path in accepted:
            with self.subTest(path=path):
                self.assertTrue(SiteMapTask.is_archivable_path(path))

        for path in rejected:
            with self.subTest(path=path):
                self.assertFalse(SiteMapTask.is_archivable_path(path))

    def test_menu_urls_are_filtered(self):
        task = SiteMapTask(domain='gadus.myspecies.info')
        rows = [
            ('about/project',),
            ('biblio/import',),
            ('node/101',),
        ]

        with patch.object(task, '_query', return_value=rows):
            self.assertEqual(
                task.get_menu_urls(),
                ['about/project', 'node/101'],
            )

    def test_compiled_view_urls_are_filtered(self):
        task = SiteMapTask(domain='gadus.myspecies.info')
        rows = [
            ('admin/content/page',),
            ('specimens',),
            ('taxonomy/term/%/csv',),
            ('tweets',),
        ]

        with patch.object(task, '_query', return_value=rows):
            self.assertEqual(task.get_view_urls(), ['specimens', 'tweets'])

    def test_biological_terms_only_add_the_combined_canonical_route(self):
        task = SiteMapTask(domain='gadus.myspecies.info')
        rows = [(8, 4), (16, 6)]

        with (
            patch.object(task, '_get_biological_vids', return_value=(4,)),
            patch.object(task, '_query', return_value=rows),
        ):
            self.assertEqual(
                task.get_term_urls(),
                [
                    'taxonomy/term/8',
                    'taxonomy/term/16',
                    'simpletaxonomy/term/16',
                    'nontaxonomy/term/16',
                ],
            )


if __name__ == '__main__':
    unittest.main()
