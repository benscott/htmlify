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
            'file-colorboxed/99',
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

    def test_forum_urls_use_the_configured_forum_vocabulary(self):
        task = SiteMapTask(domain='abrusinthailand.myspecies.info')

        with (
            patch.object(task, '_query_one', return_value=(b'i:17;',)),
            patch.object(task, '_query', return_value=[(81,), (92,)]) as query,
        ):
            self.assertEqual(task.get_forum_urls(), ['forum/81', 'forum/92'])

        self.assertIn('WHERE vid = 17', query.call_args.args[0])

    def test_forum_urls_are_empty_when_forum_is_not_configured(self):
        task = SiteMapTask(domain='gadus.myspecies.info')

        with (
            patch.object(task, '_query_one', return_value=None),
            patch.object(task, '_query') as query,
        ):
            self.assertEqual(task.get_forum_urls(), [])

        query.assert_not_called()

    def test_users_include_accounts_referenced_by_public_content(self):
        task = SiteMapTask(domain='abrusinthailand.myspecies.info')

        def query(sql):
            if "type = 'user_reference'" in sql:
                return [('field_collector',)]
            if 'field_data_field_collector' in sql:
                return [(33,), (55,)]
            if 'INNER JOIN comment' in sql:
                return [(44,)]
            if 'INNER JOIN node' in sql:
                return [(33,)]
            if 'status = 1' in sql:
                return [(10,)]
            self.fail(f'Unexpected query: {sql}')

        with patch.object(task, '_query', side_effect=query):
            self.assertEqual(
                task.get_user_urls(),
                ['user/10', 'user/33', 'user/44', 'user/55'],
            )

    def test_unsafe_user_reference_field_name_is_ignored(self):
        task = SiteMapTask(domain='gadus.myspecies.info')
        results = [
            [(10,)],
            [],
            [],
            [('field_collector; DROP TABLE users',)],
        ]

        with patch.object(task, '_query', side_effect=results) as query:
            self.assertEqual(task.get_user_urls(), ['user/10'])

        self.assertEqual(query.call_count, 4)


if __name__ == '__main__':
    unittest.main()
