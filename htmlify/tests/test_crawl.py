import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from htmlify.tasks.crawl import CrawlSiteTask


class TestCrawl(unittest.TestCase):

    def test_force_is_not_propagated_to_sitemap_dependency(self):
        task = CrawlSiteTask(domain='gadus.myspecies.info', force=True)

        self.assertFalse(task.requires().force)

    @patch('htmlify.tasks.crawl.get_site_aliases', return_value=[])
    def test_page_get_hrefs_keeps_query_parameters(self, _aliases):
        soup = BeautifulSoup(
            '<a href="/gallery?page=1">Next</a>',
            'html.parser',
        )
        task = CrawlSiteTask(domain='gadus.myspecies.info')

        hrefs = list(task._page_get_hrefs(soup))

        self.assertEqual(
            hrefs,
            ['https://gadus.myspecies.info/gallery?page=1'],
        )

    @patch('htmlify.tasks.crawl.get_site_aliases', return_value=[])
    def test_page_get_hrefs_ignores_external_links(self, _aliases):
        soup = BeautifulSoup(
            '<a href="https://example.org/page">External</a>',
            'html.parser',
        )
        task = CrawlSiteTask(domain='gadus.myspecies.info')

        self.assertEqual(list(task._page_get_hrefs(soup)), [])


if __name__ == '__main__':
    unittest.main()
