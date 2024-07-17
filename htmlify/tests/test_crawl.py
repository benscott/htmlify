import unittest
import mysql.connector
from mysql.connector import Error

from htmlify.config import PROCESSING_DATA_DIR, DB_USERNAME, DB_PASSWORD, SCHEME, logger
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.utils import get_soup

class TestCrawl(unittest.TestCase):

    def setUp(self):
        # Set up the database connection
        self.db_conn = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database='drupal'
        )
        self.cursor = self.db_conn.cursor()
        domain = 'gadus.myspecies.info'
        self.crawl_task = CrawlSiteTask(domain=domain, db_conn=self.db_conn)

    def tearDown(self):
        # Close the database connection
        if self.db_conn.is_connected():
            self.cursor.close()
            self.db_conn.close()

    def test_connection(self):
        # Test if the connection is established
        self.assertTrue(self.db_conn.is_connected(), "Database connection should be established.")

    def test_page_get_hrefs(self):
        url = 'https://gadus.myspecies.info/gallery'
        soup = get_soup(url)

        hrefs = list(self.crawl_task._page_get_hrefs(soup))
        self.assertTrue('http://gadus.myspecies.info/gallery?page=1' in hrefs, "Query params included")


if __name__ == '__main__':
    unittest.main()



if __name__ == '__main__':
    unittest.main()