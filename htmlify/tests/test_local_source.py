import os
import unittest
from unittest.mock import patch

from htmlify import config
from htmlify.url import URL
from htmlify.utils import canonicalize_source_references, get_source_url


class TestLocalSource(unittest.TestCase):

    def setUp(self):
        self.original = (config.SOURCE_DOMAIN, config.SOURCE_BASE_URL)
        config.SOURCE_DOMAIN = 'gadus.myspecies.info'
        config.SOURCE_BASE_URL = 'http://scratchpads-dev.ddev.site:8080'

    def tearDown(self):
        config.SOURCE_DOMAIN, config.SOURCE_BASE_URL = self.original

    def test_canonical_request_is_sent_to_ddev(self):
        self.assertEqual(
            get_source_url('https://gadus.myspecies.info/gallery?page=2'),
            'http://scratchpads-dev.ddev.site:8080/gallery?page=2',
        )

    def test_external_request_is_unchanged(self):
        self.assertEqual(
            get_source_url('https://example.org/image.png'),
            'https://example.org/image.png',
        )

    def test_ddev_absolute_link_is_normalised_to_canonical_domain(self):
        url = URL(
            'http://scratchpads-dev.ddev.site:8080/gallery?page=2',
            'gadus.myspecies.info',
        )

        self.assertTrue(url.is_site_internal_link())
        self.assertEqual(
            url.get_normalised(),
            'https://gadus.myspecies.info/gallery?page=2',
        )

    def test_inline_plain_and_json_escaped_origins_are_canonicalized(self):
        html = (
            'http://scratchpads-dev.ddev.site:8080/image.png '
            r'http:\/\/scratchpads-dev.ddev.site:8080/ajax'
        )

        result = canonicalize_source_references(html, 'gadus.myspecies.info')

        self.assertNotIn('scratchpads-dev.ddev.site', result)
        self.assertIn('https://gadus.myspecies.info/image.png', result)
        self.assertIn(r'https:\/\/gadus.myspecies.info/ajax', result)

    @patch.dict(os.environ, {}, clear=False)
    def test_local_source_configuration_is_inherited_by_workers(self):
        original = (
            config.SOURCE_DOMAIN,
            config.SOURCE_BASE_URL,
            config.DB_HOST_OVERRIDE,
            config.DB_PORT_OVERRIDE,
            config.DB_NAME_OVERRIDE,
            config.DB_USERNAME,
            config.DB_PASSWORD,
            config.SOLR_BASE_URL,
            config.DRUPAL_FACET_CONFIG_PATH,
            config.WRITE_APACHE_VHOSTS,
        )
        try:
            config.configure_local_source(
                domain='gadus.myspecies.info',
                base_url='http://scratchpads-dev.ddev.site:8080',
                db_host='127.0.0.1',
                db_port=49154,
                db_name='db',
                db_username='db',
                db_password='db',
                solr_url='http://scratchpads-dev.ddev.site:8983/solr/collection1',
                facet_config_path='/tmp/gadus-facets.json',
            )

            self.assertEqual(
                os.environ['SOURCE_BASE_URL'],
                'http://scratchpads-dev.ddev.site:8080',
            )
            self.assertEqual(os.environ['SOURCE_DOMAIN'], 'gadus.myspecies.info')
            self.assertEqual(os.environ['DB_PORT_OVERRIDE'], '49154')
            self.assertEqual(
                os.environ['SOLR_BASE_URL'],
                'http://scratchpads-dev.ddev.site:8983/solr/collection1',
            )
            self.assertEqual(os.environ['WRITE_APACHE_VHOSTS'], '0')
            self.assertEqual(
                os.environ['DRUPAL_FACET_CONFIG_PATH'],
                '/tmp/gadus-facets.json',
            )
        finally:
            (
                config.SOURCE_DOMAIN,
                config.SOURCE_BASE_URL,
                config.DB_HOST_OVERRIDE,
                config.DB_PORT_OVERRIDE,
                config.DB_NAME_OVERRIDE,
                config.DB_USERNAME,
                config.DB_PASSWORD,
                config.SOLR_BASE_URL,
                config.DRUPAL_FACET_CONFIG_PATH,
                config.WRITE_APACHE_VHOSTS,
            ) = original
