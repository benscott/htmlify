import unittest
from unittest.mock import Mock, patch

import requests

from htmlify import config
from htmlify.utils import SiteUnavailableError, is_under_maintenance, request


class TestMaintenanceCheck(unittest.TestCase):

    def setUp(self):
        self.original = (config.SOURCE_DOMAIN, config.SOURCE_BASE_URL)

    def tearDown(self):
        config.SOURCE_DOMAIN, config.SOURCE_BASE_URL = self.original

    @patch('htmlify.utils.requests.get')
    def test_local_source_does_not_make_redundant_request(self, get):
        config.SOURCE_DOMAIN = 'gadus.myspecies.info'
        config.SOURCE_BASE_URL = 'http://scratchpads-dev.ddev.site:8080'

        self.assertFalse(is_under_maintenance('gadus.myspecies.info'))
        get.assert_not_called()

    @patch('htmlify.utils.requests.get', side_effect=requests.exceptions.ReadTimeout)
    def test_timeout_fails_the_availability_check(self, get):
        config.SOURCE_DOMAIN = None
        config.SOURCE_BASE_URL = None

        with self.assertRaisesRegex(
            SiteUnavailableError,
            'Cannot read source site',
        ):
            is_under_maintenance('gadus.myspecies.info')

        self.assertEqual(
            get.call_args.args[0],
            f'{config.SCHEME}://gadus.myspecies.info',
        )
        self.assertEqual(get.call_count, 2)
        self.assertEqual(
            get.call_args.kwargs['timeout'],
            (5, config.REQUEST_TIMEOUT),
        )

    @patch('htmlify.utils.requests.get')
    def test_transient_timeout_is_retried(self, get):
        config.SOURCE_DOMAIN = None
        config.SOURCE_BASE_URL = None
        response = Mock(status_code=200, text='Site content')
        response.raise_for_status.return_value = None
        get.side_effect = [requests.exceptions.ReadTimeout(), response]

        self.assertFalse(is_under_maintenance('gadus.myspecies.info'))
        self.assertEqual(get.call_count, 2)

    @patch('htmlify.utils.requests.get')
    def test_maintenance_response_is_detected(self, get):
        config.SOURCE_DOMAIN = None
        config.SOURCE_BASE_URL = None
        get.return_value = Mock(
            status_code=503,
            text='Site under maintenance',
        )

        self.assertTrue(is_under_maintenance('gadus.myspecies.info'))

    @patch('htmlify.utils.requests.get')
    def test_http_error_fails_the_availability_check(self, get):
        config.SOURCE_DOMAIN = None
        config.SOURCE_BASE_URL = None
        response = Mock(status_code=502, text='Bad gateway')
        response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        get.return_value = response

        with self.assertRaisesRegex(
            SiteUnavailableError,
            'returned HTTP 502 after 2 attempts',
        ):
            is_under_maintenance('gadus.myspecies.info')

        self.assertEqual(get.call_count, 2)


class TestLocalRequestCache(unittest.TestCase):

    @patch('htmlify.utils.session.get')
    @patch('htmlify.utils.requests.get')
    def test_configured_local_source_bypasses_shared_cache(
        self,
        uncached_get,
        cached_get,
    ):
        original = (config.SOURCE_DOMAIN, config.SOURCE_BASE_URL)
        config.SOURCE_DOMAIN = 'weevil.myspecies.info'
        config.SOURCE_BASE_URL = 'http://scratchpads-dev.ddev.site:8080'
        uncached_get.return_value = Mock()
        uncached_get.return_value.raise_for_status.return_value = None
        try:
            request('https://weevil.myspecies.info/biblio')
        finally:
            config.SOURCE_DOMAIN, config.SOURCE_BASE_URL = original

        uncached_get.assert_called_once_with(
            'http://scratchpads-dev.ddev.site:8080/biblio',
            allow_redirects=True,
            verify=False,
            timeout=config.REQUEST_TIMEOUT,
        )
        cached_get.assert_not_called()
