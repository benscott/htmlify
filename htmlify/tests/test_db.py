import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from htmlify import config
from htmlify.db import DBConnectionManager, unreachable_database_hosts


class TestDBConnectionManager(unittest.TestCase):

    def test_local_source_does_not_require_canonical_domain_in_registry(self):
        manager = DBConnectionManager()
        manager._sites = pd.DataFrame(
            [('production_db', 'production-host')],
            columns=['db_name', 'db_host'],
            index=['weevil.info'],
        )
        connection = MagicMock()

        with (
            patch.object(config, 'SOURCE_DOMAIN', 'weevil.myspecies.info'),
            patch.object(config, 'DB_NAME_OVERRIDE', 'db'),
            patch.object(config, 'DB_HOST_OVERRIDE', '127.0.0.1'),
            patch.object(config, 'DB_PORT_OVERRIDE', 49123),
            patch.object(config, 'DB_USERNAME', 'db'),
            patch.object(config, 'DB_PASSWORD', 'db'),
            patch.object(config, 'DB_CONNECT_TIMEOUT', 7),
            patch(
                'htmlify.db.mysql.connector.connect',
                return_value=connection,
            ) as connect,
        ):
            manager.open_connection('weevil.myspecies.info')

        connect.assert_called_once_with(
            host='127.0.0.1',
            port=49123,
            user='db',
            password='db',
            database='db',
            connection_timeout=7,
        )
        connection.cursor.assert_called_once_with()
        manager.close_all_connections()

    def test_database_host_preflight_reports_only_unreachable_targets(self):
        connection = MagicMock()

        def connect(target, timeout):
            if target[0] == 'offline.example':
                raise TimeoutError('timed out')
            return connection

        with patch('htmlify.db.socket.create_connection', side_effect=connect):
            failures = unreachable_database_hosts({
                ('online.example', 3306),
                ('offline.example', 3306),
                ('online.example', 3306),
            }, timeout=3)

        self.assertEqual(failures, [{
            'host': 'offline.example',
            'port': 3306,
            'error': 'timed out',
        }])
        connection.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
