import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from htmlify.ddev import _solr_url, export_facet_configuration


class TestDdevConfig(unittest.TestCase):

    @patch('htmlify.ddev.subprocess.run')
    def test_exports_effective_drupal_facet_configuration(self, run):
        run.return_value = CompletedProcess(
            args=[],
            returncode=0,
            stdout=(
                'A harmless Drupal notice\n'
                'HTMLIFY_FACET_CONFIG_BEGIN\n'
                '{"version":1,"contexts":[{"name":"media"}]}\n'
                'HTMLIFY_FACET_CONFIG_END\n'
            ),
            stderr='',
        )

        payload = export_facet_configuration(
            '/tmp/drupal',
            'http://scratchpads-dev.ddev.site:8080',
        )

        self.assertEqual(payload['contexts'][0]['name'], 'media')
        command = run.call_args.args[0]
        self.assertEqual(
            command[:6],
            ['ddev', 'exec', '--raw', '--', 'php', '-r'],
        )
        self.assertIn(
            '$_SERVER[\'HTTP_HOST\'] = "scratchpads-dev.ddev.site";',
            command[6],
        )

    def test_reads_solr_core_url_from_web_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            config_dir = project / '.ddev'
            config_dir.mkdir()
            (config_dir / 'config.yaml').write_text(
                'name: scratchpads-dev\n'
                'web_environment:\n'
                '  - SOLR_DEFAULT_URL=http://scratchpads-dev.ddev.site:8983/solr/collection1\n',
                encoding='utf-8',
            )

            self.assertEqual(
                _solr_url(project),
                'http://scratchpads-dev.ddev.site:8983/solr/collection1',
            )


if __name__ == '__main__':
    unittest.main()
