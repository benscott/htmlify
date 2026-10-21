import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from htmlify.taxonomy import write_taxonomy_index


class TestTaxonomyIndex(unittest.TestCase):

    def test_generated_index_is_readable_by_the_web_server(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'assets' / 'taxonomy' / '4.json'
            with (
                patch(
                    'htmlify.taxonomy.db_manager.fetch_one',
                    return_value=('Abreae',),
                ),
                patch(
                    'htmlify.taxonomy.db_manager.fetch',
                    return_value=[(72, 'Leguminosae', 0, 0)],
                ),
            ):
                write_taxonomy_index(
                    'abrusinthailand.myspecies.info',
                    4,
                    destination,
                )

            self.assertEqual(destination.stat().st_mode & 0o777, 0o644)
            self.assertEqual(destination.parent.stat().st_mode & 0o777, 0o755)


if __name__ == '__main__':
    unittest.main()
