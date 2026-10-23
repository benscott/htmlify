import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from htmlify.taxonomy import get_term_vocabularies, write_taxonomy_index


class TestTaxonomyIndex(unittest.TestCase):

    def tearDown(self):
        get_term_vocabularies.cache_clear()

    def test_maps_all_terms_to_their_vocabulary(self):
        with patch(
            'htmlify.taxonomy.db_manager.fetch',
            return_value=[(1717, 198), (2498, 198)],
        ):
            self.assertEqual(
                get_term_vocabularies('aipa.myspecies.info'),
                {1717: 198, 2498: 198},
            )

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
