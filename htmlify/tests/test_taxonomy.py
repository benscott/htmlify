import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from htmlify.taxonomy import (
    _read_taxonomy_lookup,
    get_biological_terms,
    get_biological_vids,
    get_term_vocabularies,
    prepare_taxonomy_lookup,
    specimen_records_for_term,
    write_taxonomy_index,
)


class TestTaxonomyIndex(unittest.TestCase):

    def tearDown(self):
        _read_taxonomy_lookup.cache_clear()
        get_biological_terms.cache_clear()
        get_biological_vids.cache_clear()
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

    def test_prepared_lookup_is_reused_without_database_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            lookup_dir = Path(directory)
            with (
                patch('htmlify.taxonomy.TAXONOMY_LOOKUP_DIR', lookup_dir),
                patch(
                    'htmlify.taxonomy.db_manager.fetch_one',
                    return_value=(
                        b'a:1:{i:198;i:1;}',
                    ),
                ),
                patch(
                    'htmlify.taxonomy.db_manager.fetch',
                    side_effect=[
                        [(1717, 198), (2498, 12)],
                        [
                            (1717, 31, 'Specimen B', None),
                            (1717, 30, 'Specimen A', 'long/specimen-a'),
                            (1717, 30, 'Specimen A', 'specimen-a'),
                        ],
                    ],
                ) as fetch,
            ):
                prepare_taxonomy_lookup('example.org')
                self.assertEqual(fetch.call_count, 2)

                _read_taxonomy_lookup.cache_clear()
                get_biological_terms.cache_clear()
                get_term_vocabularies.cache_clear()
                fetch.reset_mock()

                self.assertEqual(
                    get_biological_terms('example.org'),
                    {1717: 198},
                )
                self.assertEqual(
                    get_term_vocabularies('example.org'),
                    {1717: 198, 2498: 12},
                )
                self.assertEqual(
                    specimen_records_for_term('example.org', 1717),
                    [
                        {
                            'nid': 31,
                            'title': 'Specimen B',
                            'path': '/node/31',
                        },
                        {
                            'nid': 30,
                            'title': 'Specimen A',
                            'path': '/specimen-a',
                        },
                    ],
                )
                fetch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
