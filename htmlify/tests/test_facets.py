import json
import tempfile
import unittest
from pathlib import Path

import phpserialize
from bs4 import BeautifulSoup

from htmlify.facets import (
    build_facet_index,
    configured_facet_definitions,
    extract_facet_definitions,
    extract_result_hints,
    facet_configuration_fingerprint,
    is_configured_facet_page,
    merge_facet_definitions,
    write_facet_index,
)


def serialized(value):
    return phpserialize.dumps(value, charset='utf-8')


FACETS = [
    {
        'field': 'im_field_taxonomic_name',
        'label': 'Taxonomic name',
        'observed_values': {'30170': 'Mammalia'},
    },
    {
        'field': 'im_biblio_contributor',
        'label': 'Author',
        'observed_values': {},
    },
]


def database_rows(_domain, query):
    normalized = ' '.join(query.split())
    if 'FROM apachesolr_search_page' in normalized:
        return [(
            'biblio',
            'solr',
            serialized({'fq': ['entity_type:node', 'bundle:biblio']}),
        )]
    if "name = 'apachesolr_site_hash'" in normalized:
        return [(serialized('abc123'),)]
    if 'FROM taxonomy_term_data' in normalized:
        return [(30170, 'Mammalia')]
    if 'FROM biblio_contributor_data' in normalized:
        return [(4, 'Smith, Jane')]
    raise AssertionError(f'Unexpected query: {normalized}')


class FakeSolr:
    def __init__(self):
        self.calls = []

    def __call__(self, url, params, timeout):
        self.calls.append((url, params, timeout))
        return {
            'response': {
                'numFound': 2,
                'docs': [
                    {
                        'id': 'abc123/node/10',
                        'entity_id': 10,
                        'entity_type': 'node',
                        'bundle': 'biblio',
                        'label': 'Mammals of Britain',
                        'path': 'node/10',
                        # 99999 simulates a Field API reference whose term
                        # has since been deleted from taxonomy_term_data.
                        'im_field_taxonomic_name': [30170, 99999],
                        'im_biblio_contributor': [4],
                    },
                    {
                        'id': 'abc123/node/11',
                        'entity_id': 11,
                        'entity_type': 'node',
                        'bundle': 'biblio',
                        'label': 'Another paper',
                        'path': 'node/11',
                        'im_biblio_contributor': [4],
                    },
                ],
            },
        }


class TestFacetDetection(unittest.TestCase):

    def test_loads_effective_drupal_context_facets_for_any_matching_route(self):
        configuration = {
            'version': 1,
            'contexts': [
                {
                    'name': 'bibliography',
                    'condition_mode': 0,
                    'conditions': ['path'],
                    'paths': ['biblio*'],
                    'facets': [
                        {'field': 'im_biblio_contributor', 'label': 'Author'},
                        {'field': 'tus_biblio_year', 'label': 'Publication year'},
                    ],
                },
                {
                    'name': 'media',
                    'condition_mode': 0,
                    'conditions': ['path'],
                    'paths': ['gallery*', '~gallery/private*'],
                    'facets': [
                        {'field': 'im_field_cc_licence', 'label': 'Licence'},
                    ],
                },
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'facets.json'
            path.write_text(json.dumps(configuration), encoding='utf-8')

            self.assertEqual(
                [facet['field'] for facet in configured_facet_definitions(
                    '/biblio/authors', path,
                )],
                ['im_biblio_contributor', 'tus_biblio_year'],
            )
            self.assertEqual(
                [facet['field'] for facet in configured_facet_definitions(
                    '/gallery', path,
                )],
                ['im_field_cc_licence'],
            )
            self.assertEqual(
                configured_facet_definitions('/gallery/private/1', path),
                [],
            )
            self.assertEqual(
                facet_configuration_fingerprint('/gallery', path),
                'ffe57de9529f',
            )

    def test_rendered_facets_override_config_labels_and_add_values(self):
        merged = merge_facet_definitions(
            [{'field': 'author', 'label': 'Author', 'observed_values': {}}],
            [{
                'field': 'is_uid',
                'label': 'Filter by contributor:',
                'observed_values': {'7': 'Ben'},
            }],
        )

        self.assertEqual(merged, [{
            'field': 'is_uid',
            'label': 'Filter by contributor:',
            'observed_values': {'7': 'Ben'},
        }])

    def test_detects_configured_routes_including_wildcards_but_not_search(self):
        rows = [
            ('biblio', 'solr', serialized({'fq': ['bundle:biblio']})),
            ('nontaxonomy/term/%', 'solr', serialized({'fq': []})),
            ('search/site', 'solr', serialized({})),
        ]
        fetch = lambda _domain, _query: rows

        self.assertTrue(is_configured_facet_page('example.org', '/biblio', fetch))
        self.assertTrue(
            is_configured_facet_page(
                'example.org',
                '/nontaxonomy/term/16',
                fetch,
            )
        )
        self.assertFalse(
            is_configured_facet_page('example.org', '/search/site', fetch)
        )

    def test_extracts_fields_labels_values_and_result_entities(self):
        soup = BeautifulSoup(
            '''
            <div id="region-sidebar">
              <section class="block-facetapi">
                <h2 class="block-title">Taxonomic name</h2>
                <a href="/biblio?f%5B0%5D=im_field_taxonomic_name%3A30170">
                  Mammalia (1,254)
                  <span class="element-invisible">Apply Mammalia filter</span>
                </a>
              </section>
              <section class="block-facetapi">
                <h2>Author</h2>
                <a href="?f[0]=im_biblio_contributor:4">Smith, Jane (23)</a>
              </section>
            </div>
            <section id="block-system-main">
              <a href="/node/10">A paper</a>
              <a href="/file-colorboxed/99">An image</a>
            </section>
            ''',
            'html.parser',
        )

        facets = extract_facet_definitions(soup)

        self.assertEqual([facet['field'] for facet in facets], [
            'im_field_taxonomic_name',
            'im_biblio_contributor',
        ])
        self.assertEqual(facets[0]['label'], 'Taxonomic name')
        self.assertEqual(facets[0]['observed_values']['30170'], 'Mammalia')
        self.assertEqual(extract_result_hints(soup), [('node', '10'), ('file', '99')])

    def test_resolves_empty_facet_blocks_from_drupal_delta_map(self):
        soup = BeautifulSoup(
            '''
            <section class="block-facetapi" id="block-facetapi-hashedauthor">
              <h2 class="block-title">Author</h2>
              <p>There are no facets available to filter on.</p>
            </section>
            <section class="block-facetapi" id="block-facetapi-hashedyear">
              <h2 class="block-title">Publication year</h2>
              <p>There are no facets available to filter on.</p>
            </section>
            <section class="block-facetapi" id="block-facetapi-hashedowner">
              <h2 class="block-title">Filter by contributor:</h2>
              <p>There are no facets available to filter on.</p>
            </section>
            ''',
            'html.parser',
        )
        delta_map = serialized({
            'HashedAuthor': 'apachesolr@solr:block:im_biblio_contributor',
            'HashedYear': 'apachesolr@solr:block:tus_biblio_year',
            'HashedOwner': 'apachesolr@solr:block:author_global',
        })

        facets = extract_facet_definitions(
            soup,
            domain='gadus.myspecies.info',
            fetch=lambda _domain, _query: [(delta_map,)],
        )

        self.assertEqual(
            [(facet['field'], facet['label']) for facet in facets],
            [
                ('im_biblio_contributor', 'Author'),
                ('tus_biblio_year', 'Publication year'),
                ('is_uid', 'Filter by contributor:'),
            ],
        )


class TestFacetIndex(unittest.TestCase):

    def test_media_database_records_include_archived_thumbnail_urls(self):
        facets = [
            {
                'field': 'is_uid',
                'label': 'Contributor',
                'observed_values': {},
            },
            {
                'field': 'im_field_cc_licence',
                'label': 'Licence',
                'observed_values': {},
            },
            {
                'field': 'ss_field_creator',
                'label': 'Creator',
                'observed_values': {},
            },
        ]

        def fetch(_domain, query):
            normalized = ' '.join(query.split())
            if 'FROM apachesolr_search_page' in normalized:
                return [(
                    'gallery',
                    'solr',
                    serialized({'fq': ['entity_type:file']}),
                )]
            if "name = 'apachesolr_site_hash'" in normalized:
                return [(serialized('abc123'),)]
            if 'SELECT fid, filename, uid, type, uri FROM file_managed' in normalized:
                return [(6, 'A kitten image.png', 1, 'image', 'public://photos/A kitten image.png')]
            if normalized == 'SHOW COLUMNS FROM field_data_field_cc_licence':
                return [
                    ('entity_id',),
                    ('deleted',),
                    ('field_cc_licence_licence',),
                ]
            if normalized == 'SHOW COLUMNS FROM field_data_field_creator':
                return [
                    ('entity_id',),
                    ('deleted',),
                    ('field_creator_value',),
                ]
            if 'FROM field_data_field_cc_licence' in normalized:
                return [(6, 2)]
            if 'FROM field_data_field_creator' in normalized:
                return [(6, 'Jane Smith')]
            if 'FROM users' in normalized:
                return [(1, 'Scratchpad Team')]
            raise AssertionError(f'Unexpected query: {normalized}')

        data = build_facet_index(
            'gadus.myspecies.info',
            '/gallery',
            'Media gallery',
            facets,
            [('file', '6')],
            fetch=fetch,
            solr_get=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                ConnectionError('Solr is offline')
            ),
            solr_url='http://solr:8983/solr/collection1',
        )

        self.assertEqual(data['version'], 2)
        self.assertEqual(data['page']['display'], 'thumbnails')
        self.assertEqual(
            data['records'][0]['thumbnail'],
            '/sites/gadus.myspecies.info/files/photos/A%20kitten%20image.png',
        )
        self.assertEqual(data['records'][0]['facets'], {
            'is_uid': ['1'],
            'im_field_cc_licence': ['2'],
            'ss_field_creator': ['Jane Smith'],
        })
        self.assertEqual(data['facets'][1]['values']['2'], 'Attribution CC BY')

    def test_falls_back_to_drupal_database_when_solr_is_unavailable(self):
        facets = [
            {
                'field': 'tus_biblio_year',
                'label': 'Publication year',
                'observed_values': {},
            },
            {
                'field': 'tus_biblio_type',
                'label': 'Publication type',
                'observed_values': {},
            },
        ]

        def fetch(_domain, query):
            normalized = ' '.join(query.split())
            if 'FROM apachesolr_search_page' in normalized:
                return [(
                    'biblio',
                    'solr',
                    serialized({'fq': ['bundle:biblio', 'entity_type:node']}),
                )]
            if "name = 'apachesolr_site_hash'" in normalized:
                return [(serialized('abc123'),)]
            if 'SELECT nid, title, uid, type FROM node' in normalized:
                return [(111, 'Arty', 1, 'biblio')]
            if 'SELECT nid, biblio_year' in normalized:
                return [(111, 1999, 112, None)]
            if 'FROM biblio_types' in normalized:
                return [(112, 'Artwork')]
            raise AssertionError(f'Unexpected query: {normalized}')

        def unavailable_solr(*_args, **_kwargs):
            raise ConnectionError('Solr is offline')

        data = build_facet_index(
            'gadus.myspecies.info',
            '/biblio',
            'Literature',
            facets,
            [('node', '111')],
            fetch=fetch,
            solr_get=unavailable_solr,
            solr_url='http://solr:8983/solr/collection1',
        )

        self.assertEqual(data['records'], [{
            'id': 'database/node/111',
            'title': 'Arty',
            'url': '/node/111',
            'facets': {
                'tus_biblio_year': ['1999'],
                'tus_biblio_type': ['112'],
            },
        }])
        self.assertEqual(data['facets'][1]['values']['112'], 'Artwork')

    def test_builds_index_from_route_filters_and_exact_facet_fields(self):
        solr = FakeSolr()

        data = build_facet_index(
            'phthiraptera.myspecies.info',
            '/biblio',
            'Literature',
            FACETS,
            [('node', '10')],
            fetch=database_rows,
            solr_get=solr,
            solr_url='http://solr:8983/solr/collection1',
        )

        self.assertEqual(len(data['records']), 2)
        self.assertEqual(data['records'][0]['url'], '/node/10')
        self.assertEqual(data['records'][0]['facets']['im_field_taxonomic_name'], ['30170'])
        self.assertEqual(data['facets'][0]['values']['30170'], 'Mammalia')
        self.assertNotIn('99999', data['facets'][0]['values'])
        self.assertEqual(data['facets'][1]['values']['4'], 'Smith, Jane')
        self.assertEqual(solr.calls[0][0], 'http://solr:8983/solr/collection1/select')
        self.assertEqual(solr.calls[0][1]['fq'], [
            'hash:abc123',
            'entity_type:node',
            'bundle:biblio',
        ])

    def test_writes_compact_page_specific_json(self):
        solr = FakeSolr()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'assets' / 'facets' / 'biblio.json'
            write_facet_index(
                'phthiraptera.myspecies.info',
                '/biblio',
                'Literature',
                FACETS,
                [('node', '10')],
                destination,
                fetch=database_rows,
                solr_get=solr,
                solr_url='http://solr:8983/solr/collection1',
            )

            data = json.loads(destination.read_text(encoding='utf-8'))
            self.assertEqual(data['page']['path'], '/biblio')
            self.assertEqual(data['records'][0]['title'], 'Mammals of Britain')

    def test_infers_code_exported_route_filter_from_visible_results(self):
        calls = []

        def fetch(_domain, query):
            normalized = ' '.join(query.split())
            if 'FROM apachesolr_search_page' in normalized:
                return []
            if "name = 'apachesolr_site_hash'" in normalized:
                return [(serialized('abc123'),)]
            if 'FROM taxonomy_term_data' in normalized:
                return [(30170, 'Mammalia')]
            raise AssertionError(f'Unexpected query: {normalized}')

        def solr_get(_url, params, timeout):
            calls.append(params)
            if params['q'] != '*:*':
                return {'response': {'numFound': 1, 'docs': [{
                    'entity_type': 'node',
                    'bundle': 'ecological_interactions',
                }]}}
            return {'response': {'numFound': 1, 'docs': [{
                'id': 'abc123/node/42',
                'entity_id': 42,
                'entity_type': 'node',
                'bundle': 'ecological_interactions',
                'label': 'Host association',
                'path': 'node/42',
                'im_field_taxonomic_name': [30170],
            }]}}

        data = build_facet_index(
            'phthiraptera.myspecies.info',
            '/interactions',
            'Ecological interactions',
            [FACETS[0]],
            [('node', '42')],
            fetch=fetch,
            solr_get=solr_get,
            solr_url='http://solr:8983/solr/collection1',
        )

        self.assertEqual(data['records'][0]['title'], 'Host association')
        self.assertIn('entity_type:(node)', calls[1]['fq'])
        self.assertIn('bundle:(ecological_interactions)', calls[1]['fq'])


if __name__ == '__main__':
    unittest.main()
