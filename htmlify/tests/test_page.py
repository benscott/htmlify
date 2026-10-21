import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup
import requests

from htmlify import config
from htmlify.tasks.page import PageTask, STATIC_ASSET_VERSION


class TestPageContent(unittest.TestCase):

    def test_slickgrid_parser_accepts_legacy_javascript_variants(self):
        task = PageTask(
            url='https://abrusinthailand.myspecies.info/taxonomy/term/74',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <div class="slickgrid-wrapper"><div id="slick-grid"></div></div>
            <script>
              var columns = [
                {"id": "title", "name": "Title"},
                {"id": "taxon", "name": "Taxon"}
              ];
              slickgrid = new Slickgrid(
                '#slick-grid', 'taxon-records', 'page-1'
              );
            </script>
            ''',
            'html.parser',
        )

        self.assertEqual(
            task._get_slickgrid_id(soup),
            ('taxon-records', ['Title', 'Taxon', 'ID']),
        )

    def test_unrecognised_slickgrid_does_not_fail_the_page(self):
        task = PageTask(
            url='https://abrusinthailand.myspecies.info/taxonomy/term/74',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '<div class="slickgrid-wrapper"><div id="slickgrid"></div></div>',
            'html.parser',
        )

        self.assertIs(task._load_slickgrid(soup), soup)
        self.assertIsNotNone(soup.select_one('.slickgrid-wrapper'))

    def test_colorbox_file_link_keeps_the_chrome_free_popup_route(self):
        task = PageTask(
            url='https://aba.myspecies.info/gallery',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '<a href="https://aba.myspecies.info/file-colorboxed/267">'
            'View image</a>',
            'html.parser',
        )

        task.process_links(soup)

        self.assertEqual(soup.a['href'], '/file-colorboxed/267')

    def test_biblio_author_and_keyword_links_open_static_facets(self):
        task = PageTask(
            url='https://abrusinthailand.myspecies.info/node/13',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <a href="/biblio/author/1">V. Bobbarala</a>
            <a href="https://abrusinthailand.myspecies.info/biblio/keyword/2">
              Abrus precatorius
            </a>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        self.assertEqual(
            [link['href'] for link in soup.find_all('a')],
            [
                '/biblio?facet=im_biblio_contributor%3A1',
                '/biblio?facet=im_biblio_keyword%3A2',
            ],
        )

    def test_page_with_legacy_biblio_filter_links_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://abrusinthailand.myspecies.info/node/13',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="abrusinthailand.myspecies.info">'
                '<body><a href="/biblio/author/1">Author</a></body></html>',
                encoding='utf-8',
            )

            with (
                patch(
                    'htmlify.tasks.page.configured_facet_definitions',
                    return_value=[],
                ),
                patch(
                    'htmlify.tasks.page.is_configured_facet_page',
                    return_value=False,
                ),
            ):
                self.assertFalse(task.complete())

    def test_existing_user_references_remain_links(self):
        task = PageTask(
            url=(
                'https://abrusinthailand.myspecies.info/'
                'specimen_observation'
            ),
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <a href="/user/10">Public collector</a>
              <a href="/user/12">Reference-only collector</a>
              <a href="/user/13">Missing collector</a>
            </body></html>
            ''',
            'html.parser',
        )

        with patch(
            'htmlify.tasks.page.db_manager.fetch',
            return_value=[(10,), (12,)],
        ) as fetch:
            task.process_links(soup)

        self.assertEqual(
            [(link.get_text(strip=True), link['href']) for link in soup.find_all('a')],
            [
                ('Public collector', '/user/10'),
                ('Reference-only collector', '/user/12'),
            ],
        )
        self.assertIn('Missing collector', soup.get_text())
        self.assertEqual(
            soup.html['data-htmlify-user-links-version'],
            '2',
        )
        self.assertIn('uid > 0 AND uid IN (10, 12, 13)', fetch.call_args.args[1])

    def test_blocked_database_user_gets_a_static_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            try:
                task = PageTask(
                    url='https://abrusinthailand.myspecies.info/user/33',
                    output_dir=root / 'site',
                )
                with (
                    patch(
                        'htmlify.tasks.page.db_manager.fetch_one',
                        return_value=(33, 'Historic collector', 0),
                    ),
                    patch(
                        'htmlify.tasks.page.is_configured_facet_page',
                        return_value=False,
                    ),
                    patch('htmlify.tasks.page.get_soup') as get_soup,
                ):
                    task.run()

                get_soup.assert_not_called()
                archived = Path(task.output().path).read_text(encoding='utf-8')
                self.assertIn('Historic collector', archived)
                self.assertIn('data-htmlify-user-profile-version="1"', archived)
                self.assertIn('data-htmlify-user-id="33"', archived)
                self.assertIn('href="/"', archived)
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_skipped_user_profile_is_revisited(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            try:
                task = PageTask(
                    url='https://abrusinthailand.myspecies.info/user/10',
                    output_dir=root / 'site',
                )
                task._mark_skipped('main content region is empty')

                self.assertEqual(task._user_profile_id(), 10)
                self.assertFalse(task.complete())
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_page_with_old_user_link_processing_is_revisited(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url=(
                    'https://abrusinthailand.myspecies.info/'
                    'specimen_observation'
                ),
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="abrusinthailand.myspecies.info" '
                'data-htmlify-user-links-version="1">'
                '<body>Reference-only collector</body></html>',
                encoding='utf-8',
            )

            with (
                patch(
                    'htmlify.tasks.page.configured_facet_definitions',
                    return_value=[],
                ),
                patch(
                    'htmlify.tasks.page.is_configured_facet_page',
                    return_value=False,
                ),
            ):
                self.assertFalse(task.complete())

    def test_protocol_relative_ddev_asset_uses_canonical_request_url(self):
        task = PageTask(
            url='https://weevil.myspecies.info/taxonomy/term/8',
            output_dir='/tmp/site',
        )
        response = Mock(content=b'asset')
        original_source = (config.SOURCE_DOMAIN, config.SOURCE_BASE_URL)
        config.SOURCE_DOMAIN = 'weevil.myspecies.info'
        config.SOURCE_BASE_URL = 'http://scratchpads-dev.ddev.site:8080'
        try:
            with patch('htmlify.tasks.page.request', return_value=response) as get:
                content = task._request_url(
                    '//scratchpads-dev.ddev.site:8080/modules/system.css'
                )
        finally:
            config.SOURCE_DOMAIN, config.SOURCE_BASE_URL = original_source

        self.assertEqual(content, b'asset')
        get.assert_called_once_with(
            'https://weevil.myspecies.info/modules/system.css'
        )

    def test_classification_redirect_is_archived(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            response = Mock(
                url='http://scratchpads-dev.ddev.site:8080/taxonomy/term/8',
                text='',
            )
            try:
                task = PageTask(
                    url='https://gadus.myspecies.info/classification/4',
                    output_dir=root / 'site',
                )
                with patch('htmlify.tasks.page.request', return_value=response):
                    task.run()

                archived = Path(task.output().path).read_text(encoding='utf-8')
                self.assertIn('url=/taxonomy/term/8', archived)
                self.assertIn('href="/taxonomy/term/8"', archived)
                self.assertFalse(task.skipped_path.exists())
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_taxonomy_title_and_tabs_only_is_empty(self):
        soup = BeautifulSoup(
            '''
            <div id="region-content">
              <a id="main-content"></a>
              <h1 id="page-title">Anacharis nuttallii</h1>
              <div class="tabs"><a href="/media">Media</a></div>
              <div class="messages">A development notice</div>
            </div>
            ''',
            'html.parser',
        )

        self.assertFalse(PageTask._page_has_content(None, soup))

    def test_taxonomy_description_is_content(self):
        soup = BeautifulSoup(
            '''
            <div id="region-content">
              <h1 id="page-title">Anacharis nuttallii</h1>
              <div class="field-name-body"><p>A species description.</p></div>
            </div>
            ''',
            'html.parser',
        )

        self.assertTrue(PageTask._page_has_content(None, soup))

    def test_media_without_text_is_content(self):
        soup = BeautifulSoup(
            '<div id="region-content"><img src="/sites/example/image.jpg"></div>',
            'html.parser',
        )

        self.assertTrue(PageTask._page_has_content(None, soup))

    def test_skipped_page_is_complete_without_html(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / 'site'
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            try:
                task = PageTask(
                    url='https://gadus.myspecies.info/taxonomy/term/8',
                    output_dir=output_dir,
                )
                task._mark_skipped('empty')

                self.assertTrue(task.complete())
                self.assertFalse(Path(task.output().path).exists())
                self.assertTrue(task.skipped_path.exists())
                self.assertFalse(task.skipped_path.is_relative_to(output_dir))
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_inaccessible_database_route_is_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            response = Mock(status_code=403)
            error = requests.HTTPError(response=response)
            try:
                task = PageTask(
                    url='https://gadus.myspecies.info/weight',
                    output_dir=root / 'site',
                )
                with patch('htmlify.tasks.page.get_soup', side_effect=error):
                    task.run()

                self.assertTrue(task.complete())
                self.assertIn('HTTP 403', task.skipped_path.read_text())
                self.assertFalse(Path(task.output().path).exists())
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir


class TestFacetSearch(unittest.TestCase):

    def test_replaces_facets_and_results_with_static_search(self):
        task = PageTask(
            url='https://phthiraptera.myspecies.info/biblio',
            output_dir=Path('/tmp/phthiraptera-test'),
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <div id="region-sidebar">
                <section class="block-facetapi">
                  <h2 class="block-title">Taxonomic name</h2>
                  <a href="?f[0]=im_field_taxonomic_name:30170">Mammalia (1,254)</a>
                </section>
                <div class="block-scratchpads-biblio-search">Old search</div>
              </div>
              <div id="region-content">
                <h1 id="page-title">Literature</h1>
                <section id="block-system-main"><div class="content">
                  <a href="/node/42">Old result</a>
                </div></section>
              </div>
            </body></html>
            ''',
            'html.parser',
        )

        facets = [{
            'field': 'im_field_taxonomic_name',
            'label': 'Taxonomic name',
            'observed_values': {'30170': 'Mammalia'},
        }]
        with patch('htmlify.tasks.page.write_facet_index') as write_index:
            task.facet_page(soup, facets, [('node', '42')])

        browser = soup.select_one('.htmlify-facet-browser')
        controls = soup.select_one('.htmlify-facet-controls')
        self.assertIsNotNone(browser)
        self.assertIsNotNone(controls)
        self.assertRegex(browser['data-index-url'], r'^/assets/facets/biblio-[a-f0-9]{10}\.json$')
        self.assertIsNotNone(controls.find('input', {'name': 'q'}))
        self.assertIsNotNone(controls.find_parent(id='region-sidebar'))
        self.assertIsNotNone(browser.find_parent(id='region-content'))
        self.assertTrue(
            soup.find(
                'script',
                src=f'/assets/facet-search.js?v={STATIC_ASSET_VERSION}',
            )['defer']
        )
        self.assertIsNone(soup.select_one('.block-facetapi'))
        self.assertIsNone(soup.select_one('.block-scratchpads-biblio-search'))
        self.assertNotIn('Old result', soup.get_text())
        write_index.assert_called_once()

    def test_search_route_is_excluded_from_generic_facets(self):
        task = PageTask(
            url='https://phthiraptera.myspecies.info/search/site/Mammalia',
            output_dir=Path('/tmp/phthiraptera-test'),
        )

        self.assertTrue(task._is_search_page())

    def test_faceted_page_is_incomplete_when_its_json_index_is_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            task = PageTask(
                url='https://phthiraptera.myspecies.info/gallery',
                output_dir=output_dir,
            )
            output_path = Path(task.output().path)
            output_path.parent.mkdir(parents=True)
            output_path.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="phthiraptera.myspecies.info"><body>'
                '<section class="htmlify-facet-browser" '
                'data-index-url="/assets/facets/gallery-test.json"></section>'
                '</body></html>',
                encoding='utf-8',
            )

            self.assertFalse(task.complete())
            index_path = output_dir / 'assets/facets/gallery-test.json'
            index_path.parent.mkdir(parents=True)
            index_path.write_text('{}', encoding='utf-8')
            self.assertTrue(task.complete())


class TestSpeciesPage(unittest.TestCase):

    def test_biological_term_does_not_become_a_generic_facet_page(self):
        self.assertFalse(
            PageTask._should_build_facet_page(
                species_vid=4,
                facet_definitions=[],
                configured_facet_page=True,
            )
        )
        self.assertTrue(
            PageTask._should_build_facet_page(
                species_vid=None,
                facet_definitions=[],
                configured_facet_page=True,
            )
        )

    def test_expands_slickgrid_before_copying_a_species_tab(self):
        task = PageTask(
            url='https://abrusinthailand.myspecies.info/taxonomy/term/74',
            output_dir=Path('/tmp/abrus-species-test'),
        )
        soup = BeautifulSoup(
            '''
            <html><body><div id="region-content"><div class="region-content-inner">
              <div class="tabs"><ul class="tabs">
                <li><a href="/taxonomy/term/74">Description</a></li>
                <li><a href="/taxonomy/term/74/specimens">Specimens</a></li>
              </ul></div>
              <article>A description</article>
            </div></div></body></html>
            ''',
            'html.parser',
        )
        specimens = BeautifulSoup(
            '''
            <html><head><script>
              var columns = [{"id": "title", "name": "Title"}];
              slickgrid = new Slickgrid('#slickgrid', 'specimen_records', 'page_1');
            </script></head><body>
              <div id="region-content"><div class="region-content-inner">
                <div class="tabs"><ul class="tabs"></ul></div>
                <div class="slickgrid-wrapper"><div id="slickgrid"></div></div>
              </div></div>
            </body></html>
            ''',
            'html.parser',
        )

        with (
            patch('htmlify.tasks.page.get_soup', return_value=specimens),
            patch(
                'htmlify.tasks.page.request_json',
                return_value={'data': [{'Title': 'Specimen one', 'ID': '42'}]},
            ) as request_grid,
        ):
            task.species_page(soup, 74, 3)

        specimen_panel = soup.select_one(
            '.htmlify-species-panel[data-species-tab="specimens"]'
        )
        self.assertIsNotNone(specimen_panel.find('table'))
        self.assertIn('Specimen one', specimen_panel.get_text())
        self.assertIsNone(specimen_panel.select_one('.slickgrid-wrapper'))
        request_grid.assert_called_once_with(
            'https://abrusinthailand.myspecies.info/'
            'slickgrid/get/data/specimen_records/0/100'
        )

    def test_combines_rendered_tabs_and_replaces_tinytax(self):
        task = PageTask(
            url='https://gadus.myspecies.info/taxonomy/term/8',
            output_dir=Path('/tmp/gadus-species-test'),
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <section class="block block-tinytax" id="block-tinytax-4">
                <h2>Invasive Species</h2><div class="content"></div>
              </section>
              <section class="block-facetapi">Live Solr facet</section>
              <div id="region-content" class="region region-content">
                <div class="region-content-inner">
                  <h1 id="page-title">Anacharis nuttallii</h1>
                  <div class="tabs"><ul class="tabs primary">
                    <li><a href="/taxonomy/term/8">Descriptions</a></li>
                    <li><a href="/taxonomy/term/8/media">Media</a></li>
                    <li><a href="/taxonomy/term/8/maps">Maps</a></li>
                  </ul></div>
                  <article>A description</article>
                </div>
              </div>
            </body></html>
            ''',
            'html.parser',
        )
        media = BeautifulSoup(
            '''
            <div id="region-content"><div class="region-content-inner">
              <h1>Species</h1><div class="tabs"><ul class="tabs"></ul></div>
              <div class="view-species-media"><img src="/media.jpg"></div>
            </div></div>
            ''',
            'html.parser',
        )
        maps = BeautifulSoup(
            '''
            <div id="region-content"><div class="region-content-inner">
              <div class="tabs"><ul class="tabs"></ul></div>
              <section id="block-views-species-map-block">
                <h2>Distribution</h2>
                <div class="gm3-container"><div id="gm3-map"></div></div>
              </section>
            </div></div>
            ''',
            'html.parser',
        )

        with (
            patch('htmlify.tasks.page.get_soup', side_effect=[media, maps]),
            patch('htmlify.tasks.page.write_taxonomy_index') as write_index,
        ):
            task.species_page(soup, 8, 4)

        panels = soup.select('.htmlify-species-panel')
        self.assertEqual(
            [panel['data-species-tab'] for panel in panels],
            ['descriptions', 'media'],
        )
        self.assertIn('A description', panels[0].get_text())
        self.assertEqual(soup.get_text().count('A description'), 1)
        self.assertIsNotNone(panels[1].find('img', src='/media.jpg'))
        self.assertIsNone(panels[0].find('h2', recursive=False))
        self.assertIsNone(panels[1].find('h2', recursive=False))
        self.assertIsNone(
            soup.select_one('[data-species-tab="maps"]')
        )
        self.assertEqual(
            soup.select_one('.htmlify-taxonomy-tree')['data-index-url'],
            '/assets/taxonomy/4.json',
        )
        self.assertIsNone(soup.select_one('.block-tinytax'))
        self.assertIsNone(soup.select_one('.block-facetapi'))
        self.assertEqual(
            soup.select_one('.htmlify-taxonomy-search input')['name'],
            'q',
        )
        self.assertEqual(
            soup.select_one('.htmlify-species-tabs a')['href'],
            '#species-tab-descriptions',
        )
        self.assertIsNotNone(
            soup.find(
                'script',
                src=f'/assets/species-page.js?v={STATIC_ASSET_VERSION}',
            )
        )
        write_index.assert_called_once()

    def test_removes_retired_species_api_blocks(self):
        soup = BeautifulSoup(
            '''
            <section class="block-ncbi"><h2>NCBI</h2></section>
            <section class="block-iucn"><h2>IUCN</h2></section>
            <section class="block-refindit"><h2>ReFindIt</h2></section>
            <section class="block-bhl"><h2>BHL</h2></section>
            <section class="local-description">A retained description</section>
            ''',
            'html.parser',
        )

        PageTask._remove_obsolete_species_blocks(soup)

        self.assertIsNone(soup.select_one('.block-ncbi'))
        self.assertIsNone(soup.select_one('.block-iucn'))
        self.assertIsNone(soup.select_one('.block-refindit'))
        self.assertIsNone(soup.select_one('.block-bhl'))
        self.assertIn('A retained description', soup.get_text())

    def test_empty_species_tab_scaffolding_is_not_content(self):
        empty = BeautifulSoup(
            '''
            <section class="htmlify-species-panel">
              <h2>Maps</h2><section><h2>Distribution</h2>
              <div class="gm3-container"><div id="gm3-map"></div></div>
              </section>
            </section>
            ''',
            'html.parser',
        ).section
        populated = BeautifulSoup(
            '<section><h2>Media</h2><img src="/specimen.jpg"></section>',
            'html.parser',
        ).section

        self.assertFalse(PageTask._species_panel_has_content(empty))
        self.assertTrue(PageTask._species_panel_has_content(populated))

    def test_final_species_panel_pruning_removes_content_lost_later(self):
        task = PageTask(
            url='https://abrusinthailand.myspecies.info/taxonomy/term/74',
            output_dir=Path('/tmp/abrus-species-test'),
        )
        soup = BeautifulSoup(
            '''
            <div class="tabs htmlify-species-tabs"><ul role="tablist">
              <li><a class="active" data-species-tab="descriptions"
                aria-selected="true">Descriptions</a></li>
              <li><a data-species-tab="media"
                aria-selected="false">Media</a></li>
            </ul></div>
            <section class="htmlify-species-panel"
              data-species-tab="descriptions"><h2>Descriptions</h2></section>
            <section class="htmlify-species-panel"
              data-species-tab="media"><h2>Media</h2>
              <img src="/media.jpg"></section>
            ''',
            'html.parser',
        )

        task._prune_empty_species_panels(soup)

        self.assertIsNone(
            soup.select_one('[data-species-tab="descriptions"]')
        )
        media_panel = soup.select_one(
            '.htmlify-species-panel[data-species-tab="media"]'
        )
        self.assertIsNotNone(media_panel)
        self.assertIsNone(media_panel.find('h2', recursive=False))
        media_link = soup.select_one(
            '.htmlify-species-tabs a[data-species-tab="media"]'
        )
        self.assertIn('active', media_link['class'])
        self.assertEqual(media_link['aria-selected'], 'true')
