import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup
import requests

from htmlify import config
from htmlify.tasks.page import PageTask


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
            soup.find('script', src='/assets/facet-search.js?v=7')['defer']
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

        with (
            patch('htmlify.tasks.page.get_soup', return_value=media),
            patch('htmlify.tasks.page.write_taxonomy_index') as write_index,
        ):
            task.species_page(soup, 8, 4)

        panels = soup.select('.htmlify-species-panel')
        self.assertEqual(
            [panel['data-species-tab'] for panel in panels],
            ['descriptions', 'media'],
        )
        self.assertIn('A description', panels[0].get_text())
        self.assertIsNotNone(panels[1].find('img', src='/media.jpg'))
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
            soup.find('script', src='/assets/species-page.js?v=7')
        )
        write_index.assert_called_once()
