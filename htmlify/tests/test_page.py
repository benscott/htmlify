import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup
import requests

from htmlify import config
from htmlify.exposed_views import EXPOSED_VIEW_VERSION
from htmlify.tasks.page import (
    PageTask,
    SPECIES_PAGE_VERSION,
    STATIC_ASSET_VERSION,
)


class TestPageContent(unittest.TestCase):

    def test_removes_captcha_before_runtime_images_are_archived(self):
        soup = BeautifulSoup(
            '''
            <html><body>
              <form>
                <div class="captcha">
                  <img src="/es/image_captcha/12345.png">
                  <input name="captcha_response">
                </div>
                <p>Form content</p>
              </form>
            </body></html>
            ''',
            'html.parser',
        )

        PageTask.remove_captcha(soup)

        self.assertIsNone(soup.select_one('.captcha'))
        self.assertIsNone(soup.find('img'))
        self.assertIn('Form content', soup.get_text())

    def test_removes_clustrmaps_widget_without_removing_other_content(self):
        soup = BeautifulSoup(
            '''
            <section class="block">
              <p><a href="http://www4.ClustrMaps.com/counter/maps.php">
                <img src="http://www4.clustrmaps.com/counter/index2.php">
                Locations of visitors to this page
              </a></p>
              <p>Useful sidebar content</p>
            </section>
            ''',
            'html.parser',
        )

        PageTask.remove_clustrmaps(soup)

        self.assertNotIn('clustrmaps.com', str(soup).lower())
        self.assertIn('Useful sidebar content', soup.get_text())

    def test_replaces_simplenews_block_content_with_archive_notice(self):
        soup = BeautifulSoup(
            '''
            <section class="block block-simplenews" id="block-simplenews-12">
              <h2>Newsletter</h2>
              <div class="content">
                <p>Stay informed on our latest news!</p>
                <form class="simplenews-subscribe" id="simplenews-block-form-12">
                  <input name="mail" type="email">
                  <button type="submit">Subscribe</button>
                </form>
                <div class="issues-list">Old issues</div>
              </div>
            </section>
            ''',
            'html.parser',
        )

        PageTask.replace_simplenews_blocks(soup)

        block = soup.select_one('#block-simplenews-12')
        self.assertEqual(block.h2.get_text(strip=True), 'Newsletter')
        self.assertEqual(block['data-htmlify-simplenews-version'], '1')
        self.assertIsNone(block.find('form'))
        self.assertNotIn('Old issues', block.get_text())
        notice = block.select_one('.htmlify-archive-notice[role="note"]')
        self.assertEqual(
            notice.get_text(' ', strip=True),
            'Archived site: Newsletter signup is no longer available.',
        )

    def test_replaces_custom_themed_simplenews_form(self):
        soup = BeautifulSoup(
            '''
            <aside class="custom-newsletter">
              <h2>Updates</h2>
              <form class="simplenews-subscribe">
                <input name="mail"><button>Join</button>
              </form>
            </aside>
            ''',
            'html.parser',
        )

        PageTask.replace_simplenews_blocks(soup)

        self.assertIsNone(soup.find('form'))
        self.assertEqual(soup.h2.get_text(strip=True), 'Updates')
        self.assertIsNotNone(soup.select_one('.htmlify-archive-notice'))

    def test_page_with_live_simplenews_block_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://africanpalms.myspecies.info/content/about-us',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="africanpalms.myspecies.info"><body>'
                '<section class="block block-simplenews">'
                '<form class="simplenews-subscribe"></form>'
                '</section></body></html>',
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
                self.assertEqual(
                    task._last_incomplete_reason,
                    'Simplenews block has not been archived',
                )

    def test_replaces_nonbiological_tinytax_with_static_full_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url=(
                    'https://aipa.myspecies.info/'
                    'nontaxonomy/term/2498'
                ),
                output_dir=Path(directory),
            )
            soup = BeautifulSoup(
                '''
                <html><body class="page-nontaxonomy-term-2498">
                  <section class="block block-tinytax" id="block-tinytax-198">
                    <h2>Insecta</h2>
                    <div class="content"><div class="tinytax">
                      <img class="click plus" alt="Open" src="plus.gif">
                      <a href="/es/nontaxonomy/term/2498">Diptera</a>
                    </div></div>
                  </section>
                </body></html>
                ''',
                'html.parser',
            )

            with (
                patch(
                    'htmlify.tasks.page.vocabulary_for_term',
                    return_value=198,
                ),
                patch('htmlify.tasks.page.write_taxonomy_index') as write_index,
            ):
                task.process_tinytax(soup)

            tree = soup.select_one('.htmlify-taxonomy-tree')
            self.assertIsNotNone(tree)
            self.assertIsNone(soup.select_one('.block-tinytax'))
            self.assertEqual(tree['data-current-tid'], '2498')
            self.assertEqual(tree['data-index-url'], '/assets/taxonomy/198.json')
            self.assertEqual(
                tree['data-term-path'],
                '/es/nontaxonomy/term',
            )
            self.assertIsNotNone(tree.select_one('.htmlify-taxonomy-search'))
            self.assertIsNone(tree.select_one('img.click'))
            self.assertIsNotNone(
                soup.find(
                    'script',
                    src=f'/assets/species-page.js?v={STATIC_ASSET_VERSION}',
                )
            )
            write_index.assert_called_once()

    def test_replaces_every_tinytax_block_on_the_page(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url=(
                    'https://aipa.myspecies.info/'
                    'nontaxonomy/term/2498'
                ),
                output_dir=Path(directory),
            )
            soup = BeautifulSoup(
                '''
                <html><body class="page-nontaxonomy-term-2498">
                  <section class="block block-tinytax" id="block-tinytax-198">
                    <div class="content">
                      <a href="/es/nontaxonomy/term/2498">Diptera</a>
                    </div>
                  </section>
                  <section class="block block-tinytax" id="block-tinytax-4">
                    <div class="content">
                      <a href="/es/taxonomy/term/72">Leguminosae</a>
                    </div>
                  </section>
                </body></html>
                ''',
                'html.parser',
            )

            term_vids = {2498: 198, 72: 4}
            with (
                patch(
                    'htmlify.tasks.page.vocabulary_for_term',
                    side_effect=lambda _domain, tid: term_vids.get(tid),
                ),
                patch('htmlify.tasks.page.write_taxonomy_index') as write_index,
            ):
                task.process_tinytax(soup)

            trees = soup.select('.htmlify-taxonomy-tree')
            self.assertEqual(len(trees), 2)
            self.assertIsNone(soup.select_one('.block-tinytax'))
            self.assertEqual(
                [tree['data-index-url'] for tree in trees],
                ['/assets/taxonomy/198.json', '/assets/taxonomy/4.json'],
            )
            self.assertEqual(
                [tree['data-current-tid'] for tree in trees],
                ['2498', '0'],
            )
            input_ids = [field['id'] for field in soup.select(
                '.htmlify-taxonomy-query'
            )]
            self.assertEqual(len(input_ids), len(set(input_ids)))
            self.assertEqual(write_index.call_count, 2)

    def test_static_tinytax_id_is_not_mistaken_for_live_block_class(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url=(
                    'https://aipa.myspecies.info/'
                    'nontaxonomy/term/2498'
                ),
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            taxonomy_index = (
                Path(directory) / 'assets' / 'taxonomy' / '198.json'
            )
            taxonomy_index.parent.mkdir(parents=True)
            taxonomy_index.write_text('{}', encoding='utf-8')
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="aipa.myspecies.info"><body>'
                '<section id="block-tinytax-198" '
                'class="block block-tinytax-198 htmlify-taxonomy-tree" '
                'data-index-url="/assets/taxonomy/198.json"></section>'
                f'<script src="/assets/species-page.js?v={STATIC_ASSET_VERSION}">'
                '</script></body></html>',
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
                self.assertTrue(task.complete())
                output.write_text(
                    output.read_text(encoding='utf-8').replace(
                        'class="block block-tinytax-198 htmlify-taxonomy-tree"',
                        'class="block block-tinytax block-tinytax-198"',
                    ),
                    encoding='utf-8',
                )
                self.assertFalse(task.complete())

    def test_old_exposed_view_page_is_revisited(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://example.org/people',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="example.org"><body>'
                '<div class="view view-id-people">'
                '<form><div class="views-exposed-form"></div></form>'
                '</div>'
                '</body></html>',
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

    def test_standalone_exposed_form_block_is_not_an_unconverted_view(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://cats.myspecies.info',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="cats.myspecies.info"><body>'
                '<section class="block block-views">'
                '<form action="/catalogue">'
                '<div class="views-exposed-form"></div>'
                '</form></section>'
                '</body></html>',
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
                self.assertTrue(task.complete())

    def test_static_exposed_view_requires_search_script(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://example.org/people',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="example.org" '
                f'data-htmlify-exposed-view-version="{EXPOSED_VIEW_VERSION}">'
                '<body><div class="views-exposed-form"></div></body></html>',
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

    def test_final_biblio_link_pass_is_safe_and_idempotent(self):
        task = PageTask(
            url='https://acanthaceae.myspecies.info/node/4',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <a href="/biblio/author/6" rel="nofollow">V. A. W. Graham</a>
            <a href="/biblio?facet=im_biblio_contributor%3A7">Converted</a>
            <a href="https://example.org/biblio/author/8">External</a>
            ''',
            'html.parser',
        )

        task.process_biblio_links(soup)

        self.assertEqual(
            [link['href'] for link in soup.find_all('a')],
            [
                '/biblio?facet=im_biblio_contributor%3A6',
                '/biblio?facet=im_biblio_contributor%3A7',
                'https://example.org/biblio/author/8',
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

    def test_user_references_become_plain_text(self):
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
              <a href="https://abrusinthailand.myspecies.info/user/12">
                Reference-only collector
              </a>
              <a href="/user/13">Missing collector</a>
              <a href="/users/ibrahim">Aliased collector</a>
              <a href="/es/users/mar%C3%ADa">Language-prefixed collector</a>
              <a href="https://example.org/users/external">External user</a>
            </body></html>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        self.assertEqual(
            [link['href'] for link in soup.find_all('a')],
            ['https://example.org/users/external'],
        )
        self.assertIn('Public collector', soup.get_text())
        self.assertIn('Reference-only collector', soup.get_text())
        self.assertIn('Missing collector', soup.get_text())
        self.assertIn('Aliased collector', soup.get_text())
        self.assertIn('Language-prefixed collector', soup.get_text())
        self.assertEqual(
            soup.html['data-htmlify-user-links-version'],
            '3',
        )

    def test_login_prompt_becomes_visible_archive_notice(self):
        task = PageTask(
            url='https://aipa.myspecies.info/es/forum/2497',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <p><a href="/es/user/login/destination/forum">Log in</a>
                to post new content in the forum.</p>
            </body></html>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        notice = soup.select_one('.htmlify-archive-notice[role="note"]')
        self.assertIsNotNone(notice)
        self.assertEqual(
            notice.get_text(' ', strip=True),
            'Archived site: Posting and login are no longer available.',
        )
        self.assertEqual(soup.find_all('a'), [])

    def test_standalone_auth_actions_are_removed(self):
        task = PageTask(
            url='https://aipa.myspecies.info/es/forum',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <html><body><nav><ul>
              <li><a href="/es/user/login">Log in</a></li>
              <li><a href="/user/register">Create new account</a></li>
              <li><a href="/es/user/password">Request new password</a></li>
            </ul></nav></body></html>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        self.assertEqual(soup.find_all('a'), [])
        self.assertEqual(soup.find_all('li'), [])

    def test_embedded_auth_text_is_retained_and_external_link_is_untouched(self):
        task = PageTask(
            url='https://aipa.myspecies.info/es/forum',
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <div>Account access: <a href="/es/user/login">Log in</a>.</div>
              <a href="https://accounts.example.org/login">Partner login</a>
            </body></html>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        self.assertIn('Account access: Log in', soup.get_text(' ', strip=True))
        self.assertEqual(
            [link['href'] for link in soup.find_all('a')],
            ['https://accounts.example.org/login'],
        )

    def test_malformed_absolute_link_does_not_abort_link_processing(self):
        task = PageTask(
            url=(
                'https://collectionsforall.myspecies.info/'
                'simpletaxonomy/term/26'
            ),
            output_dir='/tmp/site',
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <a href="https://[site:url]/user/login">Broken token link</a>
              <a href="/node/12">Valid internal link</a>
            </body></html>
            ''',
            'html.parser',
        )

        task.process_links(soup)

        self.assertEqual(
            [link['href'] for link in soup.find_all('a')],
            ['https://[site:url]/user/login', '/node/12'],
        )

    def test_page_with_internal_authentication_link_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://aipa.myspecies.info/es/forum/2497',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="aipa.myspecies.info"><body>'
                '<a href="/es/user/login/destination/forum">Log in</a>'
                '</body></html>',
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
                self.assertEqual(
                    task._last_incomplete_reason,
                    'authentication link remains',
                )

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

    def test_page_with_plural_user_alias_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url=(
                    'https://africanpalms.myspecies.info/'
                    'content/aau-herbarium-mg01'
                ),
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="africanpalms.myspecies.info" '
                'data-htmlify-user-links-version="3"><body>'
                '<a href="/users/ibrahim">Collector</a>'
                '</body></html>',
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
                self.assertEqual(
                    task._last_incomplete_reason,
                    'user profile link remains',
                )

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

    def test_overlong_missing_sites_asset_uses_hashed_archive_path(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://africhthy.myspecies.info/file/11678',
                output_dir=Path(directory),
            )
            filename = f'fish-{"%25E0" * 60}.jpg'
            source = (
                '/sites/africhthy.org/files/styles/large/http/'
                f'upload.wikimedia.org/commons/{filename}'
            )

            with (
                patch(
                    'htmlify.tasks.page.config.DRUPAL_FILES_MODE',
                    'symlink',
                ),
                patch.object(
                    task,
                    '_request_url',
                    return_value=b'image data',
                ),
            ):
                archived = task.process_remote_file(source)

            self.assertRegex(
                archived,
                r'^/assets/remote/[0-9a-f]{64}\.jpg$',
            )
            destination = Path(directory) / archived.lstrip('/')
            self.assertEqual(destination.read_bytes(), b'image data')
            self.assertLessEqual(len(destination.name.encode()), 255)

    def test_asset_path_prefix_collision_uses_hashed_archive_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prefix = root / 'images' / 'fish.jpg'
            prefix.parent.mkdir()
            prefix.write_bytes(b'original image')
            task = PageTask(
                url='https://africhthy.myspecies.info/file/2836',
                output_dir=root,
            )

            with patch.object(
                task,
                '_request_url',
                return_value=b'thumbnail data',
            ):
                archived = task.process_remote_file(
                    '/images/fish.jpg/thumbnail.png'
                )

            self.assertRegex(
                archived,
                r'^/assets/remote/[0-9a-f]{64}\.png$',
            )
            self.assertEqual(
                (root / archived.lstrip('/')).read_bytes(),
                b'thumbnail data',
            )
            self.assertEqual(prefix.read_bytes(), b'original image')

    def test_incomplete_drupal_style_image_is_removed_without_request(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://tachinidae.myspecies.info/gallery',
                output_dir=Path(directory),
            )
            soup = BeautifulSoup(
                '<div><img alt="Missing specimen" '
                'src="/sites/tachinidae.myspecies.info/files/styles/'
                'square_thumbnail/public"></div>',
                'html.parser',
            )

            with patch.object(task, '_request_url') as request_url:
                task.process_images(soup)

            request_url.assert_not_called()
            self.assertIsNone(soup.find('img'))

    def test_redirecting_internal_asset_does_not_fail_page_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://tachinidae.myspecies.info/gallery',
                output_dir=Path(directory),
            )
            with patch.object(
                task,
                '_request_url',
                side_effect=requests.TooManyRedirects(
                    'Exceeded 30 redirects',
                ),
            ):
                self.assertIsNone(
                    task.process_remote_file('/images/broken-thumbnail.jpg')
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

    def test_repeated_server_error_on_non_homepage_is_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            response = Mock(
                status_code=500,
                text=(
                    '<html><body><div class="messages error">'
                    'Contact module configuration is invalid.'
                    '</div></body></html>'
                ),
            )
            error = requests.HTTPError(
                '500 Server Error',
                response=response,
            )
            try:
                task = PageTask(
                    url='https://cusucobeetles.myspecies.info/contact',
                    output_dir=root / 'site',
                )
                with patch(
                    'htmlify.tasks.page.get_soup',
                    side_effect=[error, error],
                ) as get_soup:
                    task._run_page()

                self.assertEqual(get_soup.call_count, 2)
                self.assertEqual(
                    get_soup.call_args_list[1].kwargs,
                    {'cached': False},
                )
                self.assertTrue(task.complete())
                self.assertIn(
                    'HTTP 500 after retry: '
                    'Contact module configuration is invalid.',
                    task.skipped_path.read_text(),
                )
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_server_error_on_homepage_still_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            response = Mock(status_code=500)
            error = requests.HTTPError(
                '500 Server Error',
                response=response,
            )
            task = PageTask(
                url='https://cusucobeetles.myspecies.info',
                output_dir=Path(directory),
            )
            with (
                patch('htmlify.tasks.page.get_soup', side_effect=error) as get_soup,
                self.assertRaises(requests.HTTPError),
            ):
                task._run_page()

            get_soup.assert_called_once_with(task.url)

    def test_redirect_loop_on_non_homepage_is_retried_then_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            response = Mock(status_code=302, text='', reason='Found')
            error = requests.TooManyRedirects(
                'Exceeded 30 redirects',
                response=response,
            )
            try:
                task = PageTask(
                    url='https://biotalent.myspecies.info/node/35',
                    output_dir=root / 'site',
                )
                with patch(
                    'htmlify.tasks.page.get_soup',
                    side_effect=[error, error],
                ) as get_soup:
                    task._run_page()

                self.assertEqual(get_soup.call_count, 2)
                self.assertEqual(
                    get_soup.call_args_list[1].kwargs,
                    {'cached': False},
                )
                self.assertTrue(task.complete())
                self.assertIn(
                    'TooManyRedirects after retry: Exceeded 30 redirects',
                    task.skipped_path.read_text(),
                )
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir

    def test_redirect_loop_on_homepage_still_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            response = Mock(status_code=302, text='', reason='Found')
            error = requests.TooManyRedirects(
                'Exceeded 30 redirects',
                response=response,
            )
            task = PageTask(
                url='https://biotalent.myspecies.info',
                output_dir=Path(directory),
            )
            with (
                patch(
                    'htmlify.tasks.page.get_soup',
                    side_effect=error,
                ) as get_soup,
                self.assertRaises(requests.TooManyRedirects),
            ):
                task._run_page()

            get_soup.assert_called_once_with(task.url)

    def test_empty_reply_on_non_homepage_is_retried_then_skipped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_processing_dir = config.PROCESSING_DATA_DIR
            config.PROCESSING_DATA_DIR = root / 'processing'
            error = requests.ConnectionError(
                'Remote end closed connection without response',
            )
            try:
                task = PageTask(
                    url='https://cusucobeetles.myspecies.info/contact',
                    output_dir=root / 'site',
                )
                with patch(
                    'htmlify.tasks.page.get_soup',
                    side_effect=[error, error],
                ) as get_soup:
                    task._run_page()

                self.assertEqual(get_soup.call_count, 2)
                self.assertEqual(
                    get_soup.call_args_list[1].kwargs,
                    {'cached': False},
                )
                self.assertIn(
                    'ConnectionError after retry: '
                    'Remote end closed connection without response',
                    task.skipped_path.read_text(),
                )
            finally:
                config.PROCESSING_DATA_DIR = original_processing_dir


class TestFacetSearch(unittest.TestCase):

    def test_live_facet_sidebar_is_removed_from_an_ordinary_page(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://acanthaceae.myspecies.info/node/1',
                output_dir=Path(directory),
            )
            soup = BeautifulSoup(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="acanthaceae.myspecies.info"><body>'
                '<section class="block-facetapi"><ul '
                'class="facetapi-facetapi-links">'
                '<li><a href="/biblio">Literature</a></li>'
                '</ul></section></body></html>',
                'html.parser',
            )

            task.process_facets(soup)
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(str(soup), encoding='utf-8')

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
                self.assertTrue(task.complete())

            self.assertIsNone(soup.select_one('.block-facetapi'))

    def test_unprocessed_facet_sidebar_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            task = PageTask(
                url='https://acanthaceae.myspecies.info/node/1',
                output_dir=Path(directory),
            )
            output = Path(task.output().path)
            output.parent.mkdir(parents=True)
            output.write_text(
                '<html data-htmlify-page-version="1" '
                'data-htmlify-domain="acanthaceae.myspecies.info"><body>'
                '<section class="block-facetapi">Legacy facet</section>'
                '</body></html>',
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
                self.assertEqual(
                    task._last_incomplete_reason,
                    'Facet API blocks have not been processed',
                )

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

    def test_unavailable_search_page_gets_static_google_form(self):
        task = PageTask(
            url='https://bombus-bg.myspecies.info/search',
            output_dir=Path('/tmp/bombus-bg-test'),
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <section id="section-content">
                <h1>Search</h1>
                <div id="block-system-main"><div class="content">
                  <p>Search is temporarily unavailable</p>
                </div></div>
              </section>
            </body></html>
            ''',
            'html.parser',
        )

        task.search_page(soup)

        form = soup.select_one('form#search-form')
        self.assertIsNotNone(form)
        self.assertEqual(form['action'], 'https://www.google.com/search')
        self.assertEqual(form['method'], 'get')
        self.assertEqual(form.select_one('input[name="q"]')['type'], 'search')
        self.assertEqual(
            form.select_one('input[name="as_sitesearch"]')['value'],
            'bombus-bg.myspecies.info',
        )
        self.assertNotIn('temporarily unavailable', soup.get_text())
        self.assertEqual(soup.h1.get_text(strip=True), 'Search')

    def test_search_unavailable_message_on_node_is_not_a_search_failure(self):
        task = PageTask(
            url='https://bombus-bg.myspecies.info/node/603',
            output_dir=Path('/tmp/bombus-bg-test'),
        )
        soup = BeautifulSoup(
            '<section id="section-content">'
            '<p>Search is temporarily unavailable</p>'
            '</section>',
            'html.parser',
        )

        task.search_page(soup)

        self.assertIsNone(soup.select_one('form#search-form'))
        self.assertIn('Search is temporarily unavailable', soup.get_text())

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

    def test_species_page_without_tabs_is_still_staticized(self):
        task = PageTask(
            url='https://africhthy.myspecies.info/taxonomy/term/10192',
            output_dir=Path('/tmp/africhthy-species-test'),
        )
        soup = BeautifulSoup(
            '''
            <html><body>
              <section class="block block-tinytax" id="block-tinytax-7">
                <h2>Classification</h2><div class="content"></div>
              </section>
              <div id="region-content" class="region region-content">
                <h1>African fish</h1>
                <article>A species page without Drupal tabs.</article>
              </div>
            </body></html>
            ''',
            'html.parser',
        )

        with patch(
            'htmlify.tasks.page.write_taxonomy_index',
        ) as write_index:
            task.species_page(soup, 10192, 7)

        content = soup.select_one('#region-content')
        self.assertEqual(
            content['data-htmlify-species-version'],
            SPECIES_PAGE_VERSION,
        )
        self.assertIn('htmlify-species-page', content['class'])
        self.assertIn('A species page without Drupal tabs.', soup.get_text())
        self.assertEqual(
            soup.select_one('.htmlify-taxonomy-tree')['data-index-url'],
            '/assets/taxonomy/7.json',
        )
        self.assertIsNotNone(
            soup.find(
                'script',
                src=f'/assets/species-page.js?v={STATIC_ASSET_VERSION}',
            )
        )
        write_index.assert_called_once()

    def test_species_page_without_tinytax_gets_fallback_tree_and_index(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task = PageTask(
                url='https://neasianflora.myspecies.info/taxonomy/term/8',
                output_dir=root,
            )
            soup = BeautifulSoup(
                '''
                <html><body>
                  <div id="region-content" class="region region-content">
                    <h1>Species without TinyTax</h1>
                    <article>A species description.</article>
                  </div>
                </body></html>
                ''',
                'html.parser',
            )

            with patch(
                'htmlify.tasks.page.write_taxonomy_index',
            ) as write_index:
                task.species_page(soup, 8, 4)

            expected_index = root / 'assets' / 'taxonomy' / '4.json'
            write_index.assert_called_once_with(
                'neasianflora.myspecies.info',
                4,
                expected_index,
            )
            tree = soup.select_one('.htmlify-taxonomy-tree-fallback')
            self.assertIsNotNone(tree)
            self.assertEqual(tree['data-index-url'], '/assets/taxonomy/4.json')
            self.assertEqual(tree['data-current-tid'], '8')
            self.assertEqual(tree['data-term-path'], '/taxonomy/term')
            self.assertIsNotNone(tree.select_one('.htmlify-taxonomy-search'))
            self.assertIsNotNone(
                soup.find(
                    'script',
                    src=f'/assets/species-page.js?v={STATIC_ASSET_VERSION}',
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
                'htmlify.tasks.page.PageTask._specimen_records',
                return_value=None,
            ),
            patch(
                'htmlify.tasks.page.request_json',
                return_value={'data': [{'Title': 'Specimen one', 'ID': '42'}]},
            ) as request_grid,
            patch('htmlify.tasks.page.write_taxonomy_index'),
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

    def test_builds_species_specimen_list_from_database(self):
        task = PageTask(
            url='https://africanpalms.myspecies.info/taxonomy/term/74',
            output_dir=Path('/tmp/africanpalms-species-test'),
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
              slickgrid = new Slickgrid(
                '#slickgrid', 'species_specimen_observation', 'block'
              );
            </script></head><body>
              <div id="region-content"><div class="region-content-inner">
                <div class="tabs"><ul class="tabs"></ul></div>
                <div class="slickgrid-wrapper"><div id="slickgrid"></div></div>
                <div class="loading-indicator">Loading...</div>
              </div></div>
            </body></html>
            ''',
            'html.parser',
        )

        def fetch(_domain, query):
            if 'FROM node n' in query:
                self.assertIn("n.type = 'specimen_observation'", query)
                self.assertIn('ti.tid = 74', query)
                return [(181, 'AAU Herbarium MG01')]
            if 'FROM url_alias' in query:
                return [('node/181', 'content/aau-herbarium-mg01')]
            self.fail(f'Unexpected specimen query: {query}')

        with (
            patch('htmlify.tasks.page.get_soup', return_value=specimens),
            patch('htmlify.tasks.page.db_manager.fetch', side_effect=fetch),
            patch('htmlify.tasks.page.request_json') as request_grid,
            patch('htmlify.tasks.page.write_taxonomy_index'),
        ):
            task.species_page(soup, 74, 3)

        specimen_panel = soup.select_one(
            '.htmlify-species-panel[data-species-tab="specimens"]'
        )
        self.assertIsNotNone(specimen_panel)
        specimen_list = specimen_panel.select_one(
            '.htmlify-specimen-list[data-htmlify-specimens-version="1"]'
        )
        self.assertIsNotNone(specimen_list)
        self.assertEqual(
            specimen_list.a['href'],
            '/content/aau-herbarium-mg01',
        )
        self.assertEqual(
            specimen_list.a.get_text(strip=True),
            'AAU Herbarium MG01',
        )
        self.assertIsNone(specimen_panel.select_one('.slickgrid-wrapper'))
        self.assertIsNone(specimen_panel.select_one('.loading-indicator'))
        request_grid.assert_not_called()

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
