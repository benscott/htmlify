import luigi
from bs4 import BeautifulSoup
import requests
import requests_cache
from urllib.parse import urlparse, urlunparse, urlencode
import os.path
import copy
import re
import yaml
from selenium import webdriver
import time
from pathlib import Path
import os
from urllib.parse import parse_qs, unquote
from collections import OrderedDict
import json
import html

# from selenium import webdriver 
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.select import Select
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException, NoSuchElementException
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.common.exceptions import StaleElementReferenceException
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import requests_cache


import mysql.connector


from htmlify import config
from htmlify.config import logger, SCHEME, USE_SELENIUM, SITES_DIR
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.stack import UniqueStack
from htmlify.utils import (
    get_soup,
    concat_path,
    is_decommisionned_link,
    is_internal_url,
    canonicalize_source_references,
    request,
    request_json,
)
from htmlify.url import URL
from htmlify.db import db_manager
from htmlify.facets import (
    configured_facet_definitions,
    extract_facet_definitions,
    extract_result_hints,
    facet_asset_name,
    facet_asset_url,
    facet_configuration_fingerprint,
    is_configured_facet_page,
    merge_facet_definitions,
    write_facet_index,
)
from htmlify.taxonomy import (
    biological_vid_for_term,
    taxonomy_asset_url,
    write_taxonomy_index,
)


SPECIES_PAGE_VERSION = '4'
SKIPPED_PAGE_VERSION = '3'
FACET_PAGE_VERSION = '11'
STATIC_ASSET_VERSION = '7'
PAGE_ARCHIVE_VERSION = '1'

class PageTask(BaseTask):

    # Compile the regex pattern
    re_facet = re.compile(r'^f\[\d+\]$')
    url = luigi.Parameter()
    output_dir = luigi.PathParameter()

    decommisioned_js = [
        'scratchpads_search_block.js',
        'autocomplete.js',
        'modal_forms_popup.js',
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)  
        self._url = URL(self.url)

        if self._url.has_multiple_facets():
            logger.error(f'Page {self.url} is a multiple facet page')

        self.domain = self._url.domain               

    def run(self):
        # # self.subdomain = parsed_url.netloc.replace('myspecies.info', '')

        try:
            if self._classification_vid():
                response = request(self.url)
                source_path = urlparse(response.url).path.rstrip('/') or '/'
                requested_path = self._url.path or '/'
                if source_path != requested_path:
                    self._write_redirect_page(source_path)
                    return
                soup = BeautifulSoup(response.text, features='html.parser')
            else:
                soup = get_soup(self.url)
        except requests.HTTPError as error:
            status_code = getattr(error.response, 'status_code', None)
            if status_code in {401, 403, 404, 410}:
                self._mark_skipped(f'HTTP {status_code}')
                return
            raise

        if soup.find('h1', string="Technical difficulties"):
            raise Exception("Technical difficulties")

        drupal_facet_definitions = (
            configured_facet_definitions(self._url.path)
            if not self._is_search_page() else []
        )
        configured_facet_page = (
            not self._is_search_page()
            and (
                bool(drupal_facet_definitions)
                or is_configured_facet_page(self.domain, self._url.path)
            )
        )
        if (
            self._is_unavailable_search_page(soup)
            and not configured_facet_page
        ):
            raise Exception('Empty search page - Search is temporarily unavailable')

        species_tid = self._species_term_id()
        species_vid = (
            biological_vid_for_term(self.domain, species_tid)
            if species_tid else None
        )
        if species_vid:
            self.species_page(soup, species_tid, species_vid)

        facet_definitions = []
        result_hints = []
        if not species_vid and not self._is_search_page():
            rendered_facet_definitions = extract_facet_definitions(
                soup,
                domain=self.domain,
            )
            facet_definitions = merge_facet_definitions(
                drupal_facet_definitions,
                rendered_facet_definitions,
            )
            if facet_definitions or configured_facet_page:
                result_hints = extract_result_hints(soup)

        is_facet_page = self._should_build_facet_page(
            species_vid,
            facet_definitions,
            configured_facet_page,
        )

        # Do this before expanding widgets or downloading assets. Template-only
        # taxonomy tabs are common and should not cost a full page conversion.
        if (
            not is_facet_page
            and not species_vid
            and not self._page_has_content(soup)
        ):
            self._mark_skipped('main content region is empty')
            return

        if self._is_tinytax(soup):
            self.process_tinytax(soup)

        if self._is_slickgrid(soup):
            soup = self._load_slickgrid(soup)

        if is_facet_page:
            self.facet_page(soup, facet_definitions, result_hints)

        self.process_css_style_urls(soup)
        self.process_css(soup)
        self.process_js(soup)
        self.process_images(soup)

        self.replace_login(soup)
        self.process_links(soup)
        self.replace_search(soup)
        self.remove_messages(soup)
        self.remove_login_block(soup)
        self.remove_comment_form(soup)
        self.remove_eol_widgets(soup)

        self.contact_page_message(soup)
        self.search_page(soup)
        self.login_page(soup)
        self.node_page(soup)

        self.process_facets(soup)
        self.disable_autocomplete(soup)

        self.remove_feeds(soup)

        # Create a new link tag for the stylesheet
        if soup.head:
            new_stylesheet = soup.new_tag(
                'link',
                rel='stylesheet',
                href=f'/assets/style.css?v={STATIC_ASSET_VERSION}',
            )
            soup.head.append(new_stylesheet)

        if soup.html:
            soup.html['data-htmlify-page-version'] = PAGE_ARCHIVE_VERSION
            soup.html['data-htmlify-domain'] = self.domain

        self.parsed_path.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Saving page to {self.output().path}")
        self.skipped_path.unlink(missing_ok=True)
        with self.output().open('wb') as f: 
            html = canonicalize_source_references(str(soup), self.domain)
            f.write(html.encode('utf-8'))

        if species_vid:
            self._write_species_tab_redirects(soup, species_tid)

    def disable_autocomplete(self, soup):            
        autocomplete_els = soup.find_all(class_='autocomplete')
        for el in autocomplete_els:
            el.decompose()  

    def remove_comment_form(self, soup):
        if comment_form := soup.find('div', id='comments'):
            comment_form.decompose()    

    def remove_feeds(self, soup):
        if feeds_div := soup.find(class_='feed-icon'):
            feeds_div.decompose()  
              
    def remove_eol_widgets(self, soup):
        eol_ids = [
            'block-views-eol-images-block',
            'block-views-eol-text-block'
        ]
        for eol_id in eol_ids:
            if el := soup.find('section', id=eol_id):
                el.decompose()                
    
    def process_facets(self, soup):
        facet_blocks = soup.find_all('section', {"class": "block-facetapi"})

        for facet_block in facet_blocks:

            # Remove the search facet input
            if facet_form := facet_block.find('form'):
                facet_form.decompose()

            ul = facet_block.find('ul', {"class": "facetapi-facetapi-links"})
            # If we don't have links, remvoe the block
            if not ul or not ul.find_all('a'):
                facet_block.decompose()
    
    def _base_term_path(self, url):

        # Define the regex pattern to match 'taxonomy/term/number'
        pattern = r'(taxonomy/term/\d+)(?:/.*)?'
        term_path = re.sub(pattern, r'\1', url)
        if not term_path.startswith('/'):
            term_path = f'/{term_path}'
        return term_path
                                    
    def process_tinytax_links(self, soup):
        tinytax_block = soup.find('section', {"class": "block-tinytax"})
        tinytax_div = soup.find('div', {"class": "tinytax"})
        tinytax_div['class'] = 'static-tinytax'

        for a in tinytax_block.find_all('a', href=True):              
            a['href'] = self._base_term_path(a['href'])
    
    @property
    def parsed_path(self):
        if path := self._url.to_path():
            return concat_path(self.output_dir, path)
        else:
            return self.output_dir

    def output(self):
        return luigi.LocalTarget(self.parsed_path / 'index.html', format=luigi.format.Nop)

    @property
    def skipped_path(self):
        relative_path = self.parsed_path.relative_to(Path(self.output_dir))
        return (
            config.PROCESSING_DATA_DIR
            / 'skipped'
            / self.domain
            / relative_path
            / '.htmlify-skipped'
        )

    def _mark_skipped(self, reason):
        Path(self.output().path).unlink(missing_ok=True)
        try:
            self.parsed_path.rmdir()
        except (FileNotFoundError, OSError):
            pass
        self.skipped_path.parent.mkdir(parents=True, exist_ok=True)
        self.skipped_path.write_text(
            f'htmlify-skip-version: {SKIPPED_PAGE_VERSION}\n'
            f'{self.url}\n{reason}\n',
            encoding='utf-8',
        )
        logger.info(f'Skipping empty page {self.url}: {reason}')

    def complete(self):
        if self.force:
            return False
        output_path = Path(self.output().path)
        if not output_path.exists():
            if not self.skipped_path.exists():
                return False
            first_line = self.skipped_path.read_text(
                encoding='utf-8',
                errors='ignore',
            ).splitlines()[0]
            if first_line == f'htmlify-skip-version: {SKIPPED_PAGE_VERSION}':
                return True

            if self._classification_vid():
                return False

            # Legacy biological term pages were commonly marked empty because
            # their useful content lived on child tabs. Revisit only those old
            # taxonomy markers during this upgrade.
            species_tid = self._species_term_id()
            return not (
                species_tid
                and biological_vid_for_term(self.domain, species_tid)
            )

        species_tid = self._species_term_id()
        species_vid = (
            biological_vid_for_term(self.domain, species_tid)
            if species_tid else None
        )

        # A faceted page must run again when its page-specific JSON index is
        # missing. This also upgrades archives made by the older
        # bibliography-only implementation.
        html = output_path.read_text(encoding='utf-8', errors='ignore')
        if (
            f'data-htmlify-page-version="{PAGE_ARCHIVE_VERSION}"' not in html
            or f'data-htmlify-domain="{self.domain}"' not in html
        ):
            return False
        if species_vid:
            version = (
                f'data-htmlify-species-version="{SPECIES_PAGE_VERSION}"'
            )
            if version not in html:
                return False
            taxonomy_index = (
                Path(self.output_dir)
                / taxonomy_asset_url(species_vid).lstrip('/')
            )
            if not taxonomy_index.exists():
                return False
        else:
            configured_facets = configured_facet_definitions(self._url.path)
        if not species_vid and (
            configured_facets
            or is_configured_facet_page(self.domain, self._url.path)
        ):
            version = f'data-htmlify-facet-version="{FACET_PAGE_VERSION}"'
            if version not in html:
                return False
            fingerprint = facet_configuration_fingerprint(self._url.path)
            if (
                fingerprint
                and f'data-htmlify-facet-config="{fingerprint}"' not in html
            ):
                return False
        if 'htmlify-biblio-browser' in html:
            return False
        if 'block-facetapi' in html and not self._is_search_page():
            return False
        if 'htmlify-facet-browser' in html:
            match = re.search(r'data-index-url=["\']([^"\']+)["\']', html)
            if not match:
                return False
            index_path = Path(self.output_dir) / match.group(1).lstrip('/')
            if not index_path.exists():
                return False
        return True

    def _species_term_id(self):
        match = re.fullmatch(r'/taxonomy/term/(\d+)', self._url.path or '')
        return int(match.group(1)) if match else None

    def _classification_vid(self):
        match = re.fullmatch(r'/classification/(\d+)', self._url.path or '')
        return int(match.group(1)) if match else None

    def _write_redirect_page(self, destination):
        destination = '/' + destination.lstrip('/')
        escaped_destination = html.escape(destination, quote=True)
        redirect = (
            '<!doctype html><html '
            f'data-htmlify-page-version="{PAGE_ARCHIVE_VERSION}" '
            f'data-htmlify-domain="{self.domain}">'
            '<head><meta charset="utf-8">'
            f'<meta http-equiv="refresh" content="0; url={escaped_destination}">'
            f'<link rel="canonical" href="{escaped_destination}">'
            '</head><body><p>'
            f'<a href="{escaped_destination}">Continue to the classification</a>'
            '</p></body></html>'
        )
        self.parsed_path.mkdir(parents=True, exist_ok=True)
        self.skipped_path.unlink(missing_ok=True)
        Path(self.output().path).write_text(redirect, encoding='utf-8')

    @staticmethod
    def _species_tab_slug(path, base_path, label):
        suffix = path[len(base_path):].strip('/')
        if suffix:
            return suffix
        slug = re.sub(r'[^a-z0-9]+', '-', label.lower()).strip('-')
        return slug or 'overview'

    @staticmethod
    def _species_tab_label(link):
        link = copy.deepcopy(link)
        for hidden in link.select('.element-invisible'):
            hidden.decompose()
        return link.get_text(' ', strip=True)

    @staticmethod
    def _species_tab_content(tab_soup):
        root = (
            tab_soup.select_one('#region-content .region-content-inner')
            or tab_soup.select_one('#region-content')
        )
        if not root:
            return []
        tabs = root.select_one('div.tabs')
        if not tabs:
            main = root.select_one('#block-system-main')
            return [copy.deepcopy(main)] if main else []

        content = []
        found_tabs = False
        for child in root.contents:
            if child is tabs:
                found_tabs = True
                continue
            if found_tabs and (not isinstance(child, str) or child.strip()):
                content.append(copy.deepcopy(child))
        return content

    @staticmethod
    def _remove_obsolete_species_blocks(soup):
        """Remove retired remote-data blocks that cannot work statically."""
        for selector in (
            '.block-ncbi',
            '.block-iucn',
            '.block-refindit',
            '.block-bhl',
        ):
            for block in soup.select(selector):
                block.decompose()

    @staticmethod
    def _species_panel_has_content(section):
        """Return whether a tab contains useful, archivable content."""
        content = copy.deepcopy(section)
        for selector in (
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'script', 'style', 'form',
            '.contextual-links-wrapper',
            '.view-empty', '.views-empty',
        ):
            for element in content.select(selector):
                element.decompose()

        text = content.get_text(' ', strip=True).replace('\xa0', '').strip()
        if text:
            return True
        return bool(content.find([
            'audio', 'embed', 'iframe', 'img', 'object', 'table', 'video',
        ]))

    @staticmethod
    def _remove_original_species_content(soup, tabs_container):
        """Remove source content after it has been copied into tab panels."""
        root = (
            soup.select_one('#region-content .region-content-inner')
            or soup.select_one('#region-content')
        )
        if not root:
            return
        found_tabs = False
        for child in list(root.contents):
            if child is tabs_container:
                found_tabs = True
                continue
            if found_tabs:
                child.extract()

    def species_page(self, soup, tid, vid):
        """Combine the enabled Drupal species tabs into one static page."""
        base_path = f'/taxonomy/term/{tid}'

        # Solr facet blocks on biological taxonomy pages search a separate
        # result index, not the taxonomy hierarchy. They are misleading once
        # archived and used to produce an empty generic facet browser here.
        # The static taxonomy tree gets its own database-backed filter below.
        for selector in (
            '.block-facetapi',
            '.block-scratchpads-biblio-search',
            '.apachesolr-browse-blocks',
            '.htmlify-facet-controls',
            '.htmlify-facet-browser',
        ):
            for block in soup.select(selector):
                block.decompose()
        self._remove_obsolete_species_blocks(soup)

        tabs_container = soup.select_one('#region-content div.tabs')
        tabs_list = tabs_container.select_one('ul.tabs') if tabs_container else None
        if not tabs_container or not tabs_list:
            return

        tabs = []
        seen = set()
        for link in tabs_list.find_all('a', href=True):
            parsed = urlparse(link['href'])
            path = parsed.path.rstrip('/') or '/'
            if path != base_path and not path.startswith(f'{base_path}/'):
                continue
            label = self._species_tab_label(link)
            slug = self._species_tab_slug(path, base_path, label)
            if slug in seen:
                continue
            seen.add(slug)
            tabs.append({'slug': slug, 'label': label, 'path': path})

        if not tabs:
            return

        panels = []
        for tab in tabs:
            if tab['path'] == base_path:
                tab_soup = soup
            else:
                tab_url = urlunparse((
                    self._url.scheme,
                    self._url.domain,
                    tab['path'],
                    '',
                    '',
                    '',
                ))
                try:
                    tab_soup = get_soup(tab_url)
                except requests.RequestException as error:
                    logger.warning(
                        f'Could not archive species tab {tab_url}: {error}'
                    )
                    tab_soup = None

            # Drupal normally places the SlickGrid initializer outside the
            # tab's main content region. Expand the grid before copying that
            # region into the combined static species page, otherwise the
            # wrapper is copied without the script needed to identify it.
            if tab_soup and self._is_slickgrid(tab_soup):
                tab_soup = self._load_slickgrid(tab_soup)
            if tab_soup:
                self._remove_obsolete_species_blocks(tab_soup)

            section = soup.new_tag('section')
            section['class'] = ['htmlify-species-panel']
            section['id'] = f"species-tab-{tab['slug']}"
            section['data-species-tab'] = tab['slug']
            heading = soup.new_tag('h2')
            heading.string = tab['label']
            section.append(heading)

            tab_content = (
                self._species_tab_content(tab_soup) if tab_soup else []
            )
            section.extend(tab_content)
            if self._species_panel_has_content(section):
                panels.append((tab, section))

        # The base page's original content was used to build the first panel.
        # Leaving it in place makes it appear below every selected tab.
        self._remove_original_species_content(soup, tabs_container)

        if panels:
            tabs_container.clear()
            tabs_container['class'] = ['tabs', 'htmlify-species-tabs']
            tabs_container['aria-label'] = 'Species page sections'
            new_list = soup.new_tag('ul')
            new_list['role'] = 'tablist'
            for position, (tab, panel) in enumerate(panels):
                item = soup.new_tag('li')
                link = soup.new_tag(
                    'a',
                    href=f"#species-tab-{tab['slug']}",
                )
                link['data-species-tab'] = tab['slug']
                link['role'] = 'tab'
                link['aria-controls'] = f"species-tab-{tab['slug']}"
                link['aria-selected'] = 'true' if position == 0 else 'false'
                if position == 0:
                    link['class'] = ['active']
                link.string = tab['label']
                item.append(link)
                new_list.append(item)
            tabs_container.append(new_list)

            insertion_point = tabs_container
            for _, panel in panels:
                insertion_point.insert_after(panel)
                insertion_point = panel
        else:
            tabs_container.decompose()

        content_region = soup.select_one('#region-content')
        if content_region:
            classes = list(content_region.get('class', []))
            if 'htmlify-species-page' not in classes:
                classes.append('htmlify-species-page')
            content_region['class'] = classes
            content_region['data-htmlify-species-version'] = SPECIES_PAGE_VERSION

        self._replace_taxonomy_tree(soup, tid, vid)
        species_script = f'/assets/species-page.js?v={STATIC_ASSET_VERSION}'
        if not soup.find('script', src=species_script):
            script = soup.new_tag(
                'script',
                src=species_script,
                defer=True,
            )
            if soup.body:
                soup.body.append(script)

    def _replace_taxonomy_tree(self, soup, tid, vid):
        block = soup.select_one('section.block-tinytax')
        if not block:
            return

        destination = (
            Path(self.output_dir)
            / taxonomy_asset_url(vid).lstrip('/')
        )
        if self.force or not destination.exists():
            write_taxonomy_index(self.domain, vid, destination)

        classes = [
            name for name in block.get('class', [])
            if name != 'block-tinytax'
        ]
        classes.append('htmlify-taxonomy-tree')
        block['class'] = classes
        block['data-index-url'] = taxonomy_asset_url(vid)
        block['data-current-tid'] = str(tid)

        content = block.find(class_='content')
        if not content:
            content = block
        content.clear()
        form = soup.new_tag('form')
        form['class'] = ['htmlify-taxonomy-search']
        form['role'] = 'search'
        label = soup.new_tag('label', attrs={'for': 'htmlify-taxonomy-query'})
        label.string = 'Find a taxon'
        controls = soup.new_tag('div')
        controls['class'] = ['htmlify-taxonomy-search-controls']
        query = soup.new_tag(
            'input',
            id='htmlify-taxonomy-query',
            attrs={
                'name': 'q',
                'type': 'search',
                'autocomplete': 'off',
            },
        )
        submit = soup.new_tag('button', type='submit')
        submit.string = 'Filter'
        clear = soup.new_tag('button', type='button')
        clear['class'] = ['htmlify-taxonomy-clear']
        clear.string = 'Clear'
        controls.extend([query, submit, clear])
        form.extend([label, controls])
        status = soup.new_tag('p')
        status['class'] = ['htmlify-taxonomy-status']
        status['aria-live'] = 'polite'
        status.string = 'Loading taxonomy…'
        tree = soup.new_tag('div')
        tree['class'] = ['htmlify-taxonomy-list']
        content.extend([form, status, tree])

    def _write_species_tab_redirects(self, soup, tid):
        for link in soup.select('.htmlify-species-tabs a[data-species-tab]'):
            slug = link['data-species-tab']
            redirect_dir = self.parsed_path / slug
            redirect_dir.mkdir(parents=True, exist_ok=True)
            destination = f'/taxonomy/term/{tid}#species-tab-{slug}'
            escaped_destination = html.escape(destination, quote=True)
            label = html.escape(link.get_text(' ', strip=True))
            redirect = (
                '<!doctype html><html><head><meta charset="utf-8">'
                f'<meta http-equiv="refresh" content="0; url={escaped_destination}">'
                f'<link rel="canonical" href="{escaped_destination}"></head><body>'
                f'<p><a href="{escaped_destination}">View {label}</a></p>'
                '</body></html>'
            )
            (redirect_dir / 'index.html').write_text(redirect, encoding='utf-8')

    def _is_tinytax(self, soup):
        return bool(soup.find('section', {"class": "block-tinytax"})) 
    
    def process_tinytax(self, soup):

        logger.debug(f'Tiny tax detected {self.url} - expanding...')

        tiny_tax_block = soup.find('section', {"class": "block-tinytax"})

        # Extract the class attribute
        body_class = soup.body.get('class', [])
        # Join the class list into a single string (if it's a list)
        body_class_str = ' '.join(body_class)

        # Regular expression to extract the number after 'page-taxonomy-term-'
        pattern = r'page-(?:taxonomy|nontaxonomy|simpletaxonomy)-term-(\d+)'

        # Search for the pattern in the class attribute string
        match = re.search(pattern, body_class_str)        

        tid = match.group(1)

        tiny_tax_block = soup.find('section', {"class": "block-tinytax"})
        block_id = tiny_tax_block.get('id').replace('block-', '')

        tinytax = self.get_tinytax(block_id, tid)
        tinytax_soup = BeautifulSoup(tinytax, 'html.parser')
        # Find all <a> elements with the classes tinytax-bold and active
        active_a = tinytax_soup.find_all('a', class_='tinytax-bold')

        # Remove the classes from each found anchor
        for a in active_a:
            a['class'].remove('tinytax-bold')
            if 'active' in a.get('class', []):
                a['class'].remove('active')            

        active_element = tinytax_soup.find(id=f'tinytax-{tid}') 
        active_element.find('a')['class'] = ['active', 'tinytax-bold']           
        tiny_tax_block.replace_with(tinytax_soup)   

    def get_tinytax(self, block_id, tid):

        if child_tid := self.get_child_tid(tid):
            return self.get_tinytax_block(block_id, child_tid)
        
        return self.get_tinytax_block(block_id, tid)

    def get_tinytax_block(self, block_id, tid):

        query_dict = {
            'blocks': block_id,
            'path': f'taxonomy/term/{tid}'
        }
        query = urlencode(query_dict)        

        child_url = urlunparse((
            self._url.scheme, 
            self._url.domain, 
            f'ajaxblocks', 
            '', 
            query,
            ''
        ))     

        json = request_json(child_url) 
        return json.get(block_id).get('content')
    
    def get_child_tid(self, tid):
        sql = f"""
            SELECT DISTINCT td.tid
            FROM taxonomy_term_data td
            INNER JOIN taxonomy_term_hierarchy th ON td.tid = th.tid
            WHERE th.parent = {tid}
        """     

        result = db_manager.fetch(self.domain, sql)
        for child_tid, in result:
            child_url = urlunparse((
                self._url.scheme, 
                self._url.domain, 
                f'taxonomy/term/{child_tid}', 
                '', 
                '',
                ''
            )) 

            try:
                get_soup(child_url)
            except requests.exceptions.HTTPError:    
                continue
            else:
                return child_tid             
    
    def _is_slickgrid(self, soup):
        return bool(soup.find('div', {"class": "slickgrid-wrapper"}))         

    def replace_search(self, soup): 
        if search_form := soup.find('form', id="search-block-form"):
            self._update_search_form(search_form, soup)

    def _update_search_form(self, search_form, soup):
            search_form['action'] = 'https://www.google.com/search'   
            search_form['method'] = 'get'   

            input_tag = search_form.find('input', {'name': 'search_block_form'}) or search_form.find('input', {'name': 'keys'})
            input_tag['name'] = 'q'

            new_site_input = soup.new_tag('input', type='hidden')
            new_site_input['name'] = 'as_sitesearch'
            new_site_input['value'] = self.domain

            if facets := search_form.find('div', id="edit-facet"):
                facets.decompose()

            search_form.append(new_site_input)

    def process_links(self, soup):

        # Loop through the links, ensuring they match the new static site structure
        for a in soup.find_all('a', href=True):  
            href = a.get('href')
            if href.startswith('#'):
                continue
            if is_decommisionned_link(href):
                a.decompose()
                continue

            url = URL(href, self.domain)

            if not url.is_site_internal_link():
                continue

            if url.has_multiple_facets():
                logger.debug(f'Removing facet url {href}')
                a.decompose()
                continue

            a['href'] = url.to_path()

    def replace_login(self, soup):      
        if login_div := soup.find('div', id='zone-slide-top-wrapper'):
            login_div.decompose()

    def remove_messages(self, soup):
        # Find all div elements with the class 'messages'
        msg_divs = soup.find_all('div', class_='messages')

        # Remove each div element
        for div in msg_divs:
            div.decompose()        

    def process_css(self, soup): 
        for link in soup.find_all('link', href=True):
            rel = set(link.get('rel', []))
            is_page_asset = (
                link.get('type') == 'text/css'
                or bool(rel & {
                    'stylesheet', 'icon', 'shortcut', 'apple-touch-icon',
                    'mask-icon',
                })
            )
            if not is_page_asset:
                continue
            if rel_path := self.process_remote_file(link['href']):
                link['href'] = rel_path     

    def is_decommisioned_js(self, url):
        for js in self.decommisioned_js:
            if js in url: return True
        
    def process_js(self, soup): 

        for script in soup.find_all('script', {"type" : "text/javascript", "src": True}):

            if self.is_decommisioned_js(script['src']):
                script.decompose()
                continue

            if rel_path := self.process_remote_file(script['src']):
                script['src'] = rel_path                    

    def process_css_style_urls(self, soup): 
        pattern = re.compile(r'@import url\("(.*?)"\);')
        style_tags = soup.find_all('style',  {"type" : "text/css"})
        for style_tag in style_tags:

            new_style_tag = style_tag.string
            matches = pattern.findall(style_tag.string)

            for match in matches:
                if rel_path := self.process_remote_file(match):
                    new_style_tag = new_style_tag.replace(match, rel_path)

            style_tag.string.replace_with(new_style_tag)

    def process_remote_file(self, url):
        parsed_url = urlparse(url)
        # If the file has domain that isn't the one we're processing
        # we'll keep the url exactly the same 
        if not is_internal_url(url, self.domain):
            return False
        
        if parsed_url.netloc.startswith('www'):
            url = url.replace('www.', '')

        dest_path = concat_path(self.output_dir, parsed_url.path)

        # If this file is in the sites directory, we should already have it
        if parsed_url.path.startswith('/sites'):
            if config.DRUPAL_FILES_MODE == 'external':
                return parsed_url.path
            if not dest_path.exists():
                logger.error(f'Sites file {dest_path} does not exist')
            else:
                return parsed_url.path  

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            content = self._request_url(url)
        except requests.exceptions.HTTPError:
            return None
        else:
            with dest_path.open('wb') as f:
                f.write(content)
        
        return parsed_url.path  
    
    def process_images(self, soup):
        for img in soup.find_all('img', {"src": True}):
            if rel_path := self.process_remote_file(img['src']):
                img['src'] = rel_path  

    def _request_url(self, url):        
        # If it doesn;t start with a domain, download it
        parsed_url = urlparse(url)

        # Drupal can emit protocol-relative URLs using DDEV's hostname. Route
        # every internal asset through the canonical domain first; request()
        # will then map it back to SOURCE_BASE_URL with the correct scheme.
        request_domain = parsed_url.netloc or self.domain
        if is_internal_url(url, self.domain):
            request_domain = self.domain

        # Rebuild the URL substituting missing domain/schemas etc.,
        url = urlunparse((
            parsed_url.scheme or SCHEME, 
            request_domain,
            parsed_url.path, 
            '', 
            '',
            ''
        ))
        r = request(url)
        r.raise_for_status()
        return r.content                

    def _page_has_content(self, soup):
        content = (
            soup.select_one('#region-content')
            or soup.select_one('main')
            or soup.select_one('[role="main"]')
            or soup.select_one('#content')
        )
        if content is None:
            # Unknown themes should be retained rather than discarded based on
            # a layout assumption that only applies to Scratchpads.
            return True

        content = copy.deepcopy(content)
        chrome_selectors = (
            '#main-content',
            '#page-title',
            '.tabs',
            '.messages',
            '.contextual-links-wrapper',
            '.view-empty',
            '.views-empty',
            'script',
            'style',
        )
        for selector in chrome_selectors:
            for element in content.select(selector):
                element.decompose()

        text = content.get_text(' ', strip=True).replace('\xa0', '').strip()
        if text:
            return True

        return bool(content.find([
            'article', 'audio', 'canvas', 'embed', 'iframe', 'img', 'object',
            'table', 'video',
        ]))

    def remove_login_block(self, soup):
        if login_block := soup.find('div', id='block-user-login'):
            login_block.decompose()

    def contact_page_message(self, soup):
        if contact_form := soup.find('form', id='contact-site-form'):
            new_div = soup.new_tag('div', **{'class': 'messages'})
            new_div.string = "This is a static archive of a Scratchpad, retained for reference.  It is no longer possible to contact the maintainers."
            contact_form.replace_with(new_div)

    def search_page(self, soup):
        if search_form := soup.find('form', id='search-form'):
            self._update_search_form(search_form, soup) 

        if search_facets := soup.find('div', class_="apachesolr-browse-blocks"):
            search_facets.decompose()            

    def facet_page(self, soup, facets, result_hints):
        """Replace a live Solr/Facet API result page with a static browser."""
        page_path = self._url.path or '/'
        title_element = soup.find(id='page-title') or soup.find('h1')
        page_title = (
            title_element.get_text(' ', strip=True)
            if title_element else page_path.strip('/').replace('-', ' ').title()
        )
        asset_name = facet_asset_name(page_path)
        destination = Path(self.output_dir) / 'assets' / 'facets' / asset_name
        write_facet_index(
            self.domain,
            page_path,
            page_title,
            facets,
            result_hints,
            destination,
        )

        for selector in (
            '.block-facetapi',
            '.block-scratchpads-biblio-search',
            '.apachesolr-browse-blocks',
            '.biblio-links-info',
        ):
            for block in soup.select(selector):
                block.decompose()

        if soup.select_one('.htmlify-facet-browser'):
            return

        controls_html = '''
            <section class="htmlify-facet-controls">
              <h2></h2>
              <form class="htmlify-facet-search" role="search">
                <label for="htmlify-facet-query">Search these records</label>
                <div class="htmlify-facet-search-controls">
                  <input id="htmlify-facet-query" name="q" type="search">
                  <button type="submit">Search</button>
                  <button class="htmlify-facet-clear" type="button">Clear</button>
                </div>
              </form>
              <div class="htmlify-facet-selected" aria-live="polite"></div>
              <div class="htmlify-facet-groups"></div>
            </section>
        '''
        browser_html = '''
            <section class="htmlify-facet-browser">
              <p class="htmlify-facet-status" aria-live="polite">
                Loading the static index…
              </p>
              <ol class="htmlify-facet-results"></ol>
              <nav class="htmlify-facet-pagination" aria-label="Result pages"></nav>
              <noscript>
                JavaScript is required to search or browse this static index.
              </noscript>
            </section>
        '''
        controls = BeautifulSoup(controls_html, 'html.parser').find('section')
        browser = BeautifulSoup(browser_html, 'html.parser').find('section')
        browser['data-index-url'] = facet_asset_url(page_path)
        browser['data-htmlify-facet-version'] = FACET_PAGE_VERSION
        if fingerprint := facet_configuration_fingerprint(page_path):
            browser['data-htmlify-facet-config'] = fingerprint
        controls.find('h2').string = f'Filter {page_title}'

        sidebar = soup.select_one('#region-sidebar .region-inner, #region-sidebar')
        if sidebar:
            sidebar.insert(0, controls)
        else:
            browser.insert(0, controls)

        main_block = soup.select_one('#block-system-main')
        main_content = main_block.find(class_='content') if main_block else None
        if main_content:
            main_content.clear()
            main_content.append(browser)
        elif title_element:
            title_element.insert_after(browser)
        else:
            content = soup.find(id='region-content') or soup.find('main') or soup.body
            if content:
                content.insert(0, browser)

        script = soup.new_tag(
            'script',
            src=f'/assets/facet-search.js?v={STATIC_ASSET_VERSION}',
            defer=True,
        )
        if soup.body:
            soup.body.append(script)

    def _is_search_page(self):
        path = self._url.path or ''
        return path == '/search' or path.startswith('/search/')

    @staticmethod
    def _should_build_facet_page(
        species_vid,
        facet_definitions,
        configured_facet_page,
    ):
        """Keep biological term pages on their taxonomy-specific UI."""
        return not species_vid and (
            bool(facet_definitions) or configured_facet_page
        )

    def _is_unavailable_search_page(self, soup):
        if main_content := soup.find('section', id='section-content'):
            return 'Search is temporarily unavailable' in main_content.get_text()
        
    def login_page(self, soup):
        if main_content := soup.find('section', id='section-content'):
            if login_form := main_content.find('form', id='user-login'):
                login_form.replace_with('Login disabled')
            if pass_form := main_content.find('form', id='user-pass'):
                pass_form.replace_with('Login disabled')  

    def node_page(self, soup):
        if soup.find('body', class_='page-node'):
            if tabs := soup.find('div', class_='tabs'):
                tabs.decompose() 

    def _get_slickgrid_id(self, soup):
        pattern = re.compile(
            r'''new\s+Slickgrid\s*\(\s*
                (["'])\#[^"']+\1\s*,\s*
                (["'])(?P<slick_id>[^"']+)\2\s*,\s*
                (["'])[^"']+\4\s*\)
            ''',
            re.VERBOSE,
        )
        for script in soup.find_all('script'):
            content = script.string or script.get_text()
            if not content:
                continue
            match = pattern.search(content)
            if not match:
                continue

            columns = None
            column_match = re.search(r'\bvar\s+columns\s*=\s*', content)
            if column_match:
                try:
                    column_dict, _ = json.JSONDecoder().raw_decode(
                        content[column_match.end():]
                    )
                    if isinstance(column_dict, list):
                        columns = [
                            column.get('name')
                            for column in column_dict
                            if isinstance(column, dict) and column.get('name')
                        ] + ['ID']
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        f'Could not parse SlickGrid columns on {self.url}'
                    )

            return match.group('slick_id'), columns

        return None, None

    def _load_slickgrid(self, soup):

        logger.debug('Slickgrid detected - replacing grid')

        slickgrid_id, columns = self._get_slickgrid_id(soup)

        if not slickgrid_id:
            logger.warning(
                f'Could not find SlickGrid configuration on {self.url}; '
                'leaving the original page content in place'
            )
            return soup

        if slickgrid_id:

            slickgrid_wrapper = soup.find('div', {"class": "slickgrid-wrapper"})

            slickgrid_url = urlunparse((
                self._url.scheme, 
                self._url.domain, 
                f'slickgrid/get/data/{slickgrid_id}/0/100', 
                '', 
                '',
                ''
            ))     

            json = request_json(slickgrid_url) 

            data = json.get('data')
            if not data:
                logger.error(f'Could not load slickgrid data {slickgrid_url}')
                return soup

            table = soup.new_tag('table', border="1")

            # Create the table header
            thead = soup.new_tag('thead')
            tr = soup.new_tag('tr')

            if not columns:
                columns = data[0].keys()

            # # Add headers to the header row
            for col in columns:
                th = soup.new_tag('th')
                th.string = col
                tr.append(th)

            thead.append(tr)
            table.append(thead)

            # Create an empty table body
            tbody = soup.new_tag('tbody')
            table.append(tbody)       

            for row in data:
                tr = soup.new_tag('tr') 

                for field, value in row.items():
                    td = soup.new_tag('td')
                    if "<" in value:
                        a = BeautifulSoup(value, "html.parser")
                        td.append(a)
                    else:
                        td.string = value
                    tr.append(td)
                table.append(tr)            

            slickgrid_wrapper.replace_with(table)            

        if loading := soup.find(class_='loading-indicator'):
            loading.decompose()      

        return soup  

if __name__ == "__main__":   

    url='https://aframomum.myspecies.info/specimen_observation/csv'
    domain = urlparse(url).netloc

    luigi.build([
        PageTask(
            # url='http://127.0.0.1/content/search2/', 
            # url='http://127.0.0.1/taxonomy/term/11/media',
            url=url,
            # output_dir=Path('/Users/ben/Projects/Scratchpads/Sites/gadus.myspecies.info'), 
            output_dir=SITES_DIR / domain,
            force=True)
            ], 
        local_scheduler=True)           
