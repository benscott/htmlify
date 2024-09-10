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


from htmlify.config import logger, SCHEME, USE_SELENIUM, SITES_DIR
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, concat_path, is_decommisionned_link, request, request_json
from htmlify.url import URL
from htmlify.db import db_manager



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

        soup = get_soup(self.url)

        if soup.find('h1', string="Technical difficulties"):
            raise Exception("Technical difficulties")

        if self._is_unavailable_search_page(soup):
            raise Exception('Empty search page - Search is temporarily unavailable')

        if self._is_tinytax(soup):
            self.process_tinytax(soup)

        if self._is_slickgrid(soup):
            soup = self._load_slickgrid(soup)

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
        self.biblio_page(soup)
        self.login_page(soup)

        self.process_facets(soup)
        self.disable_autocomplete(soup)

        self.remove_feeds(soup)

        if not self._page_has_content(soup):
            raise Exception(f'HTML file has no content: {self.url}')    

        # Create a new link tag for the stylesheet
        if soup.head:
            new_stylesheet = soup.new_tag('link', rel='stylesheet', href='/assets/style.css')                
            soup.head.append(new_stylesheet)

        self.parsed_path.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Saving page to {self.output().path}")
        with self.output().open('wb') as f: 
            f.write(soup.encode('utf-8'))    

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

    def _is_tinytax(self, soup):
        return bool(soup.find('section', {"class": "block-tinytax"})) 
    
    def process_tinytax(self, soup):

        logger.debug(f'Tiny tax detected {self.url} - expanding...')

        tid = self._url.path.split('/')[-1]
        tiny_tax_block = soup.find('section', {"class": "block-tinytax"})
        block_id = tiny_tax_block.get('id').replace('block-', '')

        tinytax = self.get_tinytax(block_id, tid)
        tinytax_soup = BeautifulSoup(tinytax, 'html.parser')
        # Find all <a> elements with the classes tinytax-bold and active
        anchors = tinytax_soup.find_all('a', class_='tinytax-bold active')

        # Remove the classes from each found anchor
        for anchor in anchors:
            anchor['class'].remove('tinytax-bold')
            anchor['class'].remove('active')            

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
        for link in soup.find_all('link', {"type" : "text/css"}):
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
        if parsed_url.netloc and self.domain not in parsed_url.netloc:
            return False

        dest_path = concat_path(self.output_dir, parsed_url.path)

        # If this file is in the sites directory, we should already have it
        if parsed_url.path.startswith('/sites'):
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

        # Rebuild the URL substituting missing domain/schemas etc.,
        url = urlunparse((
            parsed_url.scheme or SCHEME, 
            parsed_url.netloc or self.domain, 
            parsed_url.path, 
            '', 
            '', 
            ''
        ))
        r = request(url)
        r.raise_for_status()
        return r.content                

    def _page_has_content(self, soup):
        return bool(soup.find('div'))

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

    def biblio_page(self, soup):
        if biblio_links := soup.find('div', class_='biblio-links-info'):
            biblio_links.decompose()         

    def _is_unavailable_search_page(self, soup):
        if main_content := soup.find('section', id='section-content'):
            return 'Search is temporarily unavailable' in main_content.get_text()
        
    def login_page(self, soup):
        if main_content := soup.find('section', id='section-content'):
            if login_form := main_content.find('form', id='user-login'):
                login_form.replace_with('Login disabled')
            if pass_form := main_content.find('form', id='user-pass'):
                pass_form.replace_with('Login disabled')  


    def _get_slickgrid_id(self, soup):
        pattern = r'new Slickgrid\("\#(?P<div_id>\w+)", "(?P<slick_id>\w+)", "(?P<page>\w+)"\);'

        for script in soup.find_all('script'):
            for content in script.contents:
                match = re.search(pattern, content)
                if match:
                    # FIXME: Get columns
                    column_pattern = r'var columns\s*=\s*(\[\{.*?\}\])'
                    
                    column_match = re.search(column_pattern, content)
                    if column_match:

                        column_dict = json.loads(column_match.group(1))
                        columns = [col.get('name') for col in column_dict] + ['ID']
                    else:
                        columns = None

                    return match.group('slick_id'), columns

    def _load_slickgrid(self, soup):

        logger.debug('Slickgrid detected - replacing grid')

        slickgrid_id, columns = self._get_slickgrid_id(soup)

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
                logger.error('Could not load slickgrid data')
                return

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
                    if "<a" in value:
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

    url='https://solanaceaesource.myspecies.info/collections'
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
