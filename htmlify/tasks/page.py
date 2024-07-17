import luigi
from bs4 import BeautifulSoup
import requests
import requests_cache
from urllib.parse import urlparse, urlunparse
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

from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import requests_cache


import mysql.connector


from htmlify.config import logger, SCHEME, USE_SELENIUM
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, concat_path, is_decommisionned_link




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
        parsed_url = urlparse(self.url)
        self.domain = parsed_url.netloc                 

    def run(self):


        # # self.subdomain = parsed_url.netloc.replace('myspecies.info', '')

        soup = get_soup(self.url)
        driver = None

        # if self._is_unavailable_search_page(soup):
        #     raise Exception('Empty search page - Search is temporarily unavailable')


        if self._is_tiny_tax(soup):
            logger.debug(f'Tiny tax delected {self.url} - expanding...')
            # chrome_options = Options()
            # chrome_options.add_argument("--headless=new") # for Chrome >= 109
            # driver = webdriver.Chrome(options=chrome_options)         

            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)            
            driver.get(self.url)

            self._expand_tiny_tax(driver)
            soup = BeautifulSoup(driver.page_source, "html.parser")

        self.process_css_style_urls(soup)
        self.process_css(soup)
        self.process_js(soup)
        self.process_images(soup)

        self.replace_login(soup)
        self.process_links(soup)
        self.replace_search(soup)
        self.remove_messages(soup)
        self.remove_login_block(soup)

        self.contact_page_message(soup)
        self.search_page(soup)
        self.biblio_page(soup)

        if not self._page_has_content(soup):
            raise Exception(f'HTML file has no content: {self.url}')    

        # Create a new link tag for the stylesheet
        if soup.head:
            new_stylesheet = soup.new_tag('link', rel='stylesheet', href='/assets/style.css')                
            soup.head.append(new_stylesheet)

        self.parsed_path.mkdir(parents=True, exist_ok=True)

        with self.output().open('wb') as f: 
            f.write(soup.encode('utf-8'))         

        if driver:
            driver.quit()

    @property
    def parsed_url(self):
        return urlparse(self.url)
    
    @property
    def parsed_path(self):

        if self.parsed_url.path:
            path = self.resolve_url_to_path(self.parsed_url)
            return concat_path(self.output_dir, str(path))
        else:
            return self.output_dir

    def output(self):
        return luigi.LocalTarget(self.parsed_path / 'index.html', format=luigi.format.Nop)

    def _is_tiny_tax(self, soup):
        return bool(soup.find('section', {"class": "block-tinytax"})) 

    def _expand_tiny_tax(self, driver):

        active_link = driver.find_element(By.XPATH, '//div[@class="tinytax"]//a[contains(@class, "active") or contains(@class, "tinytax-bold")]')

        try:
            plus_img = active_link.find_element(By.XPATH, 'preceding-sibling::img[contains(@src,"plus.gif")]')
        except NoSuchElementException:
            print('Active tiny tax link doesn\'t have child elements to expand')
        else:
            self._click_tiny_tax(driver, plus_img)

    def _click_tiny_tax(self, driver, plus_img):
        driver.execute_script("arguments[0].click();", plus_img);   
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="tinytax"]//a[contains(@class, "active") or contains(@class, "tinytax-bold")]//following-sibling::ul')))
            print("Tinytax is ready!")            
        except TimeoutException:            
            print("Loading took too much time!")      

    def replace_search(self, soup): 
        if search_form := soup.find('form', id="search-block-form"):
            self._update_search_form(search_form, soup)

    def _update_search_form(self, search_form, soup):
            search_form['action'] = 'http://www.google.com/search'   
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

            parsed_url = urlparse(href)

            if not self._is_site_internal_link(parsed_url):
                continue

            resolved_path = self.resolve_url_to_path(parsed_url)
            a['href'] = resolved_path

    def resolve_url_to_path(self, parsed_url):

        path = Path(parsed_url.path)

        if parsed_url.query:
            decoded_query = unquote(parsed_url.query)
            parsed_query = parse_qs(decoded_query)  
            query_dict = {}
            for key, value in parsed_query.items():
                # print(value)
                if self.re_facet.match(key):
                    split_value = value[0].split(':')
                    query_dict[split_value[0]] = split_value[1]

                else:
                    query_dict[key] = value[0]

            sorted_query_dict = OrderedDict(sorted(query_dict.items()))
            for key, value in sorted_query_dict.items():
                path = path / key / value

        return path

    def _is_site_internal_link(self, parsed_url):           
        # Is this link relative or for the current domain  
        return not parsed_url.netloc or self.domain in parsed_url.netloc

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

        # File exists elsewhere in the filesystem - download it
        else:
        
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
        r = requests.get(url, allow_redirects=True)
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

    def biblio_page(self, soup):
        if biblio_links := soup.find('div', class_='biblio-links-info'):
            biblio_links.decompose()         

    def _is_unavailable_search_page(self, soup):
        if main_content := soup.find('section', id='section-content'):
            return 'Search is temporarily unavailable' in main_content.get_text()

if __name__ == "__main__":    
    luigi.build([
        PageTask(
            # url='http://127.0.0.1/content/search2/', 
            # url='http://127.0.0.1/taxonomy/term/11/media',
            url='http://gadus.myspecies.info/gallery?page=1',
            # output_dir=Path('/Users/ben/Projects/Scratchpads/Sites/gadus.myspecies.info'), 
            output_dir=Path('/var/www/gadus.myspecies.info'),
            force=True)
            ], 
        local_scheduler=True)           
