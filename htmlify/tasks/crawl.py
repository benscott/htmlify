import luigi
from bs4 import BeautifulSoup
import requests
import requests_cache
from urllib.parse import urlparse, urlunparse
import os.path
import copy
import re
import yaml


import mysql.connector


from htmlify.config import PROCESSING_DATA_DIR, DB_USERNAME, DB_PASSWORD, SCHEME, logger
from htmlify.tasks.base import BaseTask
from htmlify.tasks.sitemap import SiteMapTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, is_decommisionned_link, get_site_aliases
from htmlify.url import URL

class CrawlSiteTask(BaseTask):

    domain = luigi.Parameter()
    # db_conn = luigi.Parameter()
    
    output_dir = PROCESSING_DATA_DIR / 'crawl'   

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)            
        self._seen_paths = set()     

    def requires(self):
        # return SiteMapTask(domain=self.domain, db_conn=self.db_conn)
        return SiteMapTask(domain=self.domain)
    
    def run(self):

        with self.input().open('r') as f:        
            sitemap = yaml.full_load(f)

        sitemap_urls = [self._path_to_url(url) for url in sitemap]



        # # sitemap_urls = sitemap_urls[:10]

        # # sitemap_urls = ['https://aba.myspecies.info/biblio/author/tus_biblio_year/1937']

        url_stack = UniqueStack(sitemap_urls)

        re_skip = re.compile(r'file/[0-9]+$|file-colorboxed/[0-9]+$|node/[0-9]+|user/|content/|taxonomy/term/[0-9]+|simpletaxonomy/term/[0-9]+|nontaxonomy/term/[0-9]+|category/|comment/')

        # url_stack = UniqueStack(['https://microgastrinae.myspecies.info/gallery'])

        for url in url_stack:

            # No point crawling file pages
            # And lets skip some more to speed this up
            if re_skip.search(url):
                continue

            logger.debug(f'Crawling {url} for links')

            try:
                soup = get_soup(url)
            except Exception as e:
                logger.warning(e)
                url_stack.discard(url)        
            else:

                if soup.find('h1', string="Technical difficulties"):
                    logger.errot(f'Technical difficulties {url}') 
                    continue
                            
                hrefs = self._page_get_hrefs(soup)
                # Remove any trailing /s
                hrefs = [re.sub('/+$', '', href) for href in hrefs]
                # Filter out any hrefs we know we want to exclude
                hrefs = [href for href in hrefs if not is_decommisionned_link(href)]  

                before = set(url_stack._seen)
                url_stack.update(hrefs)
                num_new = len(url_stack._seen) - len(before)

                logger.debug(f'{num_new} new URLS added to stack (total {len(url_stack._seen)}). {len(url_stack._items)} URLs to process')

        print('NEW:')
        print(url_stack._seen.difference(sitemap_urls))

        print('Links found: ', len(url_stack))

        with self.output().open('w') as f: 
            logger.debug(f'Crawl data outputted to {self.output().path}')
            yaml.dump(url_stack.items, f)        

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')           

    def _path_to_url(self, path):
        return urlunparse((SCHEME, self.domain, path, '', '', ''))
    
    @staticmethod
    def _is_sitemap_path(path):
        # Define the regular expression pattern
        pattern = r"^\/?(node|user|taxonomy\/term)\/\d+$"
        
        # Use re.match() to check if the input string matches the pattern
        return bool(re.match(pattern, path))    
    
    def _page_get_hrefs(self, soup):   

        aliases = get_site_aliases(self.domain)
        alias_paths = {a[0] for a in aliases}

        links = soup.find_all('a')
        
        for link in links:
            href = link.get('href')

            url = URL(href, self.domain)

            # parsed_link = urlparse(href)

            if not href or not url.path:
                continue 

            if url.path.endswith('csv'):
                continue
                
            #Filter out fragment only links (skip to content etc.,)
            if not url.path and url.parsed_url.fragment:
                continue

            if url.extension() and url.extension() not in ['html', 'php']:
                continue

            # If this is a URL pattern we should have from the sitemap, skip it
            # Could be ignored due to perms etc.,  
            if self._is_sitemap_path(url.path):
                logger.debug(f'Skipping sitemap path {url.path}')
                continue

            if url.path.strip('/') in alias_paths:
                logger.debug(f'Skipping alias {url.path}')
                continue

            # Skip search links with parameters
            if 'search' in url.path and url.parsed_url.query:
                continue

            # url = URL(href, self.domain)

            if not url.is_site_internal_link():
                continue

            resolved_path = url.to_path()
            if resolved_path in self._seen_paths:                
                continue

            self._seen_paths.add(resolved_path)

            if url.is_faceted():
                yield from url.get_single_facet_urls() 
            else:
                yield url.get_normalised()
    
if __name__ == "__main__":    
    domain = 'aframomum.myspecies.info'
    
    luigi.build([CrawlSiteTask(domain=domain, force=True)], local_scheduler=True)   
