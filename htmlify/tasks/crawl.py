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


from htmlify.config import DATA_DIR
from htmlify.tasks.base import BaseTask
from htmlify.tasks.sitemap import SiteMapTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup

class CrawlSiteTask(BaseTask):

    domain = luigi.Parameter()
    
    output_dir = DATA_DIR / 'crawl'    
    scheme = 'http'

    urls_regex_to_remove = [
        r'http://127.0.0.1/biblio/export/[a-z]+/[0-9]+',
        r'http://127.0.0.1/biblio_search_export/[a-z]+',
        r'http://127.0.0.1/taxonomy/term/[0-9]+/feed',
        r'http://127.0.0.1/comment/reply/[0-9]+',
        r'http://127.0.0.1/user/[0-9]+/contact',
        r'rss.xml'
    ]    

    urls_regex_to_remove_comp = [re.compile(r) for r in urls_regex_to_remove]

    def requires(self):
        return SiteMapTask(domain=self.domain) 
    
    def run(self):

        with self.input().open('r') as f:        
            sitemap = yaml.full_load(f)


        sitemap_urls = [self._path_to_url(url) for url in sitemap]

        url_stack = UniqueStack(sitemap_urls)

        for url in url_stack:
            hrefs = self._page_get_hrefs(url)
            # Remove any trailing /s
            hrefs = [re.sub('/+$', '', href) for href in hrefs]
            # Filter out any hrefs we know we want to exclude
            hrefs = [href for href in hrefs if not self.is_url_to_remove(href)]
            url_stack.update(hrefs)
            
        print('NEW:')
        print(url_stack._seen.difference(sitemap_urls))

        print('Links found: ', len(url_stack))

        with self.output().open('w') as f: 
            yaml.dump(url_stack.items, f)        

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')           

    def is_url_to_remove(self, url):
        for regex in self.urls_regex_to_remove_comp:
            if regex.match(url):
                return True
        return False        

    def _path_to_url(self, path):
        return urlunparse((self.scheme, self.domain, path, '', '', ''))
    
    def _page_get_hrefs(self, url):

        try:
            soup = get_soup(url)
        except requests.HTTPError as e:
            print(e)
            pass
        else:    
            links = soup.find_all('a')
            for link in links:
                href = link.get('href')

                parsed_link = urlparse(href)

                if not href or not parsed_link.path:
                    continue 
                    
                #Filter out fragment only links (skip to content etc.,)
                if not parsed_link.path and parsed_link.fragment:
                    continue

                if parsed_link.path:
                    extension = os.path.splitext(parsed_link.path)[1]
                    if extension and extension not in ['html', 'php']:
                        continue

                # Is this the domain we're looking at?
                # Netloc is empty (relative link) - or netloc == domain
                if not parsed_link.netloc:
                    yield urlunparse((self.scheme, self.domain, parsed_link.path, '', '', ''))
                elif parsed_link.netloc in self.domain:
                    yield href    
    

    
if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([CrawlSiteTask(domain=domain, force=True)], local_scheduler=True)   
