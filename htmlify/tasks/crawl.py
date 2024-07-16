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
from htmlify.utils import get_soup, is_decommisionned_link

class CrawlSiteTask(BaseTask):

    domain = luigi.Parameter()
    db_conn = luigi.Parameter()
    
    output_dir = PROCESSING_DATA_DIR / 'crawl'    

    def requires(self):
        return SiteMapTask(domain=self.domain) 
    
    def run(self):

        with self.input().open('r') as f:        
            sitemap = yaml.full_load(f)

        sitemap_urls = [self._path_to_url(url) for url in sitemap]

        url_stack = UniqueStack(sitemap_urls)

        for url in url_stack:
            try:
                soup = get_soup(url)
            except requests.HTTPError as e:
                logger.warning(e)
                url_stack.discard(url)        
            else:
                hrefs = self._page_get_hrefs(soup)
                # Remove any trailing /s
                hrefs = [re.sub('/+$', '', href) for href in hrefs]
                # Filter out any hrefs we know we want to exclude
                hrefs = [href for href in hrefs if not is_decommisionned_link(href)]
                url_stack.update(hrefs)
            
        print('NEW:')
        print(url_stack._seen.difference(sitemap_urls))

        print('Links found: ', len(url_stack))

        with self.output().open('w') as f: 
            yaml.dump(url_stack.items, f)        

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')           

    def _path_to_url(self, path):
        return urlunparse((SCHEME, self.domain, path, '', '', ''))
    
    def _page_get_hrefs(self, soup):         
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
                yield urlunparse((SCHEME, self.domain, parsed_link.path, '', '', ''))
            elif parsed_link.netloc in self.domain:
                yield href    
    

    
if __name__ == "__main__":    
    domain = '127.0.0.1'
    db_conn = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database='drupal'
        )      
    luigi.build([CrawlSiteTask(domain=domain, db_conn=db_conn, force=True)], local_scheduler=True)   
