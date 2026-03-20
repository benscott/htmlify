import luigi
import shutil
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
import typer
import pandas as pd


from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import requests_cache


import mysql.connector


from htmlify.config import CRAWL_SITES, SITES_DIR, ASSETS_DIR, TEMPLATE_DIR, DB_PASSWORD, DB_USERNAME, PLATFORMS_ROOT_PATH, APACHE_VHOSTS_DIR, logger
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.tasks.page import PageTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, get_first_directory, get_site_aliases, is_under_maintenance
from htmlify.tasks.sitemap import SiteMapTask
from htmlify.url import URL
from htmlify.db import db_manager


MAX_PAGE_THRESHOLD = 10000

class SiteTask(BaseTask):

    domain = luigi.Parameter()
    platform_path = luigi.PathParameter()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)  
        if is_under_maintenance(self.domain):
            raise Exception(f'{self.domain} is under maintenance')
    
    def requires(self):      

        self.setup()

        # We have to do the dynamic dependencies this way, as if we put tasks
        # in run we get the error
        task = CrawlSiteTask(domain=self.domain)
        luigi.build([task], local_scheduler=True)
        
        # We don't have to yield this - but keeps the dependency graph accurate
        yield task
        
        with task.output().open() as f:
            links = yaml.full_load(f)

            if MAX_PAGE_THRESHOLD and len(links) > MAX_PAGE_THRESHOLD:
                raise Exception(f'Links exceed {MAX_PAGE_THRESHOLD} maximum {len(links)}')

            for link in links:
                if self.url_is_valid_filename(link):
                    yield PageTask(url=link, output_dir=self.output().path)    

    def url_is_valid_filename(self, link):
        url  = URL(link)
        path = url.to_path()
        # Ensure no parts of the URL will be longer than 255
        max_filename_len = max([len(p) for p in path.split('/')])
        if max_filename_len >= 255:
            return False
        
        return True

    def setup(self):

        # Copy accross additional assets
        self.copy_across_assets()

        # Symlink drupal directory (files etc.,)
        self.symlink_drupal_dir()

        self.setup_vhost()
    
    def symlink_drupal_dir(self):

        sites_dir = Path(self.output().path)
        # Scratchped with have file url as /sites/domain/... so we symlink
        # to the appropriate 
        symlink_path = (sites_dir / 'sites')
        symlink_path.parent.mkdir(parents=True, exist_ok=True)

        if not symlink_path.exists():

            # If platforms are mounted in a different location
            if PLATFORMS_ROOT_PATH:
                relative_path = self.platform_path.relative_to(get_first_directory(self.platform_path))
                platform_path = PLATFORMS_ROOT_PATH / relative_path
            else:
                platform_path = self.platform_path 

            # print(symlink_path)
            platform_sites_path = platform_path / 'sites'

            if not platform_sites_path.exists():
                raise FileNotFoundError(f'platform path {platform_sites_path} does not exist. This should be the path to where the drupal source files are mounted. ')

            symlink_path.symlink_to(platform_sites_path)        

    def copy_across_assets(self):
        # Copy across assets like style.css
        sites_dir = Path(self.output().path)
        dest_assets_dir = sites_dir / 'assets'

        if dest_assets_dir.exists():
            shutil.rmtree(dest_assets_dir)

        shutil.copytree(ASSETS_DIR, dest_assets_dir)        


    def setup_vhost(self):

        sites_dir = Path(self.output().path)

        with (TEMPLATE_DIR / 'vhosts.tpl').open('r') as f:
            content = f.read()    
            content = content.replace('{{DOMAIN}}', self.domain)
            content = content.replace('{{DOCUMENT_ROOT}}', str(sites_dir))

        vhosts_path = APACHE_VHOSTS_DIR / f'{self.domain}.conf'

        with vhosts_path.open('w') as outf:
            outf.write(content)            

    def run(self):
        logger.debug(f'Creating symlinks')
        sites_dir = Path(self.output().path)
        for alias, path in get_site_aliases(self.domain):

            symlink_path = sites_dir / alias
            target_path = sites_dir / path
            symlink_path.parent.mkdir(parents=True, exist_ok=True)

            if symlink_path.is_symlink():
                resolved_path = symlink_path.resolve()
                if resolved_path != target_path:
                    symlink_path.unlink()
                    symlink_path.symlink_to(target_path)
            else:
                symlink_path.symlink_to(target_path)

    def output(self):
        return luigi.LocalTarget(SITES_DIR / self.domain)

    def complete(self):
        try:
            # Check all the inputs exist
            for i in self.input():
                if not Path(i.path).exists():
                    return False
        except FileNotFoundError:
            return False
        
        sites_dir = Path(self.output().path)

        # Ensure symlinks (site aliases) all exist
        for alias, path in get_site_aliases(self.domain):

            symlink_path = sites_dir / alias
            target_path = sites_dir / path

            if not symlink_path.is_symlink():
                 logger.error(f'ALIAS {symlink_path} does not exist')
                 return False

            if not target_path.exists():
                logger.error(f'Target of alias {target_path} does not exist')
                return False
            
        db_manager.close_connection(self.domain)
            
        return True
    


if __name__ == "__main__":    

    # sites_list_task = SitesListTask()
    domain = 'dipteratyoryhma.myspecies.info'
    luigi.build([
        SiteTask(
            domain=domain, 
            platform_path='/var/aegir/platforms/scratchpads-2.10.1',
            force=True
        )], local_scheduler=True)         