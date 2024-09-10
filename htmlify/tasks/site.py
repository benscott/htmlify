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


from htmlify.config import SITES_DIR, ASSETS_DIR, DB_PASSWORD, DB_USERNAME, PLATFORMS_ROOT_PATH, APACHE_VHOSTS_DIR, logger
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.tasks.page import PageTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, get_first_directory
from htmlify.tasks.sites_list import SitesListTask
from htmlify.url import URL


class SiteTask(BaseTask):

    domain = luigi.Parameter()
    platform_path = luigi.PathParameter()
    # db_name = luigi.Parameter()
    # db_host = luigi.Parameter()
    # connection = None

    # def __init__(self,  *args, **kwargs):
    #     super().__init__(*args, **kwargs)  
    #     self.connection = mysql.connector.connect(
    #         host=self.db_host,
    #         port=3306,
    #         user=DB_USERNAME,
    #         password=DB_PASSWORD,
    #         database=self.db_name
    #     )
    #     if not self.connection.is_connected():
    #         raise ConnectionError()

    # def __del__(self):
    #     if self.connection:
    #         self.connection.close()

    def requires(self):      

        self.setup()

        # We have to do the dynamic dependencies this way, as if we put tasks
        # in run we get the error
        # crawl_task = CrawlSiteTask(domain=self.domain, db_conn=self.connection)
        crawl_task = CrawlSiteTask(domain=self.domain)
        luigi.build([crawl_task], local_scheduler=True)
        # We don;t have t yield this - but keeps the dependency graph accurate
        yield crawl_task
        
        with crawl_task.output().open() as f:
            links = yaml.full_load(f)
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
        sites_dir = Path(self.output().path)

        dest_assets_dir = sites_dir / 'assets'

        symlink_path = (sites_dir / 'sites')
        symlink_path.parent.mkdir(parents=True, exist_ok=True)

        if not symlink_path.exists():

            # If platforms are mounted in a different location
            if PLATFORMS_ROOT_PATH:
                relative_path = self.platform_path.relative_to(get_first_directory(self.platform_path))
                platform_path = PLATFORMS_ROOT_PATH / relative_path
            else:
                platform_path = self.platform_path         
            
            symlink_path.symlink_to(platform_path / 'sites')

        if dest_assets_dir.exists():
            shutil.rmtree(dest_assets_dir)

        shutil.copytree(ASSETS_DIR, dest_assets_dir)

        self.setup_vhost()

    def setup_vhost(self):

        sites_dir = Path(self.output().path)

        with (ASSETS_DIR / 'vhosts.tpl').open('r') as f:
            content = f.read()    
            content = content.replace('{{DOMAIN}}', self.domain)
            content = content.replace('{{DOCUMENT_ROOT}}', str(sites_dir))

        vhosts_path = APACHE_VHOSTS_DIR / f'{self.domain}.conf'

        with vhosts_path.open('w') as outf:
            outf.write(content)            



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

        return True
    


if __name__ == "__main__":    

    # sites_list_task = SitesListTask()
    domain = 'acanthaceae.myspecies.info'
    luigi.build([
        SiteTask(
            domain=domain, 
            platform_path='/var/aegir/platforms/scratchpads-2.10.1',
            force=True
        )], local_scheduler=True)         