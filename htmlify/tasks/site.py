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


from htmlify.config import SITES_DIR, DATA_DIR, DB_PASSWORD, DB_USERNAME, PLATFORMS_ROOT_PATH
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.tasks.page import PageTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, get_first_directory



class SiteTask(BaseTask):

    domain = luigi.Parameter()
    platform_path = luigi.PathParameter()
    db_name = luigi.Parameter()
    db_host = luigi.Parameter()

    def __init__(self,  *args, **kwargs):
        super().__init__(*args, **kwargs)  
        self.connection = mysql.connector.connect(
            host=self.db_host,
            port=3306,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=self.db_name
        )         

    def __del__(self):
        self.connection.close()

    def requires(self):      

        self.setup()

        # We have to do the dynamic dependencies this way, as if we put tasks
        # in run we get the error
        crawl_task = CrawlSiteTask(domain=self.domain, db_conn=self.connection)
        luigi.build([crawl_task], local_scheduler=True)
        # We don;t have t yield this - but keeps the dependency graph accurate
        yield crawl_task
        with crawl_task.output().open() as f:
            links = yaml.full_load(f)
            for link in links:
                yield PageTask(url=link, output_dir=self.output().path)    


    def setup(self):

        # Copy accross additional assets
        ASSETS_DIR = Path(DATA_DIR / 'assets')
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

    def output(self):
        return luigi.LocalTarget(SITES_DIR / self.domain)
    
    def complete(self):
        return (Path(self.output().path) / 'index.html').exists()

if __name__ == "__main__":    



    domain = '127.0.0.1'
    luigi.build([
        SiteTask(
            domain=domain, 
            db_name='drupal',
            db_host='127.0.0.1',
            platform_path='/Users/ben/Projects/Scratchpads/Code/scratchpads2',
            force=True
        )], local_scheduler=True)         