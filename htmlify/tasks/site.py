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


from htmlify.config import PLATFORMS_DIR, SITES_DIR, DATA_DIR
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.tasks.page import PageTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup



class SiteTask(BaseTask):

    domain = luigi.Parameter()

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)            



    def requires(self):  
        # We have to do the dynamic dependencies this way, as if we put tasks
        # in run we get the error
        crawl_task = CrawlSiteTask(domain=self.domain)
        luigi.build([crawl_task], local_scheduler=True)
        # We don;t have t yield this - but keeps the dependency graph accurate
        yield crawl_task
        # with crawl_task.output().open() as f:
        #     links = yaml.full_load(f)
        #     for link in links:
        #         yield PageTask(url=link, output_dir=self.output().path)    

    def run(self):

        # Copy accross additional assets
        ASSETS_DIR = Path(DATA_DIR / 'assets')

        dest_assets_dir = Path(self.output().path) / 'assets'

        if dest_assets_dir.exists():
            shutil.rmtree(dest_assets_dir)

        shutil.copytree(ASSETS_DIR, dest_assets_dir)

        pass

        # for i in self.input():
        #     if i.path.endswith('.html'):
        #         with i.open('r') as f:  
        #             soup = BeautifulSoup(f, "html.parser")  

        #             if not soup.find('div', id='section-content'):
        #                 raise Exception(f'HTML file has no content: {i.path}')

        #             # TODO


        #     print(type(i))

        # with self.input().open('r') as f:  
        #     soup = BeautifulSoup(self.driver.page_source, "html.parser")      
        #     print(soup)
        #     break
        #     # break
        #     pass
        #     urls = yaml.full_load(f)
        #     luigi.build([
        #         PageTask(url=url, output_dir=self.output().path) for url in urls
        #     ], local_scheduler=True)

        # print(links)

    def setup(self):
        print('SETUP')

    def output(self):
        return luigi.LocalTarget(SITES_DIR / self.domain)


if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([SiteTask(domain=domain, force=True)], local_scheduler=True)         