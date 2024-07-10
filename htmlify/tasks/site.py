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


from htmlify.config import DATA_DIR
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup

class PageTask(BaseTask):
    url = luigi.Parameter()

    def run(self):

        parsed_url = urlparse(self.url)
        self.domain = parsed_url.netloc
        # self.subdomain = parsed_url.netloc.replace('myspecies.info', '')



        print('PAGE:', self.domain)


class SiteTask(BaseTask):

    domain = luigi.Parameter()

    def requires(self):  
        # We have to do the dynamic dependencies this way, as if we put tasks
        # in run we get the error
        crawl_task = CrawlSiteTask(domain=self.domain)
        luigi.build([crawl_task], local_scheduler=True)
        # We don;t have t yield this - but keeps the dependency graph accurate
        yield crawl_task
        with crawl_task.output().open() as f:
            links = yaml.full_load(f)
            for link in links:
                yield PageTask(url=link)    
                break

    def run(self):
        print('RUN')

        # with self.input().open('r') as f:        
        #     links = yaml.full_load(f)

        # print(links)


if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([PageTask(url='http://127.0.0.1/taxonomy/term/8', force=True)], local_scheduler=True)         