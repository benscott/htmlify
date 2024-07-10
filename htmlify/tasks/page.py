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


from htmlify.config import DATA_DIR, logger
from htmlify.tasks.base import BaseTask
from htmlify.tasks.crawl import CrawlSiteTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup


class PageTask(BaseTask):
    url = luigi.Parameter()

    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # for Chrome >= 109
    driver = webdriver.Chrome(options=chrome_options)

    def run(self):

        parsed_url = urlparse(self.url)
        self.domain = parsed_url.netloc
        # self.subdomain = parsed_url.netloc.replace('myspecies.info', '')

        self.driver.get(self.url)
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        if self._is_tiny_tax(soup):
            logger.debug('HEYEYEYEYE')

        print('PAGE:', self.domain)

    def _is_tiny_tax(self, soup):
        return bool(soup.find('section', {"class": "block-tinytax"}))        


if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([PageTask(url='http://127.0.0.1/taxonomy/term/8', force=True)], local_scheduler=True)           
