from bs4 import BeautifulSoup
import requests
import requests_cache
import re
from pathlib import Path
import urllib
import urllib3
# from urllib import parse_qsl,urlencode
from operator import itemgetter

from htmlify.config import DATA_DIR
from htmlify.db import db_manager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
session = requests_cache.CachedSession(DATA_DIR / '.cache')

links = [
    r'biblio/export/[a-z]+/[0-9]+$',
    r'biblio_search_export/[a-z]+',
    r'taxonomy/term/[0-9]+/feed',
    r'blog/[0-9]+/feed',
    r'node/[0-9]+/view',
    r'node/[0-9]+/revisions',
    r'taxonomy/term/[0-9]+/revisions',
    r'taxonomy/term/[0-9]+/view',
    r'comment/[0-9]+/view',
    r'comment/reply/[0-9]+',
    r'user/[0-9]+/contact',
    r'rss\.xml',
    r'contact/1',
    r'contact/2',
    r'modal_forms/nojs/contact/1',
    r'modal_forms/nojs/contact/2',
    r'biblio\.bib',
    r'files/file[a-zA-Z0-9]+$',
    r'xml$',
    r'raw-plain',
    r'slickgrid_export_form',
    r'user/register'
]    

links_with_domain = [f'(?:https?://[^/]+/)?/?{l}' for l in links]
url_pattern = re.compile('|'.join(links_with_domain))

def is_decommisionned_link(link):
    return bool(url_pattern.search(link)) or 'raw-plain' in link


def request(url, cached=True):
    if cached:
        r = session.get(url, allow_redirects=True, verify=False)
    else:
        r = requests.get(url, allow_redirects=True, verify=False)

    r.raise_for_status()    
    return r


def get_soup(url, cached=True):
    r = request(url, cached)
    return BeautifulSoup(r.text, features="html.parser")

def request_json(url, cached=True):
    r = request(url, cached)
    return r.json()

def concat_path(p1: Path, p2: Path):
    # Strip first slash if it exists
    p2 = p2[1:] if p2.startswith('/') else p2
    return p1 / p2

def get_first_directory(path):
    # Convert to Path object if it's not already
    path = Path(path)
    
    # Split the path into its parts
    parts = path.parts
    
    # Find the first part that is not the root
    for part in parts:
        if part != path.anchor:
            return Path(path.anchor) / part

def get_site_aliases(domain):
    sql = "SELECT alias, source FROM url_alias"
    return db_manager.fetch(domain, sql)

def is_under_maintenance(domain):
    url = f'http://{domain}'
    r = requests.get(url)
    if r.status_code == 503 and 'Site under maintenance' in r.text:
        return True

if __name__ == "__main__":
    x = is_under_maintenance('dipteratyoryhma.myspecies.info')
    print(x)