from bs4 import BeautifulSoup
import requests
import requests_cache
import re
from pathlib import Path
import urllib
# from urllib import parse_qsl,urlencode
from operator import itemgetter

from htmlify.config import DATA_DIR

session = requests_cache.CachedSession(DATA_DIR / '.cache')

links = [
    r'biblio/export/[a-z]+/[0-9]+$',
    r'biblio_search_export/[a-z]+',
    r'taxonomy/term/[0-9]+/feed',
    r'blog/[0-9]+/feed',
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
    r'slickgrid_export_form'
]    

links_with_domain = [f'(?:https?://[^/]+/)?/?{l}' for l in links]
url_pattern = re.compile('|'.join(links_with_domain))

def is_decommisionned_link(link):
    return bool(url_pattern.search(link)) or 'raw-plain' in link


def request(url, cached=True):
    if cached:
        r = session.get(url, allow_redirects=True)
    else:
        r = requests.get(url, allow_redirects=True)

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


if __name__ == "__main__":
    links = [
        'https://acanthaceae.myspecies.info/sites/acanthaceae.myspecies.info/files/filesq9hmR',
        'https://acanthaceae.myspecies.info/taxonomy/term/149%2B150%2B151%2B152%2B153%2B154%2B155%2B156%2B157%2B158%2B159%2B160%2B161%2B162%2B163%2B164%2B165%2B166%2B167%2B168%2B169%2B170%2B171%2B172%2B173%2B174%2B175%2B176%2B177%2B178%2B179%2B180%2B181%2B182%2B183%2B184%2B185%2B186%2B187%2B188%2B189%2B190%2B191/xml',
        'https://acanthaceae.myspecies.info/slickgrid/get/form/slickgrid_export_form',
        'https://acanthaceae.myspecies.info/modal_forms/nojs/contact/1',
        'https://acanthaceae.myspecies.info/modal_formsraw-plain/nojs/contact/1',
        'https://acanthaceae.myspecies.info/sites/acanthaceae.myspecies.info/files/fileD2BJK7'
    ] 
    for link in links:
        print(link)
        print(is_decommisionned_link(link))