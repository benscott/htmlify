from bs4 import BeautifulSoup
import requests
import requests_cache
import re
from pathlib import Path


session = requests_cache.CachedSession('.cache')

links = [
    r'biblio/export/[a-z]+/[0-9]+$',
    r'biblio_search_export/[a-z]+',
    r'taxonomy/term/[0-9]+/feed',
    r'comment/reply/[0-9]+',
    r'user/[0-9]+/contact',
    r'rss\.xml',
    r'contact/1',
    r'contact/2',
    r'modal_forms/nojs/contact/1',
    r'modal_forms/nojs/contact/2',
    r'biblio\.bib',
]    

links_with_domain = [f'(?:https?://[^/]+/)?/?{l}' for l in links]
url_pattern = re.compile('|'.join(links_with_domain))

def is_decommisionned_link(link):
    return bool(url_pattern.match(link)) or 'raw-plain' in link


def get_soup(url, cached=True):
    if cached:
        r = session.get(url)
    else:
        r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text)

def concat_path(p1: Path, p2: Path):
    # Strip first slash if it exists
    p2 = p2[1:] if p2.startswith('/') else p2
    return p1 / p2
