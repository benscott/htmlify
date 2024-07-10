from bs4 import BeautifulSoup
import requests
import requests_cache


session = requests_cache.CachedSession('.cache')

def get_soup(url, cached=True):
    if cached:
        r = session.get(url)
    else:
        r = requests.get(url)
    r.raise_for_status()
    return BeautifulSoup(r.text)