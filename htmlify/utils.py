from bs4 import BeautifulSoup
import requests
import requests_cache
import re
from pathlib import Path
import urllib
import urllib3
from urllib.parse import urlparse, urlunparse
# from urllib import parse_qsl,urlencode
from operator import itemgetter

from htmlify import config
from htmlify.db import db_manager

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
session = requests_cache.CachedSession(config.DATA_DIR / '.cache')

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


class SiteUnavailableError(RuntimeError):
    """Raised when the source site cannot be read before migration."""


def is_decommisionned_link(link):
    return bool(url_pattern.search(link)) or 'raw-plain' in link


def _normalise_hostname(hostname):
    if not hostname:
        return ''
    hostname = hostname.lower()
    return hostname[4:] if hostname.startswith('www.') else hostname


def is_internal_url(url, domain):
    """Return whether a URL belongs to the canonical site or local source."""
    parsed = urlparse(url)
    if not parsed.netloc:
        return True

    hostname = _normalise_hostname(parsed.hostname)
    if hostname == _normalise_hostname(domain):
        return True

    if config.SOURCE_BASE_URL and domain == config.SOURCE_DOMAIN:
        source_hostname = urlparse(config.SOURCE_BASE_URL).hostname
        return hostname == _normalise_hostname(source_hostname)

    return False


def get_source_url(url):
    """Map a canonical site URL to the configured local rendering origin."""
    if not config.SOURCE_BASE_URL or not config.SOURCE_DOMAIN:
        return url

    parsed = urlparse(url)
    if _normalise_hostname(parsed.hostname) != _normalise_hostname(config.SOURCE_DOMAIN):
        return url

    source = urlparse(config.SOURCE_BASE_URL)
    return urlunparse((
        source.scheme,
        source.netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))


def canonicalize_source_references(html, domain):
    """Replace local source origins in HTML, including JSON-escaped URLs."""
    if not config.SOURCE_BASE_URL or domain != config.SOURCE_DOMAIN:
        return html

    source = urlparse(config.SOURCE_BASE_URL)
    source_host = re.escape(source.hostname or '')
    canonical_origin = f'{config.SCHEME}://{domain}'
    canonical_escaped_origin = canonical_origin.replace('/', r'\/')

    html = re.sub(
        rf'https?://{source_host}(?::\d+)?',
        canonical_origin,
        html,
    )
    html = re.sub(
        rf'https?:\\/\\/{source_host}(?::\d+)?',
        lambda _match: canonical_escaped_origin,
        html,
    )
    html = re.sub(
        rf'(?<!:)//{source_host}(?::\d+)?',
        f'//{domain}',
        html,
    )
    return html


def request(url, cached=True):
    parsed = urlparse(url)
    source_request = False
    if config.SOURCE_BASE_URL and config.SOURCE_DOMAIN:
        source_hostname = urlparse(config.SOURCE_BASE_URL).hostname
        request_hostname = _normalise_hostname(parsed.hostname)
        source_request = request_hostname in {
            _normalise_hostname(config.SOURCE_DOMAIN),
            _normalise_hostname(source_hostname),
        }

    url = get_source_url(url)
    # A single DDEV hostname is reused while different Scratchpad databases
    # are imported. Caching that URL would allow one site's response to leak
    # into the next site's archive.
    if cached and not source_request:
        r = session.get(
            url,
            allow_redirects=True,
            verify=False,
            timeout=config.REQUEST_TIMEOUT,
        )
    else:
        r = requests.get(
            url,
            allow_redirects=True,
            verify=False,
            timeout=config.REQUEST_TIMEOUT,
        )

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
    # A configured local source is already known to be the requested site. A
    # separate homepage render adds latency and can abort Luigi scheduling on a
    # transient DDEV timeout; actual page requests still enforce HTTP errors.
    if config.SOURCE_BASE_URL and domain == config.SOURCE_DOMAIN:
        return False

    url = f'{config.SCHEME}://{domain}'
    last_error = None
    last_status = None
    for _attempt in range(2):
        try:
            r = requests.get(
                url,
                allow_redirects=True,
                verify=False,
                # Match the read timeout used for normal page rendering. The
                # old ten-second probe produced false negatives for slow but
                # otherwise readable Scratchpads.
                timeout=(5, config.REQUEST_TIMEOUT),
            )
            last_status = r.status_code
            if r.status_code == 503 and 'Site under maintenance' in r.text:
                return True
            r.raise_for_status()
            return False
        except requests.exceptions.RequestException as error:
            last_error = error

    if last_status is not None:
        raise SiteUnavailableError(
            f'Source site {url} returned HTTP {last_status} after 2 attempts'
        ) from last_error
    raise SiteUnavailableError(
        f'Cannot read source site {url} after 2 attempts: {last_error}'
    ) from last_error

if __name__ == "__main__":
    x = is_under_maintenance('dipteratyoryhma.myspecies.info')
    print(x)
