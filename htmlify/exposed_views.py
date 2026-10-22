"""Convert paged Drupal Views exposed forms into static client-side filters."""

from copy import deepcopy
import re
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from htmlify.config import logger
from htmlify.utils import get_soup, is_internal_url


EXPOSED_VIEW_VERSION = '1'
MAX_EXPOSED_VIEW_PAGES = 1000


def exposed_views(soup):
    """Return top-level Views which contain a rendered exposed filter form."""
    views = []
    for form in soup.select('.view form .views-exposed-form'):
        view = form.find_parent(class_='view')
        if view is None or view in views:
            continue
        # A nested View belongs to the nearest .view only.
        views.append(view)
    return views


def view_identity(view):
    """Return stable Drupal View classes, excluding the per-request DOM id."""
    classes = set(view.get('class', []))
    view_id = next(
        (name for name in classes if name.startswith('view-id-')),
        None,
    )
    display_id = next(
        (name for name in classes if name.startswith('view-display-id-')),
        None,
    )
    if not view_id:
        return None
    return view_id, display_id


def find_matching_view(soup, identity):
    if not identity:
        return None
    view_id, display_id = identity
    for view in soup.select(f'.{view_id}'):
        if display_id is None or display_id in view.get('class', []):
            return view
    return None


def result_rows(view):
    """Return result rows owned by this View, avoiding rows in nested Views."""
    rows = view.select('.view-content .views-row')
    if not rows:
        rows = view.select('.view-content tbody > tr')
    return [row for row in rows if row.find_parent(class_='view') is view]


def _canonical_page_url(url):
    parsed = urlparse(url)
    path = re.sub(r'/+', '/', parsed.path or '/')
    if path != '/':
        path = path.rstrip('/')
    return urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        '',
        parsed.query,
        '',
    ))


def pager_urls(view, page_url, domain):
    """Return internal result-page links from this View's own pager."""
    urls = []
    for link in view.select('.pager a[href]'):
        absolute = urljoin(page_url, link['href'])
        if not is_internal_url(absolute, domain):
            continue
        parsed = urlparse(absolute)
        if parsed.scheme not in {'http', 'https'}:
            continue
        urls.append(_canonical_page_url(absolute))
    return list(dict.fromkeys(urls))


def collect_view_pages(page_url, initial_view, fetch=get_soup):
    """Follow a View pager and return its matching view from every page."""
    domain = urlparse(page_url).netloc
    identity = view_identity(initial_view)
    start_url = _canonical_page_url(page_url)
    pages = [(start_url, initial_view)]
    seen = {start_url}
    queued = pager_urls(initial_view, page_url, domain)

    while queued:
        next_url = queued.pop(0)
        canonical = _canonical_page_url(next_url)
        if canonical in seen:
            continue
        if len(seen) >= MAX_EXPOSED_VIEW_PAGES:
            raise RuntimeError(
                f'Exposed View on {page_url} has more than '
                f'{MAX_EXPOSED_VIEW_PAGES} result pages'
            )
        seen.add(canonical)
        page_soup = fetch(next_url)
        matching = find_matching_view(page_soup, identity)
        if matching is None:
            raise RuntimeError(
                f'Could not find View {identity!r} on pager page {next_url}'
            )
        pages.append((canonical, matching))
        for discovered in pager_urls(matching, next_url, domain):
            if discovered not in seen and discovered not in queued:
                queued.append(discovered)

    return pages


def _page_number(url, base_url):
    """Return Drupal's zero-based pager number for stable row ordering."""
    if _canonical_page_url(url) == _canonical_page_url(base_url):
        return 0
    parsed = urlparse(url)
    query_match = re.search(r'(?:^|&)page=(?:\d+,)*?(\d+)(?:&|$)', parsed.query)
    if query_match:
        return int(query_match.group(1))
    path_match = re.search(r'/page/(\d+)/?$', parsed.path)
    if path_match:
        return int(path_match.group(1))
    return MAX_EXPOSED_VIEW_PAGES


def _normalise_row_classes(row, position, total):
    classes = [
        name for name in row.get('class', [])
        if not re.fullmatch(r'views-row-\d+', name)
        and name not in {'views-row-odd', 'views-row-even', 'views-row-first', 'views-row-last'}
    ]
    if 'views-row' not in classes:
        classes.append('views-row')
    classes.extend([
        f'views-row-{position + 1}',
        'views-row-odd' if position % 2 == 0 else 'views-row-even',
        'htmlify-exposed-view-row',
    ])
    if position == 0:
        classes.append('views-row-first')
    if position == total - 1:
        classes.append('views-row-last')
    row['class'] = list(dict.fromkeys(classes))


def _prepare_form(view, page_url):
    exposed = view.select_one('form .views-exposed-form')
    form = exposed.find_parent('form') if exposed else None
    if form is None:
        return

    form['class'] = list(dict.fromkeys([
        *form.get('class', []),
        'htmlify-exposed-view-search',
    ]))
    form['action'] = urlparse(page_url).path or '/'
    form['method'] = 'get'
    form['autocomplete'] = 'off'
    for hidden in form.select(
        'input[type="hidden"][name="form_build_id"], '
        'input[type="hidden"][name="form_id"], '
        'input[type="hidden"][name="op"]'
    ):
        hidden.decompose()
    for field in form.select('input.form-autocomplete'):
        field['class'] = [
            name for name in field.get('class', [])
            if name != 'form-autocomplete'
        ]
        field['autocomplete'] = 'off'

    if not form.select_one('.htmlify-exposed-view-clear'):
        widget = BeautifulSoup(
            '<div class="views-exposed-widget htmlify-exposed-view-clear-widget">'
            '<button class="htmlify-exposed-view-clear" type="button">'
            'Clear</button></div>',
            'html.parser',
        ).div
        widgets = form.select_one('.views-exposed-widgets') or exposed
        widgets.append(widget)


def staticize_exposed_view(soup, view, page_url, fetch=get_soup):
    """Merge all result pages and wire one exposed View for local filtering."""
    pages = collect_view_pages(page_url, view, fetch=fetch)
    pages.sort(key=lambda item: _page_number(item[0], page_url))
    all_rows = []
    for _url, page_view in pages:
        all_rows.extend(deepcopy(row) for row in result_rows(page_view))

    original_rows = result_rows(view)
    if original_rows:
        container = original_rows[0].parent
        for row in original_rows:
            row.decompose()
    else:
        container = view.select_one('.view-content')

    if container is not None:
        for position, row in enumerate(all_rows):
            _normalise_row_classes(row, position, len(all_rows))
            container.append(row)

    for pager in view.select('.pager'):
        pager.decompose()
    if all_rows:
        for empty in view.select('.view-empty, .views-empty'):
            empty.decompose()

    _prepare_form(view, page_url)
    view['data-htmlify-exposed-view-version'] = EXPOSED_VIEW_VERSION
    view['data-htmlify-page-size'] = '50'

    content = view.select_one('.view-content')
    if content:
        status = soup.new_tag('p')
        status['class'] = ['htmlify-exposed-view-status']
        status['aria-live'] = 'polite'
        status.string = f'{len(all_rows)} records.'
        content.insert_before(status)
        pagination = soup.new_tag('nav')
        pagination['class'] = ['htmlify-exposed-view-pagination']
        pagination['aria-label'] = 'Result pages'
        content.insert_after(pagination)

    logger.info(
        'Combined %d records from %d pages for exposed View %s on %s',
        len(all_rows),
        len(pages),
        view_identity(view),
        page_url,
    )
    return len(all_rows)


def staticize_exposed_views(soup, page_url, fetch=get_soup):
    """Convert every rendered, non-nested exposed View on a page."""
    found = exposed_views(soup)
    for view in found:
        staticize_exposed_view(soup, view, page_url, fetch=fetch)
    if found and soup.html:
        soup.html['data-htmlify-exposed-view-version'] = EXPOSED_VIEW_VERSION
    return len(found)
