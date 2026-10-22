import unittest

from bs4 import BeautifulSoup

from htmlify.exposed_views import (
    EXPOSED_VIEW_VERSION,
    collect_view_pages,
    exposed_views,
    staticize_exposed_views,
)


def view_page(rows, pager=''):
    return BeautifulSoup(
        f'''
        <html><body>
          <div class="view view-people view-id-people view-display-id-page">
            <div class="view-filters">
              <form action="/people" method="get">
                <div class="views-exposed-form">
                  <div class="views-exposed-widgets">
                    <div class="views-exposed-widget views-widget-filter-name">
                      <label for="edit-name">Name</label>
                      <input id="edit-name" name="name" type="text">
                    </div>
                    <div class="views-exposed-widget views-widget-filter-country">
                      <label for="edit-country">Country</label>
                      <select id="edit-country" name="country">
                        <option value="All">- Any -</option>
                        <option value="GB">United Kingdom</option>
                      </select>
                    </div>
                    <div class="views-exposed-widget views-submit-button">
                      <input type="submit" value="Apply">
                    </div>
                  </div>
                </div>
              </form>
            </div>
            <div class="view-content"><ul>{rows}</ul></div>
            {pager}
          </div>
        </body></html>
        ''',
        'html.parser',
    )


class TestExposedViews(unittest.TestCase):

    def test_detects_rendered_exposed_view(self):
        soup = view_page('<li class="views-row">One</li>')

        self.assertEqual(len(exposed_views(soup)), 1)

    def test_collects_every_pager_page_once(self):
        first = view_page(
            '<li class="views-row">First</li>',
            '<ul class="pager"><li><a href="/people/page/1">2</a></li>'
            '<li><a href="/people/page/2">3</a></li></ul>',
        )
        second = view_page(
            '<li class="views-row">Second</li>',
            '<ul class="pager"><li><a href="/people">1</a></li>'
            '<li><a href="/people/page/2">3</a></li></ul>',
        )
        third = view_page(
            '<li class="views-row">Third</li>',
            '<ul class="pager"><li><a href="/people/page/1">2</a></li></ul>',
        )
        pages = {
            'https://example.org/people/page/1': second,
            'https://example.org/people/page/2': third,
        }
        fetched = []

        def fetch(url):
            fetched.append(url)
            return pages[url]

        collected = collect_view_pages(
            'https://example.org/people',
            exposed_views(first)[0],
            fetch=fetch,
        )

        self.assertEqual(len(collected), 3)
        self.assertEqual(fetched, list(pages))

    def test_merges_results_and_prepares_static_form(self):
        first = view_page(
            '<li class="views-row views-row-1 views-row-odd">Alice</li>',
            '<ul class="pager"><li><a href="/people/page/1">2</a></li>'
            '</ul>',
        )
        second = view_page(
            '<li class="views-row views-row-1 views-row-odd">Bob</li>'
        )

        count = staticize_exposed_views(
            first,
            'https://example.org/people',
            fetch=lambda _url: second,
        )

        self.assertEqual(count, 1)
        self.assertEqual(
            [row.get_text(strip=True) for row in first.select(
                '.htmlify-exposed-view-row'
            )],
            ['Alice', 'Bob'],
        )
        self.assertIsNone(first.select_one('.pager'))
        self.assertEqual(
            first.html['data-htmlify-exposed-view-version'],
            EXPOSED_VIEW_VERSION,
        )
        form = first.select_one('.htmlify-exposed-view-search')
        self.assertEqual(form['action'], '/people')
        self.assertIsNotNone(form.select_one('.htmlify-exposed-view-clear'))
        self.assertEqual(
            first.select_one('.htmlify-exposed-view-status').get_text(
                ' ', strip=True
            ),
            '2 records.',
        )
        content = first.select_one('.view-content')
        self.assertIs(
            first.select_one('.htmlify-exposed-view-status').parent,
            content,
        )
        self.assertIs(
            first.select_one('.htmlify-exposed-view-pagination').parent,
            content,
        )


if __name__ == '__main__':
    unittest.main()
