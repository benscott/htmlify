import json
import tempfile
import unittest
from pathlib import Path

from htmlify.validation import validate_copy, validate_site


class TestCopyValidation(unittest.TestCase):

    def test_matching_trees_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            destination = root / 'destination'
            source.mkdir()
            destination.mkdir()
            (source / 'image.jpg').write_bytes(b'image')
            (destination / 'image.jpg').write_bytes(b'image')

            result = validate_copy(source, destination, checksums=True)

            self.assertTrue(result.ok)
            self.assertEqual(result.checked_files, 1)

    def test_missing_changed_and_extra_files_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            destination = root / 'destination'
            source.mkdir()
            destination.mkdir()
            (source / 'missing.txt').write_text('missing', encoding='utf-8')
            (source / 'changed.txt').write_text('source', encoding='utf-8')
            (destination / 'changed.txt').write_text('longer destination', encoding='utf-8')
            (destination / 'extra.txt').write_text('extra', encoding='utf-8')

            result = validate_copy(source, destination)

            self.assertFalse(result.ok)
            self.assertEqual(result.missing, ['missing.txt'])
            self.assertEqual(result.changed, ['changed.txt'])
            self.assertEqual(result.extra, ['extra.txt'])

    def test_checksums_detect_same_size_content_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            destination = root / 'destination'
            source.mkdir()
            destination.mkdir()
            (source / 'same-size.txt').write_text('abc', encoding='utf-8')
            (destination / 'same-size.txt').write_text('xyz', encoding='utf-8')

            size_only = validate_copy(source, destination)
            with_checksums = validate_copy(source, destination, checksums=True)

            self.assertTrue(size_only.ok)
            self.assertEqual(with_checksums.changed, ['same-size.txt'])


class TestSiteValidation(unittest.TestCase):

    def test_reports_missing_pages_by_prefix_files_and_all_referrers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'index.html').write_text(
                '''
                <html data-htmlify-domain="example.org"><head>
                  <link rel="stylesheet" href="/assets/site.css">
                  <link rel="canonical" href="https://example.org/">
                  <link rel="alternate" href="/rss.xml" type="application/rss+xml">
                </head><body>
                  <a href="/node/1">Present</a>
                  <a href="/node/404?view=full#details">Missing node</a>
                  <a href="https://outside.example/missing">External</a>
                  <img src="/sites/example/files/missing.jpg">
                </body></html>
                ''',
                encoding='utf-8',
            )
            (root / 'node' / '1').mkdir(parents=True)
            (root / 'node' / '1' / 'index.html').write_text(
                '<a href="/">Home</a>',
                encoding='utf-8',
            )
            (root / 'biblio').mkdir()
            (root / 'biblio' / 'index.html').write_text(
                '''
                <a href="/node/404">Same missing node</a>
                <section data-index-url="/assets/facets/biblio.json"></section>
                ''',
                encoding='utf-8',
            )
            (root / 'assets').mkdir()
            (root / 'assets' / 'site.css').write_text(
                '.hero { background: url("missing-background.png"); }',
                encoding='utf-8',
            )
            (root / 'assets' / 'facets').mkdir()
            (root / 'assets' / 'facets' / 'biblio.json').write_text(
                json.dumps({
                    'records': [{
                        'url': '/taxonomy/term/8',
                        'thumbnail': '/sites/example/files/thumb.jpg',
                    }],
                }),
                encoding='utf-8',
            )

            result = validate_site(root)

            self.assertFalse(result.ok)
            self.assertEqual(result.domain, 'example.org')
            self.assertEqual(result.checked_html_files, 3)
            self.assertEqual(result.checked_css_files, 1)
            self.assertEqual(result.checked_facet_indexes, 1)
            self.assertEqual(result.skipped_external_references, 1)
            self.assertEqual(result.missing_pages_by_prefix, {
                'node': 1,
                'taxonomy': 1,
            })
            node = next(
                issue for issue in result.missing_pages
                if issue['target'] == '/node/404'
            )
            self.assertEqual(node['references'], 2)
            self.assertEqual(node['referrers'], ['/', '/biblio/'])
            self.assertEqual(node['pages'], ['/', '/biblio/'])
            background = next(
                issue for issue in result.missing_files
                if issue['target'] == '/assets/missing-background.png'
            )
            self.assertEqual(background['referrers'], ['/assets/site.css'])
            self.assertEqual(background['pages'], ['/'])
            taxonomy = next(
                issue for issue in result.missing_pages
                if issue['target'] == '/taxonomy/term/8'
            )
            self.assertEqual(
                taxonomy['referrers'],
                ['/assets/facets/biblio.json'],
            )
            self.assertEqual(taxonomy['pages'], ['/biblio/'])
            self.assertEqual(
                {issue['target'] for issue in result.missing_files},
                {
                    '/assets/missing-background.png',
                    '/rss.xml',
                    '/sites/example/files/missing.jpg',
                    '/sites/example/files/thumb.jpg',
                },
            )

    def test_relative_links_resolve_from_the_referring_page(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'guide').mkdir()
            (root / 'guide' / 'index.html').write_text(
                '<a href="chapter">Chapter</a><img src="../image.png">',
                encoding='utf-8',
            )
            (root / 'guide' / 'chapter').mkdir()
            (root / 'guide' / 'chapter' / 'index.html').write_text(
                '<p>Present</p>',
                encoding='utf-8',
            )
            (root / 'image.png').write_bytes(b'image')

            result = validate_site(root, domain='example.org')

            self.assertTrue(result.ok)

    def test_broken_symlinks_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'index.html').write_text('<p>Home</p>', encoding='utf-8')
            (root / 'missing-alias').symlink_to(root / 'does-not-exist')

            result = validate_site(root, domain='example.org')

            self.assertFalse(result.ok)
            self.assertEqual(result.broken_symlinks[0]['path'], '/missing-alias')
