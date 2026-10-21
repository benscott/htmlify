import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from htmlify.thumbnails import (
    create_missing_gallery_thumbnails,
    gallery_thumbnails_complete,
)


class TestGalleryThumbnails(unittest.TestCase):

    def test_creates_missing_remote_thumbnail_from_large_derivative(self):
        with tempfile.TemporaryDirectory() as directory:
            site_root = Path(directory)
            source = (
                site_root / 'sites' / 'example.org' / 'files' / 'styles'
                / 'large' / 'http' / 'images.example.org' / 'plant.jpg'
            )
            source.parent.mkdir(parents=True)
            Image.new('RGB', (400, 200), '#2f7d32').save(source, 'JPEG')

            thumbnail_url = (
                '/sites/example.org/files/styles/square_thumbnail/'
                'http/images.example.org/plant.jpg'
            )
            index = site_root / 'assets' / 'facets' / 'gallery.json'
            index.parent.mkdir(parents=True)
            index.write_text(json.dumps({
                'page': {'display': 'thumbnails'},
                'records': [{'thumbnail': thumbnail_url}],
            }), encoding='utf-8')

            self.assertFalse(gallery_thumbnails_complete(site_root))
            result = create_missing_gallery_thumbnails(site_root)

            destination = site_root / thumbnail_url.lstrip('/')
            self.assertEqual(result['created'], 1)
            self.assertTrue(gallery_thumbnails_complete(site_root))
            with Image.open(destination) as thumbnail:
                self.assertEqual(thumbnail.size, (85, 85))
            self.assertEqual(destination.stat().st_mode & 0o777, 0o644)


if __name__ == '__main__':
    unittest.main()
