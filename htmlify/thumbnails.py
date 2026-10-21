import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import unquote, urlparse

from PIL import Image, ImageOps

from htmlify.config import logger


THUMBNAIL_SIZE = (85, 85)
STYLE_SEGMENTS = ('styles', 'square_thumbnail')


def _path_within(root, url):
    parsed = urlparse(str(url))
    if parsed.scheme or parsed.netloc or not parsed.path.startswith('/'):
        return None
    root = Path(root).resolve()
    path = (root / unquote(parsed.path).lstrip('/')).resolve()
    return path if path.is_relative_to(root) else None


def _source_candidates(thumbnail):
    """Return copied originals/derivatives that can seed one thumbnail."""
    parts = thumbnail.parts
    for position in range(len(parts) - 1):
        if parts[position:position + 2] != STYLE_SEGMENTS:
            continue
        files_root = Path(*parts[:position])
        style_tail = parts[position + 2:]
        candidates = [files_root / 'styles' / 'large' / Path(*style_tail)]
        if style_tail and style_tail[0] == 'public':
            candidates.insert(0, files_root / Path(*style_tail[1:]))
        return candidates
    return []


def _write_square_thumbnail(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.parent.chmod(0o755)
    temporary_path = None
    try:
        with Image.open(source) as original:
            image_format = original.format
            image = ImageOps.exif_transpose(original)
            image.seek(0)
            image = ImageOps.fit(
                image,
                THUMBNAIL_SIZE,
                method=Image.Resampling.LANCZOS,
            )
            if image_format == 'JPEG' and image.mode not in ('L', 'RGB'):
                image = image.convert('RGB')
            with NamedTemporaryFile(
                mode='w+b',
                dir=destination.parent,
                prefix=f'.{destination.name}.',
                delete=False,
            ) as temporary:
                image.save(temporary, format=image_format)
                temporary_path = Path(temporary.name)
        temporary_path.chmod(0o644)
        temporary_path.replace(destination)
    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()


def gallery_thumbnail_paths(site_root):
    """Yield unique local thumbnail paths referenced by facet indexes."""
    site_root = Path(site_root)
    seen = set()
    facet_dir = site_root / 'assets' / 'facets'
    if not facet_dir.is_dir():
        return
    for index_path in facet_dir.glob('*.json'):
        try:
            payload = json.loads(index_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as error:
            logger.warning(f'Could not read facet index {index_path}: {error}')
            continue
        if payload.get('page', {}).get('display') != 'thumbnails':
            continue
        for record in payload.get('records', []):
            thumbnail = record.get('thumbnail')
            path = _path_within(site_root, thumbnail) if thumbnail else None
            if path and path not in seen:
                seen.add(path)
                yield path


def create_missing_gallery_thumbnails(site_root):
    """Create missing square thumbnails from already-copied image files."""
    created = 0
    missing_sources = []
    failed = []
    for destination in gallery_thumbnail_paths(site_root):
        if destination.is_file():
            continue
        source = next(
            (candidate for candidate in _source_candidates(destination)
             if candidate.is_file()),
            None,
        )
        if source is None:
            missing_sources.append(destination)
            continue
        try:
            _write_square_thumbnail(source, destination)
            created += 1
        except Exception as error:
            failed.append((destination, str(error)))
            logger.warning(
                f'Could not create gallery thumbnail {destination}: {error}'
            )
    return {
        'created': created,
        'missing_sources': missing_sources,
        'failed': failed,
    }


def gallery_thumbnails_complete(site_root):
    return all(path.is_file() for path in gallery_thumbnail_paths(site_root))
