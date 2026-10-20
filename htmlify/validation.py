from dataclasses import asdict, dataclass
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import posixpath
import re
from urllib.parse import unquote, urljoin, urlsplit


@dataclass(frozen=True)
class FileInfo:
    kind: str
    size: int | None = None
    link_target: str | None = None


@dataclass
class CopyValidationResult:
    source: str
    destination: str
    missing: list[str]
    changed: list[str]
    extra: list[str]
    checked_files: int
    checksums: bool

    @property
    def ok(self):
        return not self.missing and not self.changed and not self.extra

    def as_dict(self):
        result = asdict(self)
        result['ok'] = self.ok
        return result

    def write_json(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), indent=2, sort_keys=True) + '\n',
            encoding='utf-8',
        )


@dataclass
class SiteValidationResult:
    root: str
    domain: str | None
    checked_html_files: int
    checked_css_files: int
    checked_facet_indexes: int
    checked_internal_references: int
    skipped_external_references: int
    missing_pages: list[dict]
    missing_files: list[dict]
    broken_symlinks: list[dict]
    errors: list[dict]

    @property
    def ok(self):
        return not any((
            self.missing_pages,
            self.missing_files,
            self.broken_symlinks,
            self.errors,
        ))

    @property
    def missing_pages_by_prefix(self):
        counts = {}
        for issue in self.missing_pages:
            prefix = issue['prefix']
            counts[prefix] = counts.get(prefix, 0) + 1
        return dict(sorted(counts.items()))

    def as_dict(self):
        return {
            'root': self.root,
            'domain': self.domain,
            'summary': {
                'ok': self.ok,
                'checked_html_files': self.checked_html_files,
                'checked_css_files': self.checked_css_files,
                'checked_facet_indexes': self.checked_facet_indexes,
                'checked_internal_references': self.checked_internal_references,
                'skipped_external_references': self.skipped_external_references,
                'missing_pages': len(self.missing_pages),
                'missing_pages_by_prefix': self.missing_pages_by_prefix,
                'missing_files': len(self.missing_files),
                'broken_symlinks': len(self.broken_symlinks),
                'errors': len(self.errors),
            },
            'missing_pages': self.missing_pages,
            'missing_files': self.missing_files,
            'broken_symlinks': self.broken_symlinks,
            'errors': self.errors,
        }

    def write_json(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), indent=2, sort_keys=True) + '\n',
            encoding='utf-8',
        )


CSS_URL = re.compile(
    r'''url\(\s*(["']?)(.*?)\1\s*\)|@import\s+(["'])(.*?)\3''',
    re.IGNORECASE,
)
REFRESH_URL = re.compile(r'\burl\s*=\s*([^;]+)', re.IGNORECASE)
FILE_HREF_EXTENSIONS = {
    '.7z', '.avi', '.avif', '.bib', '.csv', '.doc', '.docx', '.eps', '.gif',
    '.gz', '.ico', '.jpeg', '.jpg', '.json', '.mov', '.mp3', '.mp4', '.mpeg',
    '.ods', '.odt', '.pdf', '.png', '.ppt', '.pptx', '.rar', '.rdf', '.rtf',
    '.svg', '.tar', '.tif', '.tiff', '.tsv', '.txt', '.wav', '.webm', '.webp',
    '.xls', '.xlsx', '.xml', '.zip',
}


class _StaticHTMLParser(HTMLParser):
    """Collect URL-bearing attributes without constructing an HTML tree."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.references = []
        self.base_href = None
        self._style_depth = 0

    def handle_starttag(self, tag, attrs):
        self._handle_tag(tag, attrs)

    def handle_startendtag(self, tag, attrs):
        self._handle_tag(tag, attrs)

    def handle_endtag(self, tag):
        if tag.lower() == 'style' and self._style_depth:
            self._style_depth -= 1

    def handle_data(self, data):
        if self._style_depth:
            self.references.extend(
                (url, 'file') for url in _css_urls(data)
            )

    def _handle_tag(self, tag, attrs):
        tag = tag.lower()
        values = {str(name).lower(): value for name, value in attrs}
        if tag == 'style':
            self._style_depth += 1
        if tag == 'base' and values.get('href') and self.base_href is None:
            self.base_href = values['href']

        href = values.get('href')
        if href and tag != 'base':
            kind = 'page'
            if tag == 'link':
                rel = str(values.get('rel') or '').lower().split()
                asset_relations = {
                    'icon', 'manifest', 'modulepreload', 'preload', 'stylesheet',
                }
                kind = (
                    'file'
                    if _href_is_file(href) or asset_relations & set(rel)
                    else 'page'
                )
            elif tag == 'use' or _href_is_file(href):
                kind = 'file'
            self.references.append((href, kind))

        if action := values.get('action'):
            self.references.append((action, 'page'))

        for attribute in ('src', 'poster', 'data-index-url'):
            if value := values.get(attribute):
                kind = 'page' if tag in {'frame', 'iframe'} and attribute == 'src' else 'file'
                self.references.append((value, kind))

        if tag == 'object' and values.get('data'):
            self.references.append((values['data'], 'file'))

        for attribute in ('srcset', 'imagesrcset'):
            if value := values.get(attribute):
                self.references.extend(
                    (candidate, 'file') for candidate in _srcset_urls(value)
                )

        if style := values.get('style'):
            self.references.extend((url, 'file') for url in _css_urls(style))

        if (
            tag == 'meta'
            and str(values.get('http-equiv') or '').lower() == 'refresh'
            and values.get('content')
        ):
            if match := REFRESH_URL.search(values['content']):
                self.references.append((match.group(1).strip(' \"\''), 'page'))


def _css_urls(css):
    for match in CSS_URL.finditer(css or ''):
        value = match.group(2) or match.group(4)
        if value:
            yield value.strip()


def _srcset_urls(srcset):
    # Archived Scratchpads srcsets contain ordinary URLs rather than data URLs.
    for candidate in str(srcset).split(','):
        url = candidate.strip().split(maxsplit=1)[0]
        if url:
            yield url


def _href_is_file(href):
    path = urlsplit(str(href)).path.lower()
    return Path(path).suffix in FILE_HREF_EXTENSIONS


def _source_url(relative_path):
    relative_path = Path(relative_path).as_posix()
    if relative_path == 'index.html':
        return '/'
    if relative_path.endswith('/index.html'):
        return '/' + relative_path[:-len('index.html')]
    return '/' + relative_path


def _path_prefix(path):
    parts = [part for part in str(path).split('/') if part]
    return parts[0] if parts else '/'


def _normalise_reference(raw_url, source_url, domain, base_href=None):
    raw_url = str(raw_url or '').strip()
    if not raw_url or raw_url.startswith(('#', '?')):
        return None, 'local'
    if raw_url.lower().startswith(('data:', 'javascript:', 'mailto:', 'tel:')):
        return None, 'external'

    base_url = f'https://{domain or "archive.invalid"}{source_url}'
    if base_href:
        base_url = urljoin(base_url, base_href)
    absolute = urlsplit(urljoin(base_url, raw_url))
    if absolute.scheme not in {'http', 'https'}:
        return None, 'external'
    if domain and absolute.hostname and absolute.hostname.lower() != domain.lower():
        return None, 'external'
    if not domain and absolute.hostname != 'archive.invalid':
        return None, 'external'

    try:
        decoded = unquote(absolute.path or '/')
    except (UnicodeDecodeError, ValueError):
        return None, 'invalid'
    normalised = posixpath.normpath('/' + decoded.lstrip('/'))
    if '\x00' in normalised:
        return None, 'invalid'
    return normalised, 'internal'


def _target_exists(root, path, kind):
    relative = path.lstrip('/')
    target = root / relative
    if kind == 'file':
        return target.is_file()
    if target.is_file():
        return True
    return (target / 'index.html').is_file()


def _referring_pages(referrers, html_sources, asset_consumers):
    pages = set()
    pending = list(referrers)
    seen = set()
    while pending:
        source = pending.pop()
        if source in seen:
            continue
        seen.add(source)
        if source in html_sources:
            pages.add(source)
        pending.extend(asset_consumers.get(source, ()))
    return sorted(pages)


def _issue_list(
    issues,
    html_sources,
    asset_consumers,
    include_prefix=False,
):
    output = []
    for target, details in sorted(issues.items()):
        issue = {
            'target': target,
            'references': details['references'],
            'referrers': sorted(details['referrers']),
            'pages': _referring_pages(
                details['referrers'],
                html_sources,
                asset_consumers,
            ),
        }
        if include_prefix:
            issue['prefix'] = _path_prefix(target)
        output.append(issue)
    return output


def _add_issue(issues, target, referrer):
    issue = issues.setdefault(target, {'references': 0, 'referrers': set()})
    issue['references'] += 1
    issue['referrers'].add(referrer)


def _site_domain(root, requested_domain=None):
    if requested_domain:
        return requested_domain.lower()
    index = root / 'index.html'
    if not index.is_file():
        return None
    try:
        head = index.read_text(encoding='utf-8', errors='replace')[:8192]
    except OSError:
        return None
    match = re.search(r'\bdata-htmlify-domain=["\']([^"\']+)', head, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _inventory(root):
    root = Path(root)
    inventory = {}
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory = Path(directory)

        # os.walk does not descend into directory symlinks, so record them as
        # entries while ignoring ordinary directories.
        for name in list(dirnames):
            path = directory / name
            if path.is_symlink():
                relative = path.relative_to(root).as_posix()
                inventory[relative] = FileInfo('symlink', link_target=os.readlink(path))
                dirnames.remove(name)

        for name in filenames:
            path = directory / name
            relative = path.relative_to(root).as_posix()
            if path.is_symlink():
                inventory[relative] = FileInfo('symlink', link_target=os.readlink(path))
            else:
                inventory[relative] = FileInfo('file', size=path.stat().st_size)

    return inventory


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_copy(source, destination, checksums=False):
    """Compare a copied tree by relative path, type, size and optional hash."""
    source = Path(source).expanduser().resolve()
    destination = Path(destination).expanduser().resolve()
    if not source.is_dir():
        raise FileNotFoundError(f'Source directory does not exist: {source}')
    if not destination.is_dir():
        raise FileNotFoundError(f'Destination directory does not exist: {destination}')

    source_files = _inventory(source)
    destination_files = _inventory(destination)
    missing = sorted(set(source_files) - set(destination_files))
    extra = sorted(set(destination_files) - set(source_files))
    changed = []

    for relative in sorted(set(source_files) & set(destination_files)):
        source_info = source_files[relative]
        destination_info = destination_files[relative]
        if source_info != destination_info:
            changed.append(relative)
            continue
        if checksums and source_info.kind == 'file':
            if _sha256(source / relative) != _sha256(destination / relative):
                changed.append(relative)

    return CopyValidationResult(
        source=str(source),
        destination=str(destination),
        missing=missing,
        changed=changed,
        extra=extra,
        checked_files=len(source_files),
        checksums=checksums,
    )


def validate_site(root, domain=None):
    """Check that internal page and asset references resolve in an archive."""
    root = Path(root).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f'Site directory does not exist: {root}')

    domain = _site_domain(root, domain)
    html_files = []
    css_files = []
    facet_indexes = []
    broken_symlinks = []
    errors = []

    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory = Path(directory)
        for name in list(dirnames):
            path = directory / name
            if path.is_symlink():
                if not path.exists():
                    broken_symlinks.append({
                        'path': '/' + path.relative_to(root).as_posix(),
                        'target': os.readlink(path),
                    })
                dirnames.remove(name)

        for name in filenames:
            path = directory / name
            if path.is_symlink() and not path.exists():
                broken_symlinks.append({
                    'path': '/' + path.relative_to(root).as_posix(),
                    'target': os.readlink(path),
                })
                continue
            suffix = path.suffix.lower()
            if suffix in {'.html', '.htm'}:
                html_files.append(path)
            elif suffix == '.css':
                css_files.append(path)
            elif (
                suffix == '.json'
                and path.parent.name == 'facets'
                and path.parent.parent.name == 'assets'
            ):
                facet_indexes.append(path)

    missing_pages = {}
    missing_files = {}
    existence_cache = {}
    asset_consumers = {}
    html_sources = {
        _source_url(path.relative_to(root)) for path in html_files
    }
    checked_internal_references = 0
    skipped_external_references = 0

    def check_references(references, source, source_url, base_href=None):
        nonlocal checked_internal_references, skipped_external_references
        for raw_url, kind in references:
            target, status = _normalise_reference(
                raw_url,
                source_url,
                domain,
                base_href,
            )
            if status == 'external':
                skipped_external_references += 1
                continue
            if status == 'local' or target is None:
                if status == 'invalid':
                    errors.append({
                        'source': source,
                        'reference': str(raw_url),
                        'error': 'Invalid URL',
                    })
                continue

            checked_internal_references += 1
            if kind == 'file':
                asset_consumers.setdefault(target, set()).add(source)
            cache_key = (kind, target)
            if cache_key not in existence_cache:
                existence_cache[cache_key] = _target_exists(root, target, kind)
            if existence_cache[cache_key]:
                continue
            issues = missing_pages if kind == 'page' else missing_files
            _add_issue(issues, target, source)

    for path in sorted(html_files):
        relative = path.relative_to(root)
        source = _source_url(relative)
        try:
            parser = _StaticHTMLParser()
            parser.feed(path.read_text(encoding='utf-8', errors='replace'))
            parser.close()
            check_references(
                parser.references,
                source,
                source,
                parser.base_href,
            )
        except (OSError, ValueError) as error:
            errors.append({'source': source, 'error': str(error)})

    for path in sorted(css_files):
        source = _source_url(path.relative_to(root))
        try:
            references = [
                (url, 'file')
                for url in _css_urls(
                    path.read_text(encoding='utf-8', errors='replace')
                )
            ]
            check_references(references, source, source)
        except OSError as error:
            errors.append({'source': source, 'error': str(error)})

    for path in sorted(facet_indexes):
        source = _source_url(path.relative_to(root))
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
            references = []
            for record in payload.get('records', []):
                if record.get('url'):
                    references.append((record['url'], 'page'))
                if record.get('thumbnail'):
                    references.append((record['thumbnail'], 'file'))
            check_references(references, source, source)
        except (OSError, json.JSONDecodeError, AttributeError) as error:
            errors.append({'source': source, 'error': str(error)})

    return SiteValidationResult(
        root=str(root),
        domain=domain,
        checked_html_files=len(html_files),
        checked_css_files=len(css_files),
        checked_facet_indexes=len(facet_indexes),
        checked_internal_references=checked_internal_references,
        skipped_external_references=skipped_external_references,
        missing_pages=_issue_list(
            missing_pages,
            html_sources,
            asset_consumers,
            include_prefix=True,
        ),
        missing_files=_issue_list(
            missing_files,
            html_sources,
            asset_consumers,
        ),
        broken_symlinks=sorted(broken_symlinks, key=lambda item: item['path']),
        errors=sorted(errors, key=lambda item: item['source']),
    )
