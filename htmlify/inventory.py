from dataclasses import asdict, dataclass, field
import json
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SiteInventoryEntry:
    domain: str
    sitemap_urls: int | None
    status: str
    has_run: bool

    def as_dict(self):
        return asdict(self)


@dataclass
class SitemapInventory:
    sites: list[SiteInventoryEntry]
    failures: list[dict] = field(default_factory=list)

    @property
    def summary(self):
        statuses = {}
        for site in self.sites:
            statuses[site.status] = statuses.get(site.status, 0) + 1
        return {
            'sites': len(self.sites),
            'sitemaps': sum(site.sitemap_urls is not None for site in self.sites),
            'urls': sum(site.sitemap_urls or 0 for site in self.sites),
            'statuses': dict(sorted(statuses.items())),
        }

    def as_dict(self):
        return {
            'summary': self.summary,
            'sites': [site.as_dict() for site in self.sites],
            'failures': self.failures,
        }

    def write_json(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.as_dict(), indent=2, sort_keys=True) + '\n',
            encoding='utf-8',
        )


def sitemap_url_count(path):
    path = Path(path)
    if not path.is_file():
        return None
    with path.open(encoding='utf-8') as sitemap:
        urls = yaml.safe_load(sitemap) or []
    if not isinstance(urls, list):
        raise ValueError(f'Sitemap is not a URL list: {path}')
    return len(urls)


def site_run_status(
    domain,
    sitemap_path,
    sites_dir,
    processing_dir,
    archive_version,
):
    sitemap_path = Path(sitemap_path)
    site_root = Path(sites_dir) / domain
    complete_marker = Path(processing_dir) / 'sites' / f'{domain}.complete'
    partial_markers = Path(processing_dir) / 'sites' / 'partial' / domain

    if complete_marker.is_file():
        try:
            version = complete_marker.read_text(
                encoding='utf-8', errors='replace',
            ).strip()
        except OSError:
            version = None
        current = (
            version == str(archive_version)
            and site_root.joinpath('index.html').is_file()
            and sitemap_path.is_file()
            and complete_marker.stat().st_mtime >= sitemap_path.stat().st_mtime
        )
        return 'complete' if current else 'stale'

    has_partial_marker = (
        partial_markers.is_dir()
        and any(partial_markers.glob('*.complete'))
    )
    if has_partial_marker or site_root.joinpath('index.html').is_file():
        return 'partial'
    return 'not run'


def build_sitemap_inventory(
    domains,
    sitemap_dir,
    sites_dir,
    processing_dir,
    archive_version,
):
    entries = []
    for domain in sorted(set(str(domain) for domain in domains)):
        sitemap_path = Path(sitemap_dir) / f'{domain}.yaml'
        status = site_run_status(
            domain,
            sitemap_path,
            sites_dir,
            processing_dir,
            archive_version,
        )
        entries.append(SiteInventoryEntry(
            domain=domain,
            sitemap_urls=sitemap_url_count(sitemap_path),
            status=status,
            has_run=status != 'not run',
        ))
    return SitemapInventory(entries)
