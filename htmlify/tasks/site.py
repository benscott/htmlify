import luigi
import shutil
from bs4 import BeautifulSoup
import requests
import requests_cache
from urllib.parse import urlparse, urlunparse
import os.path
import copy
import hashlib
import re
import yaml
from selenium import webdriver
import time
from pathlib import Path
import os
import typer
import pandas as pd


from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import requests_cache


import mysql.connector


from htmlify import config
from htmlify.config import CRAWL_SITES, SITES_DIR, ASSETS_DIR, TEMPLATE_DIR, DB_PASSWORD, DB_USERNAME, PLATFORMS_ROOT_PATH, APACHE_VHOSTS_DIR, logger, MAX_PAGE_THRESHOLD
from htmlify.tasks.base import BaseTask
from htmlify.tasks.page import PageTask
from htmlify.stack import UniqueStack
from htmlify.utils import get_soup, get_first_directory, get_site_aliases
from htmlify.tasks.sitemap import SiteMapTask
from htmlify.url import URL
from htmlify.db import db_manager
from htmlify.progress import initialise_site_progress


STATIC_ARCHIVE_VERSION = '14'


class SiteTask(BaseTask):

    domain = luigi.Parameter()
    platform_path = luigi.PathParameter()
    page_path = luigi.OptionalParameter(default=None)
    page_prefix = luigi.OptionalParameter(default=None)
    page_limit = luigi.OptionalIntParameter(default=None)
    page_offset = luigi.IntParameter(default=0)

    def requires(self):      
        # The sitemap is assembled from Drupal's database, including content,
        # menu links, and compiled View routes. No recursive site crawl is
        # needed before page rendering.
        return SiteMapTask(domain=self.domain)

    def path_to_url(self, path):
        path = str(path).strip()
        path = f'/{path.lstrip("/")}' if path else ''
        return urlunparse((config.SCHEME, self.domain, path, '', '', ''))

    @staticmethod
    def normalise_page_path(value):
        """Return a sitemap-style path from a path or complete URL."""
        value = str(value).strip()
        parsed = urlparse(value)
        path = parsed.path if parsed.scheme or parsed.netloc else value
        path = path.split('?', 1)[0].split('#', 1)[0]
        return path.strip('/')

    @property
    def is_filtered_run(self):
        return any((
            self.page_path,
            self.page_prefix,
            self.page_limit is not None,
            self.page_offset,
        ))

    def select_links(self, links):
        """Apply deterministic page-level filters to sitemap paths."""
        selected = list(links)

        if self.page_path:
            page_path = self.normalise_page_path(self.page_path)
            selected = [
                link for link in selected
                if self.normalise_page_path(link) == page_path
            ]
        elif self.page_prefix:
            page_prefix = self.normalise_page_path(self.page_prefix)
            selected = [
                link for link in selected
                if (
                    self.normalise_page_path(link) == page_prefix
                    or self.normalise_page_path(link).startswith(
                        f'{page_prefix}/'
                    )
                )
            ]

        if self.page_offset:
            selected = selected[self.page_offset:]
        if self.page_limit is not None:
            selected = selected[:self.page_limit]

        # Every partial archive needs a usable entry point. The homepage is
        # additional to the requested limit, so "100 taxonomy pages" remains
        # 100 taxonomy pages plus the site index.
        if self.is_filtered_run and '' not in selected:
            selected.insert(0, '')

        return selected

    def url_is_valid_filename(self, link):
        url  = URL(link)
        path = url.to_path()
        # Ensure no parts of the URL will be longer than 255
        max_filename_len = max([len(p) for p in path.split('/')])
        if max_filename_len >= 255:
            return False
        
        return True

    def setup(self):

        # Copy accross additional assets
        self.copy_across_assets()

        # Drupal files can either be exposed from a shared local checkout or
        # copied separately (for example, by rsync on the archive server).
        if config.DRUPAL_FILES_MODE == 'symlink':
            self.symlink_drupal_dir()
        elif config.DRUPAL_FILES_MODE != 'external':
            raise ValueError(
                f'Unsupported DRUPAL_FILES_MODE: {config.DRUPAL_FILES_MODE}'
            )

        if config.WRITE_APACHE_VHOSTS:
            self.setup_vhost()
    
    def symlink_drupal_dir(self):

        sites_dir = Path(self.output().path)
        # Scratchped with have file url as /sites/domain/... so we symlink
        # to the appropriate 
        symlink_path = (sites_dir / 'sites')
        symlink_path.parent.mkdir(parents=True, exist_ok=True)

        if not symlink_path.exists():

            platform_sites_path = self.get_platform_sites_path()

            if not platform_sites_path.exists():
                raise FileNotFoundError(f'platform path {platform_sites_path} does not exist. This should be the path to where the drupal source files are mounted. ')

            symlink_path.symlink_to(platform_sites_path)        

    def get_platform_sites_path(self):
        """Resolve Drupal's sites directory locally or through its mount root."""
        platform_path = Path(self.platform_path)
        direct_path = platform_path / 'sites'

        # DDEV supplies a real local app root. Prefer it when it exists rather
        # than rewriting it through the production platform mount setting.
        if direct_path.exists() or not PLATFORMS_ROOT_PATH:
            return direct_path

        relative_path = platform_path.relative_to(
            get_first_directory(platform_path)
        )
        return PLATFORMS_ROOT_PATH / relative_path / 'sites'

    def copy_across_assets(self):
        # Copy shared assets like style.css without deleting page-specific
        # facet indexes. Luigi restarts run() after dynamic dependencies have
        # completed, so removing the whole directory here would delete those
        # indexes and make their PageTasks incomplete again forever.
        sites_dir = Path(self.output().path)
        dest_assets_dir = sites_dir / 'assets'

        dest_assets_dir.mkdir(parents=True, exist_ok=True)
        for child in dest_assets_dir.iterdir():
            if child.name in {'facets', 'taxonomy'}:
                continue
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()

        shutil.copytree(ASSETS_DIR, dest_assets_dir, dirs_exist_ok=True)

    def _alias_target_should_exist(self, target, sitemap_paths):
        target = self.normalise_page_path(target)
        if target not in sitemap_paths:
            return False

        task = PageTask(
            url=self.path_to_url(target),
            output_dir=self.output().path,
        )
        if Path(task.output().path).exists():
            return True
        # A current skip marker means the route was deliberately omitted
        # after being requested (empty content, 404, access denied, etc.).
        if task.skipped_path.exists() and task.complete():
            return False
        return True

    def _alias_mappings(self, sitemap_paths=None):
        """Return unique, safe aliases and whether each contains aliases."""
        if sitemap_paths is not None:
            sitemap_paths = {
                self.normalise_page_path(path)
                for path in sitemap_paths
            }
        mappings = {}
        skipped_targets = 0
        skipped_aliases = set()
        for alias, target in get_site_aliases(self.domain):
            alias = str(alias).strip('/')
            target = str(target).strip('/')
            if (
                not alias
                or not target
                or '..' in Path(alias).parts
                or '..' in Path(target).parts
            ):
                logger.warning(
                    f'Ignoring unsafe site alias {alias!r} -> {target!r}'
                )
                continue
            if (
                sitemap_paths is not None
                and not self._alias_target_should_exist(
                    target,
                    sitemap_paths,
                )
            ):
                skipped_targets += 1
                skipped_aliases.add(alias)
                continue
            mappings[alias] = target

        if skipped_targets:
            logger.info(
                f'Skipping {skipped_targets} aliases whose targets are not '
                f'part of the {self.domain} archive'
            )

        parent_aliases = set()
        for alias in mappings:
            parts = Path(alias).parts
            for length in range(1, len(parts)):
                parent = str(Path(*parts[:length]))
                if parent in mappings:
                    parent_aliases.add(parent)

        return mappings, parent_aliases, skipped_aliases

    @staticmethod
    def _ensure_symlink(link_path, target_path, *, target_is_directory=False):
        if link_path.is_symlink():
            if link_path.resolve(strict=False) == target_path.resolve(strict=False):
                return
            link_path.unlink()
        elif link_path.exists():
            raise FileExistsError(
                f'Cannot create alias {link_path}: a non-symlink already exists'
            )

        link_path.symlink_to(
            target_path,
            target_is_directory=target_is_directory,
        )

    def create_site_aliases(self, sitemap_paths=None):
        sites_dir = Path(self.output().path)
        mappings, parent_aliases, skipped_aliases = self._alias_mappings(
            sitemap_paths,
        )

        # Remove broken aliases produced by an older run, but never remove a
        # real directory that may contain another valid nested alias.
        for alias in skipped_aliases:
            link_path = sites_dir / alias
            if link_path.is_symlink():
                link_path.unlink()
            elif link_path.is_dir():
                index_path = link_path / 'index.html'
                if index_path.is_symlink():
                    index_path.unlink()

        # Parent aliases must be real directories so their child aliases can
        # coexist. Their own route is represented by an index.html symlink.
        for alias in sorted(parent_aliases, key=lambda value: len(Path(value).parts)):
            alias_dir = sites_dir / alias
            if alias_dir.is_symlink():
                alias_dir.unlink()
            alias_dir.mkdir(parents=True, exist_ok=True)
            target_index = sites_dir / mappings[alias] / 'index.html'
            self._ensure_symlink(alias_dir / 'index.html', target_index)

        for alias, target in mappings.items():
            if alias in parent_aliases:
                continue
            link_path = sites_dir / alias
            target_path = sites_dir / target
            link_path.parent.mkdir(parents=True, exist_ok=True)
            self._ensure_symlink(
                link_path,
                target_path,
                target_is_directory=True,
            )

    def create_file_colorbox_aliases(self):
        """Keep legacy popup URLs working via canonical file pages."""
        sites_dir = Path(self.output().path)
        canonical_dir = sites_dir / 'file'
        compatibility_dir = sites_dir / 'file-colorboxed'

        if compatibility_dir.is_symlink() or not compatibility_dir.exists():
            self._ensure_symlink(
                compatibility_dir,
                canonical_dir,
                target_is_directory=True,
            )
            return

        # A partially generated archive may already contain real popup page
        # directories. Preserve those and fill only the missing IDs.
        if compatibility_dir.is_dir() and canonical_dir.is_dir():
            for canonical_file in canonical_dir.iterdir():
                legacy_file = compatibility_dir / canonical_file.name
                if not legacy_file.exists() and not legacy_file.is_symlink():
                    self._ensure_symlink(
                        legacy_file,
                        canonical_file,
                        target_is_directory=True,
                    )

    def file_colorbox_aliases_complete(self):
        sites_dir = Path(self.output().path)
        canonical_dir = sites_dir / 'file'
        compatibility_dir = sites_dir / 'file-colorboxed'
        if compatibility_dir.is_symlink():
            return (
                compatibility_dir.resolve(strict=False)
                == canonical_dir.resolve(strict=False)
            )
        if not compatibility_dir.is_dir():
            return False
        if not canonical_dir.is_dir():
            return True
        return all(
            (compatibility_dir / child.name).exists()
            for child in canonical_dir.iterdir()
        )

    def site_aliases_complete(self, sitemap_paths=None):
        sites_dir = Path(self.output().path)
        mappings, parent_aliases, _skipped_aliases = self._alias_mappings(
            sitemap_paths,
        )

        for alias, target in mappings.items():
            if alias in parent_aliases:
                link_path = sites_dir / alias / 'index.html'
                target_path = sites_dir / target / 'index.html'
            else:
                link_path = sites_dir / alias
                target_path = sites_dir / target

            if not link_path.is_symlink():
                logger.error(f'ALIAS {link_path} does not exist')
                return False
            if link_path.resolve(strict=False) != target_path.resolve(strict=False):
                logger.error(
                    f'ALIAS {link_path} does not point to {target_path}'
                )
                return False
            if not target_path.exists():
                logger.error(f'Target of alias {target_path} does not exist')
                return False

        return True


    def setup_vhost(self):

        sites_dir = Path(self.output().path)

        with (TEMPLATE_DIR / 'vhosts.tpl').open('r') as f:
            content = f.read()    
            content = content.replace('{{DOMAIN}}', self.domain)
            content = content.replace('{{DOCUMENT_ROOT}}', str(sites_dir))

        vhosts_path = APACHE_VHOSTS_DIR / f'{self.domain}.conf'

        with vhosts_path.open('w') as outf:
            outf.write(content)            

    def run(self):
        self.setup()

        with self.input().open() as f:
            all_links = yaml.full_load(f) or []

        links = self.select_links(all_links)
        if self.is_filtered_run:
            logger.info(
                f'Selected {len(links)} of {len(all_links)} sitemap pages '
                f'for {self.domain}'
            )
            if not links:
                raise ValueError(
                    f'No sitemap pages matched the requested page filters '
                    f'for {self.domain}'
                )

        if MAX_PAGE_THRESHOLD and len(links) > MAX_PAGE_THRESHOLD:
            raise Exception(
                f'Links exceed {MAX_PAGE_THRESHOLD} maximum {len(links)}'
            )

        page_tasks = [
            PageTask(
                url=self.path_to_url(link),
                output_dir=self.output().path,
            )
            for link in links
            if self.url_is_valid_filename(self.path_to_url(link))
        ]
        pending_page_tasks = [
            task for task in page_tasks if not task.complete()
        ]
        initialise_site_progress(
            self.domain,
            len(page_tasks),
            completed=len(page_tasks) - len(pending_page_tasks),
        )
        if pending_page_tasks:
            yield pending_page_tasks

        self.create_file_colorbox_aliases()

        # Partial and whole-site runs have distinct completion targets, so a
        # filtered test can satisfy Luigi without marking the site complete.
        if not self.is_filtered_run:
            logger.debug('Creating site aliases')
            self.create_site_aliases(all_links)

        completion_target = self.completion_output()
        Path(completion_target.path).parent.mkdir(parents=True, exist_ok=True)
        with completion_target.open('w') as f:
            f.write(f'{STATIC_ARCHIVE_VERSION}\n')

    def output(self):
        return luigi.LocalTarget(SITES_DIR / self.domain)

    def completion_output(self):
        if self.is_filtered_run:
            selection = '|'.join((
                str(self.page_path or ''),
                str(self.page_prefix or ''),
                str(self.page_limit or ''),
                str(self.page_offset),
            ))
            selection_hash = hashlib.sha256(
                selection.encode('utf-8')
            ).hexdigest()[:16]
            return luigi.LocalTarget(
                config.PROCESSING_DATA_DIR
                / 'sites'
                / 'partial'
                / self.domain
                / f'{selection_hash}.complete'
            )

        return luigi.LocalTarget(
            config.PROCESSING_DATA_DIR / 'sites' / f'{self.domain}.complete'
        )

    def complete(self):
        if self.force:
            return False

        completion_path = Path(self.completion_output().path)
        sitemap_path = Path(SiteMapTask(domain=self.domain).output().path)
        if not completion_path.exists() or not sitemap_path.exists():
            return False
        if completion_path.read_text(encoding='utf-8').strip() != STATIC_ARCHIVE_VERSION:
            return False
        if completion_path.stat().st_mtime < sitemap_path.stat().st_mtime:
            return False

        sites_dir = Path(self.output().path)
        if not sites_dir.exists():
            return False

        if self.is_filtered_run:
            with SiteMapTask(domain=self.domain).output().open() as sitemap:
                links = self.select_links(yaml.full_load(sitemap) or [])
            return all(
                PageTask(
                    url=self.path_to_url(link),
                    output_dir=self.output().path,
                ).complete()
                for link in links
                if self.url_is_valid_filename(self.path_to_url(link))
            )

        if not self.file_colorbox_aliases_complete():
            return False

        with SiteMapTask(domain=self.domain).output().open() as sitemap:
            sitemap_paths = yaml.full_load(sitemap) or []
        if not self.site_aliases_complete(sitemap_paths):
            return False
            
        db_manager.close_connection(self.domain)
            
        return True
    


if __name__ == "__main__":    

    # sites_list_task = SitesListTask()
    domain = 'dipteratyoryhma.myspecies.info'
    domain = 'tabanidae.myspecies.info'
    luigi.build([
        SiteTask(
            domain=domain, 
            platform_path='/var/aegir/platforms/scratchpads-2.10.1',
            force=True
        )], local_scheduler=True)
