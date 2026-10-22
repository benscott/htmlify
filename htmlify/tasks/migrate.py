import luigi
import mysql.connector
import phpserialize
import itertools
import yaml
import pandas as pd
import requests
from urllib.parse import urlparse


from htmlify.config import DATA_DIR, logger
from htmlify import config
from htmlify.inventory import sitemap_url_count
from htmlify.tasks.base import BaseExternalTask, BaseTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.tasks.site import SiteTask
from htmlify.utils import is_under_maintenance
       
class MigrateTask(BaseTask):

    domain = luigi.Parameter(default=None) 
    limit = luigi.IntParameter(default=None) 
    offset = luigi.IntParameter(default=None) 
    platform_path = luigi.Parameter(default=None, significant=False)
    page_path = luigi.OptionalParameter(default=None)
    page_prefix = luigi.OptionalParameter(default=None)
    page_limit = luigi.OptionalIntParameter(default=None)
    page_offset = luigi.IntParameter(default=0)
    sort_by = luigi.Parameter(default='name')
    sort_order = luigi.Parameter(default='asc')

    tranche_1_domains = [
        'alkcarb.myspecies.info',
        'fungi.myspecies.info',
        'milichiidae.myspecies.info',
        'mosquito-taxonomic-inventory.myspecies.info',
        'myriatrix.myspecies.info',
        'phthiraptera.myspecies.info',
        'sciaroidea.myspecies.info',
        'solanaceaesource.myspecies.info',
        'sphingidae.myspecies.info',  
        'wallacefund.myspecies.info',
        'wallaceletters.myspecies.info',
        # Sites to ignore
        'acoela.myspecies.info',
        'agromyzidae.myspecies.info',      
    ]
    
    def requires(self):

        sites_list_task = SitesListTask()
        luigi.build([sites_list_task], local_scheduler=True)
        # We don;t have to yield this - but keeps the dependency graph accurate
        yield sites_list_task

        df = pd.read_csv(sites_list_task.output().path)

        if self.domain:
            df = df[df.resolved_domain == self.domain]
            if df.empty:
                raise Exception(f'No domain matching {self.domain}')
            logger.debug(f'Migrating site {self.domain}')
        else:
            # Sort and slice only sites that can actually be scheduled. This
            # keeps --limit accurate and avoids requiring counts for the
            # deliberately excluded tranche.
            df = df[df.platform_path.notnull()]
            df = df[~df.resolved_domain.isin(self.tranche_1_domains)]
            df = self.order_sites(df)

        if self.offset:
            df = df.iloc[self.offset:]         

        if self.limit:
            df = df.iloc[:self.limit]
        
        logger.debug(f'Migrating {len(df)} sites')
            
        for i, (idx, row) in enumerate(df[df.platform_path.notnull()].iterrows()):
            domain = row.resolved_domain

            if domain in self.tranche_1_domains:
                logger.debug(f'Skipping tranche 1 site {domain}')
                continue

            # This also raises SiteUnavailableError on a timeout or HTTP
            # failure, preventing an unreadable site from being archived.
            if is_under_maintenance(domain):
                logger.critical(f'Skipping site {domain} - under maintenance')
                continue

            count = row.get('_sitemap_urls')
            count_label = (
                f' ({int(count):,} URLs)'
                if count is not None and not pd.isna(count)
                else ''
            )
            logger.info(f'Queuing site {i + 1} - {domain}{count_label}')

        #     # if row.domain == 'microgastrinae.myspecies.info': continue

            yield SiteTask(
                domain=domain,
                platform_path=self.platform_path or row.platform_path,
                page_path=self.page_path,
                page_prefix=self.page_prefix,
                page_limit=self.page_limit,
                page_offset=self.page_offset,
            )

    def order_sites(self, sites):
        """Order multi-site migrations by hostname or existing URL count."""
        if self.sort_by not in {'name', 'url-count'}:
            raise ValueError('sort_by must be "name" or "url-count"')
        if self.sort_order not in {'asc', 'desc'}:
            raise ValueError('sort_order must be "asc" or "desc"')

        ascending = self.sort_order == 'asc'
        if self.sort_by == 'name':
            return sites.sort_values(
                by=['resolved_domain'],
                ascending=ascending,
                kind='stable',
            )

        counts = {}
        missing = []
        for domain in sites.resolved_domain.astype(str):
            sitemap_path = (
                config.PROCESSING_DATA_DIR
                / 'sitemaps'
                / f'{domain}.yaml'
            )
            count = sitemap_url_count(sitemap_path)
            if count is None:
                missing.append(domain)
            else:
                counts[domain] = count

        if missing:
            examples = ', '.join(sorted(missing)[:5])
            if len(missing) > 5:
                examples += f', and {len(missing) - 5} more'
            raise RuntimeError(
                'URL-count ordering requires an existing sitemap for every '
                f'selected site. Missing {len(missing)}: {examples}. '
                'Run `uv run htmlify sitemaps` first.'
            )

        ordered = sites.copy()
        ordered['_sitemap_urls'] = ordered.resolved_domain.map(counts)
        return ordered.sort_values(
            by=['_sitemap_urls', 'resolved_domain'],
            ascending=[ascending, True],
            kind='stable',
        )

    # def resolve_non_myspecies_domain(self, domain):

    #     protocols = ['http', 'https']

    #     response = None

    #     for protocol in protocols:
    #         url = f'{protocol}://{domain}'
    #         try:
    #             response = requests.get(url, allow_redirects=True)
    #         except Exception as e:
    #             continue
    #         else:
    #             break

    #     if response:
    #         resolved_domain = urlparse(response.url).netloc
    #         if resolved_domain != domain and 'myspecies.info' in resolved_domain:
    #             logger.debug(f'Resolved domain {domain} to {resolved_domain}')
    #             return resolved_domain


if __name__ == "__main__":    
    domain = 'tabanidae.myspecies.info'

    luigi.build([MigrateTask(force=True, domain=domain)], local_scheduler=True, workers=10)    
