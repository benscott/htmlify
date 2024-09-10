import luigi
import mysql.connector
import phpserialize
import itertools
import yaml
import pandas as pd
import requests
from urllib.parse import urlparse


from htmlify.config import DATA_DIR, logger
from htmlify.tasks.base import BaseExternalTask, BaseTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.tasks.site import SiteTask

       
class MigrateTask(BaseTask):

    domain = luigi.Parameter(default=None) 
    limit = luigi.IntParameter(default=None) 

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
        'wallaceletters.myspecies.info'      
    ]
    
    def requires(self):

        sites_list_task = SitesListTask()
        luigi.build([sites_list_task], local_scheduler=True)
        # We don;t have to yield this - but keeps the dependency graph accurate
        yield sites_list_task

        df = pd.read_csv(sites_list_task.output().path)

        if self.domain:
            df = df[df.domain == self.domain]
            if df.empty:
                raise Exception(f'No domain matching {self.domain}')
            logger.debug(f'Migrating site {self.domain}')
        else:            
            df = df.sort_values(by=['domain'])            

        # None of the emonocot domains resolve
        df = df[~df.domain.str.contains('.e-monocot.org')]

        if self.limit:
            df = df.iloc[:self.limit]
        
        logger.debug(f'Migrating {len(df)} sites')
            
        for i, (idx, row) in enumerate(df[df.platform_path.notnull()].iterrows()):
            if 'myspecies.info' not in row.domain:
                logger.debug(f'Not a myspecies domain {row.domain} - resolving domain...')
                myspecies_domain = self.resolve_non_myspecies_domain(row.domain)
                if not myspecies_domain:
                    logger.error(f'Could not resolve non-myspecies domain {row.domain}')
                    continue
            else:
                myspecies_domain = row.domain

            if myspecies_domain in self.tranche_1_domains:
                logger.debug(f'Skipping tranche 1 site {myspecies_domain}')
                continue

            logger.debug(f'Queuing site {i} - {myspecies_domain}')

        #     # if row.domain == 'microgastrinae.myspecies.info': continue

            yield SiteTask(
                domain=row.domain,
                platform_path=row.platform_path,             
            )

    def resolve_non_myspecies_domain(self, domain):

        protocols = ['http', 'https']

        response = None

        for protocol in protocols:
            url = f'{protocol}://{domain}'
            try:
                response = requests.get(url, allow_redirects=True)
            except Exception as e:
                continue
            else:
                break

        if response:
            resolved_domain = urlparse(response.url).netloc
            if resolved_domain != domain and 'myspecies.info' in resolved_domain:
                logger.debug(f'Resolved domain {domain} to {resolved_domain}')
                return resolved_domain

if __name__ == "__main__":    
    # domain = 'acanthaceae.myspecies.info'
    luigi.build([MigrateTask(force=True, limit=10)], local_scheduler=True, workers=10)    
