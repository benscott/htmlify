import luigi
import mysql.connector
import phpserialize
import itertools
import yaml
import pandas as pd


from htmlify.config import DATA_DIR, logger
from htmlify.tasks.base import BaseExternalTask, BaseTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.tasks.site import SiteTask

       
class MigrateTask(BaseTask):

    domain = luigi.Parameter(default=None) 
    
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
            logger.debug(f'Migrating {len(df)} sites')

        for i, row in df.iterrows():
            yield SiteTask(
                domain=row.domain,
                platform_path=row.platform_path,
                db_name=row.db_name,
                db_host=row.db_host                
            )



if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([MigrateTask(domain=domain, force=True)], local_scheduler=True)    