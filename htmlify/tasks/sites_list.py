import luigi
import mysql.connector
import phpserialize
import itertools
import yaml
import pandas as pd


from htmlify.config import DATA_DIR, PROCESSING_DATA_DIR
from htmlify.tasks.base import BaseExternalTask, BaseTask

class SitesListTask(BaseExternalTask):

    def run(self):
        # column_names = ['nid', 'domain', 'platform_path', 'db_name', 'db_host']
        df = pd.read_csv(DATA_DIR / 'vhosts.csv')  

        site_aliases = pd.read_csv(DATA_DIR / 'static-site-aliases.csv', sep='\t', names=['nid', 'alias'])      

        def _get_resolved_domain(row):
            aliases = site_aliases[site_aliases.nid == row.nid]
            if not aliases.empty:
                return aliases.alias.values[0]
            return row.domain
        
        df.resolved_domain = df.apply(_get_resolved_domain, axis=1)
        df.to_csv(self.output().path, index=False)
    
    def output(self):
        return luigi.LocalTarget(DATA_DIR / 'sites-list.csv')       



if __name__ == "__main__":    
    domain = '127.0.0.1'
    luigi.build([SitesListTask(force=True)], local_scheduler=True)    