import luigi
import mysql.connector
import phpserialize
import itertools
import yaml
import pandas as pd


from htmlify.config import PROCESSING_DATA_DIR, DB_USERNAME, DB_PASSWORD, logger
from htmlify.tasks.base import BaseExternalTask, BaseTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.db import db_manager

class SiteMapTask(BaseTask):
    
    domain = luigi.Parameter()
    # db_conn = luigi.Parameter()
    output_dir = PROCESSING_DATA_DIR / 'sitemaps'

    def run(self):

        default_urls = [
            "biblio",
            "gallery",
            "legal",
            "search",
            "contact",
            "contact/1",
            "contact/2",
            "user",
            "user/register",
            "user/password",            
        ]

        urls = [
            default_urls,
            self.get_node_urls(),
            self.get_file_urls(),
            self.get_classification_urls(),
            self.get_image_urls(),
            self.get_user_urls(),
            self.get_comment_urls(),
            self.get_public_aliases_urls(),
            self.get_term_urls()
        ]
        urls = list(itertools.chain.from_iterable((urls)))

        logger.debug(f'{len(urls)} found in sitemap')

        with self.output().open('w') as f: 
            yaml.dump(urls, f)

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')       

    def _get_biological_vids(self):
        value = self._query_one(f'SELECT value FROM variable where name="biological_vids"')
        unserialized_data = phpserialize.loads(value[0])
        vids = [vid for vid, is_class in unserialized_data.items() if is_class]
        return vids   

    def _query(self, sql): 
        return db_manager.fetch(self.domain, sql)
    
    def _query_one(self, sql): 
        return db_manager.fetch_one(self.domain, sql)    
    
    def get_node_urls(self):

        revisions_are_public = bool(self._query_one("""
            SELECT 1
            FROM role_permission
            WHERE rid = 1
            AND permission = 'view revisions'
            LIMIT 1;     
        """))   

        nodes = self._query("""
            SELECT n.nid, 
                (CASE WHEN rev_count.revision_count > 1 THEN 1 ELSE 0 END) AS has_revisions
            FROM node n
            LEFT JOIN (
            SELECT nid, COUNT(vid) AS revision_count
            FROM node_revision
            GROUP BY nid
            ) rev_count ON n.nid = rev_count.nid
            WHERE n.status = 1
        """)
        
        urls = []

        for nid, has_revisions in nodes:
            urls.append(f'node/{nid}')
            urls.append(f'node/{nid}/view')
            if revisions_are_public and has_revisions:
                urls.append(f'node/{nid}/revisions')
                urls.append(f'node/{nid}/revisions/view')
                revisions = self._query(f'SELECT vid FROM node_revision WHERE nid="{nid}"')
                for vid in revisions:
                    urls.append(f'node/{nid}/revisions/{vid[0]}')
                    urls.append(f'node/{nid}/revisions/{vid[0]}/view')

        return urls
    
    def get_file_urls(self):
        urls = []
        fids = self._query(f'SELECT fid FROM file_managed WHERE status = 1')
        for fid in fids:
            urls.append(f'file/{fid[0]}')    

        return urls    

    def get_classification_urls(self):
        urls = []
        for vid in self._get_biological_vids():
            urls.append(f'classification/{vid}')
        return urls


    def get_image_urls(self):
        urls = []
        result = self._query(f"SELECT fid, uri FROM file_managed WHERE filemime LIKE 'image/%'")
        for fid, uri in result:
            urls.append(f'file-colorboxed/{fid}')    

        return urls
    
    def get_comment_urls(self):
        urls = []
        result = self._query(f"SELECT c.cid, c.nid FROM comment c INNER JOIN node n ON c.nid = n.nid WHERE c.status = 1 AND n.status = 1")
        for cid, nid in result:
            urls.append(f'comment/{cid}')
            urls.append(f'comment/{cid}/view')

        return urls
    
    def get_user_urls(self):
        urls = []
        result = self._query(f"SELECT uid, status FROM users WHERE status = 1")
        for uid, _ in result:
            urls.append(f'user/{uid}')

        return urls    
    
    def get_public_aliases_urls(self):
        urls = []
        result = self._query(f"SELECT alias, source FROM url_alias")
        for alias, _ in result:
            urls.append(alias)

        return urls
    
    def get_term_urls(self):
        bio_vids = self._get_biological_vids()

        tax_revisions_are_public = bool(self._query_one("""
            SELECT 1
            FROM role_permission
            WHERE rid = 1
            AND permission = 'view taxonomy term revisions'
            LIMIT 1;  
        """))  

        urls = []
        result = self._query(f"SELECT tid, vid FROM taxonomy_term_data")

        bio_tabs = ['overview', 'descriptions', 'literature', 'maps', 'media', 'specimens']

        if tax_revisions_are_public:
            bio_tabs.append('revisions')

        for tid, vid in result:

            urls.append(f'taxonomy/term/{tid}')
            urls.append(f'taxonomy/term/{tid}/view')
            if vid in bio_vids:
                for tab in bio_tabs:
                    urls.append(f'taxonomy/term/{tid}/{tab}')
            else:
                urls.append(f'simpletaxonomy/term/{tid}')
                urls.append(f'nontaxonomy/term/{tid}')

        return urls    

if __name__ == "__main__":    
    domain = 'gadus.myspecies.info'

    # db_conn = mysql.connector.connect(
    #         host='157.140.2.164',
    #         port=3306,
    #         user=DB_USERNAME,
    #         password=DB_PASSWORD,
    #         database='abamyspeciesin_0'
    #     )      

    luigi.build([SiteMapTask(domain=domain, force=True)], local_scheduler=True)    
