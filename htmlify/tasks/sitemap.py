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
            "",
            "biblio",
            "gallery",
            "legal",
            "search",
            "contact",
            "contact/1",
            "contact/2",
            "user",
            # Removed as this can be disabled, and no longer needed
            # Added to decommisioned links
            # "user/register",
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
            self.get_blog_urls(),
            # BUGFIX: Moved to crating symlinks
            # self.get_public_aliases_urls(),
            self.get_term_urls()
        ]
        
        urls = list(itertools.chain.from_iterable((urls)))

        logger.debug(f'{len(urls)} found in sitemap')

        with self.output().open('w') as f: 
            yaml.dump(urls, f)

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')   

    def _get_biological_vids(self):
        value =self._query_one(f'SELECT value FROM variable where name="biological_vids"')        
        if not value: return []
        unserialized_data = phpserialize.loads(value[0])
        vids = [vid for vid, is_class in unserialized_data.items() if is_class]
        return vids   

    def _query(self, sql): 
        return db_manager.fetch(self.domain, sql)
    
    def _query_one(self, sql): 
        return db_manager.fetch_one(self.domain, sql)    
    
    def get_node_urls(self):

        nodes = self._query("""
            SELECT n.nid, n.type 
            FROM node n
            WHERE n.status = 1
        """)
        
        urls = []

        for nid, node_type in nodes:
            urls.append(f'node/{nid}')            

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

        return urls
    
    def get_blog_urls(self):
        urls = []
        result = self._query(f"SELECT distinct(u.uid) FROM users_roles ur JOIN role_permission rp ON ur.rid = rp.rid JOIN users u ON ur.uid = u.uid WHERE rp.permission = 'create blog content' and u.status=1")
        for uid in result:
            urls.append(f'blog/{uid[0]}')

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

        urls = []
        # Remove the two inbuilt taxonomies
        result = self._query(f"SELECT td.tid, td.vid FROM taxonomy_term_data td INNER JOIN taxonomy_vocabulary v on v.vid=td.vid where v.name != 'Imaging technique' and v.name != 'Preperation technique'")

        bio_tabs = ['literature', 'maps', 'media', 'specimens']

        for tid, vid in result:

            urls.append(f'taxonomy/term/{tid}')

            if vid in bio_vids:
                for tab in bio_tabs:
                    urls.append(f'taxonomy/term/{tid}/{tab}')
            else:
                urls.append(f'simpletaxonomy/term/{tid}')
                urls.append(f'nontaxonomy/term/{tid}')

        return urls    

if __name__ == "__main__":    
    domain = 'macrostomorpha.info'

    # db_conn = mysql.connector.connect(
    #         host='157.140.2.164',
    #         port=3306,
    #         user=DB_USERNAME,
    #         password=DB_PASSWORD,
    #         database='abamyspeciesin_0'
    #     )      

    luigi.build([SiteMapTask(domain=domain, force=True)], local_scheduler=True)    
