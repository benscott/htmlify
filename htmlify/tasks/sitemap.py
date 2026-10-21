import luigi
import mysql.connector
import itertools
import phpserialize
import yaml
import pandas as pd


from htmlify.config import PROCESSING_DATA_DIR, DB_USERNAME, DB_PASSWORD, logger
from htmlify.tasks.base import BaseExternalTask, BaseTask
from htmlify.tasks.sites_list import SitesListTask
from htmlify.db import db_manager
from htmlify.taxonomy import get_biological_vids


SITEMAP_VERSION = '4'

class SiteMapTask(BaseTask):
    
    domain = luigi.Parameter()
    # db_conn = luigi.Parameter()
    output_dir = PROCESSING_DATA_DIR / 'sitemaps'

    excluded_menu_names = {
        'devel',
        'management',
        'navigation',
        'user-menu',
    }

    excluded_path_prefixes = (
        'admin',
        'batch',
        'devel',
        'dwca-export',
        'dwca_export',
        'file-colorboxed',
    )

    excluded_path_parts = {
        'ajax',
        'autocomplete',
        'nojs',
        'preview',
    }

    excluded_path_suffixes = (
        '.bib',
        '.csv',
        '.json',
        '.xml',
        '/csv',
        '/feed',
        '/xml',
    )

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
            self.get_user_urls(),
            self.get_comment_urls(),
            self.get_blog_urls(),
            self.get_forum_urls(),
            self.get_menu_urls(),
            self.get_view_urls(),
            # BUGFIX: Moved to crating symlinks
            # self.get_public_aliases_urls(),
            self.get_term_urls()
        ]
        
        urls = list(dict.fromkeys(itertools.chain.from_iterable(urls)))

        logger.debug(f'{len(urls)} found in sitemap')

        with self.output().open('w') as f:
            f.write(f'# htmlify-sitemap-version: {SITEMAP_VERSION}\n')
            yaml.dump(urls, f)

    def output(self):
        return luigi.LocalTarget(self.output_dir / f'{self.domain}.yaml')   

    def complete(self):
        if self.force or not self.output().exists():
            return False
        with self.output().open() as sitemap:
            first_line = sitemap.readline().strip()
        return first_line == f'# htmlify-sitemap-version: {SITEMAP_VERSION}'

    def _get_biological_vids(self):
        return get_biological_vids(self.domain)

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

    def get_forum_urls(self):
        """Return Drupal forum routes for every configured forum term."""
        row = self._query_one(
            'SELECT value FROM variable '
            'WHERE name = "forum_nav_vocabulary"',
        )
        if not row or not row[0]:
            return []

        try:
            forum_vid = int(phpserialize.loads(row[0]))
        except (TypeError, ValueError):
            logger.warning(
                'Could not decode forum vocabulary for %s',
                self.domain,
            )
            return []

        terms = self._query(f"""
            SELECT tid
            FROM taxonomy_term_data
            WHERE vid = {forum_vid}
            ORDER BY tid
        """)
        return [f'forum/{tid}' for tid, in terms]
    
    def get_user_urls(self):
        urls = []
        result = self._query(f"SELECT uid, status FROM users WHERE status = 1")
        for uid, _ in result:
            urls.append(f'user/{uid}')

        return urls    

    @classmethod
    def is_archivable_path(cls, path):
        """Return whether a database route is a useful public HTML page."""
        if not path:
            return False

        path = path.strip().strip('/')
        if not path or path in {'<front>', '<nolink>'}:
            return False

        lower_path = path.lower()
        if '%' in path or '://' in path:
            return False
        if lower_path == 'biblio/import':
            return False
        if any(
            lower_path == prefix or lower_path.startswith(f'{prefix}/')
            for prefix in cls.excluded_path_prefixes
        ):
            return False
        if any(
            part in cls.excluded_path_parts
            for part in lower_path.split('/')
        ):
            return False
        if lower_path.endswith(cls.excluded_path_suffixes):
            return False

        return True

    def get_menu_urls(self):
        """Return visible internal links from non-administrative menus."""
        excluded_names = ', '.join(
            f"'{name}'" for name in sorted(self.excluded_menu_names)
        )
        result = self._query(f"""
            SELECT DISTINCT link_path
            FROM menu_links
            WHERE hidden = 0
              AND external = 0
              AND menu_name NOT IN ({excluded_names})
            ORDER BY link_path
        """)
        return [
            path
            for path, in result
            if self.is_archivable_path(path)
        ]

    def get_view_urls(self):
        """Return compiled page-display routes, including code-exported Views."""
        result = self._query("""
            SELECT DISTINCT path
            FROM menu_router
            WHERE page_callback = 'views_page'
            ORDER BY path
        """)
        return [
            path
            for path, in result
            if self.is_archivable_path(path)
        ]
    
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

        for tid, vid in result:

            urls.append(f'taxonomy/term/{tid}')

            # Biological terms are archived as one combined page. PageTask
            # discovers that vocabulary's enabled tabs from Drupal and embeds
            # their content in the canonical term route.
            if vid not in bio_vids:
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
