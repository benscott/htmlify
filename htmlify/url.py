import urllib
from operator import itemgetter
from pathlib import Path
import re

from htmlify.config import logger

class URL:

    query_sort_params = ['sort', 'order']
    default_scheme = 'https'
    re_facet = re.compile(r'f\[(.*?)\]')

    def __init__(self, url, domain=None):
        self._url = url
        self.parsed_url = urllib.parse.urlparse(url)

        if not domain:
            # logger.warning('Using domain from page url')
            domain =  self.parsed_url.netloc

        self.domain = domain
        
        self.path = self.parsed_url.path.rstrip('/')        
        self.scheme = self.parsed_url.scheme if self.parsed_url.scheme else self.default_scheme      
        
        self.parsed_query = self._parse_query(self.parsed_url)
        self.facets = [(k, v) for k,v in self.parsed_query if self._query_param_is_facet(k)]  
    
    def has_multiple_facets(self):
        return len(self.facets) > 1 
    
    def is_faceted(self):
        return bool(self.facets)     

    def is_site_internal_link(self):
        return not self.parsed_url.netloc or self.domain in self.parsed_url.netloc
        
    def get_single_facet_urls(self):
        non_facets = [(k, v) for k,v in self.parsed_query if not self._query_param_is_facet(k)] 

        facet_urls = []
        for facet in self.facets:
            new_query = non_facets.copy()
            new_query.append(facet)           
            url = self._unparse(new_query, self.path)
            facet_urls.append(url)   
        return facet_urls
        
    def _parse_query(self, parsed_url):
        if parsed_url.query:
            decoded_query = urllib.parse.unquote(parsed_url.query)
            parsed_query = urllib.parse.parse_qsl(decoded_query)     
    
            parsed_query = [(k, v) for k,v in parsed_query if k not in self.query_sort_params]
            return sorted(parsed_query, key=itemgetter(0))    
            
        return []
    
    @staticmethod
    def _query_param_is_facet(param):
        return param.startswith('f[')    

    def get_normalised(self):        
        return self._unparse(self.parsed_query, self.path)

    def _unparse(self, query, path):
        if query:
            query = urllib.parse.urlencode(query, quote_via=urllib.parse.quote)

        domain = self.parsed_url.netloc if self.parsed_url.netloc else self.domain
            
        return urllib.parse.urlunparse((
            self.scheme, 
            domain, 
            str(path), 
            '', 
            query,
            ''
        ))   
    
    def to_path(self):

        if not self.path:
            return '/'

        path = Path(self.path)
        if self.parsed_query:
            for key, value in self.parsed_query:
                if self._query_param_is_facet(key):

                    if ':' in value:
                        split_value = value.split(':')
                        path = path / split_value[0] / split_value[1]
                    else:
                        facet = self.re_facet.match(key).group(1)
                        path = path / facet / value
                else:
                    path = path / key / value

        return str(path)
    
    def __repr__(self):
        return self._url