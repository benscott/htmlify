import mysql.connector
import atexit
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import socket
import typer

from htmlify import config


def _build_site_lookup(sites, aliases=None):
    """Index database details by both hosted domains and site aliases."""
    columns = ['db_name', 'db_host']
    original = sites[['domain', *columns]].rename(
        columns={'domain': 'lookup_domain'},
    )
    lookups = [original]

    if (
        aliases is not None
        and 'nid' in sites.columns
        and {'nid', 'alias'} <= set(aliases.columns)
    ):
        site_details = sites[['nid', *columns]].copy()
        alias_details = aliases[['nid', 'alias']].copy()
        # vhosts.csv contains blank NIDs, so pandas normally reads that column
        # as floats while the aliases file uses integers. Normalise both before
        # joining them.
        site_details['nid'] = pd.to_numeric(
            site_details['nid'], errors='coerce',
        ).astype('Int64')
        alias_details['nid'] = pd.to_numeric(
            alias_details['nid'], errors='coerce',
        ).astype('Int64')
        alias_details = alias_details.merge(site_details, on='nid', how='inner')
        alias_details = alias_details.rename(
            columns={'alias': 'lookup_domain'},
        )
        lookups.append(alias_details[['lookup_domain', *columns]])

    lookup = pd.concat(lookups, ignore_index=True)
    lookup = lookup.dropna(subset=['lookup_domain'])
    lookup['lookup_domain'] = lookup['lookup_domain'].astype(str).str.strip()
    lookup = lookup[lookup.lookup_domain != '']
    # Prefer the hostname from vhosts.csv if a malformed alias duplicates it.
    lookup = lookup.drop_duplicates('lookup_domain', keep='first')
    return lookup.set_index('lookup_domain')[columns]


def unreachable_database_hosts(targets, timeout=5):
    """Return TCP database endpoints that cannot be reached quickly."""
    targets = sorted({(str(host), int(port)) for host, port in targets})

    def probe(target):
        host, port = target
        try:
            connection = socket.create_connection(target, timeout=timeout)
            connection.close()
            return None
        except OSError as error:
            return {
                'host': host,
                'port': port,
                'error': str(error),
            }

    with ThreadPoolExecutor(max_workers=max(1, min(10, len(targets)))) as pool:
        return [result for result in pool.map(probe, targets) if result]


def database_authentication_errors(
    targets,
    username,
    password,
    timeout=5,
):
    """Authenticate once against each distinct MySQL endpoint."""
    targets = sorted({(str(host), int(port)) for host, port in targets})

    def probe(target):
        host, port = target
        try:
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                connection_timeout=timeout,
            )
            connection.close()
            return None
        except mysql.connector.Error as error:
            return {
                'host': host,
                'port': port,
                'error': str(error),
            }

    with ThreadPoolExecutor(max_workers=max(1, min(10, len(targets)))) as pool:
        return [result for result in pool.map(probe, targets) if result]


class DBConnectionManager:
    def __init__(self):

        # Load the hosts data
        sites = pd.read_csv(config.DATA_DIR / 'vhosts.csv')
        aliases_path = config.DATA_DIR / 'static-site-aliases.csv'
        aliases = None
        if aliases_path.is_file():
            aliases = pd.read_csv(
                aliases_path,
                sep='\t',
                names=['nid', 'alias'],
            )
        self._sites = _build_site_lookup(sites, aliases)
        self._connections = {}  # Dictionary to hold multiple connections

    def _get_site(self, domain):
        try:
            site = self._sites.loc[domain, ['db_name', 'db_host']]
        except KeyError as error:
            raise LookupError(
                f'No database mapping for domain {domain}',
            ) from error

        if site.empty:
            raise LookupError(f'No database mapping for domain {domain}')
        
        return tuple(site)


    def open_connection(self, domain):
        if domain not in self._connections:
            use_overrides = not config.SOURCE_DOMAIN or domain == config.SOURCE_DOMAIN

            # A canonical domain can differ from the hostname stored in the
            # production registry (for example weevil.myspecies.info is
            # registered as weevil.info). A fully configured local source does
            # not need that production lookup at all.
            if (
                use_overrides
                and config.DB_NAME_OVERRIDE
                and config.DB_HOST_OVERRIDE
            ):
                db_name = config.DB_NAME_OVERRIDE
                db_host = config.DB_HOST_OVERRIDE
            else:
                db_name, db_host = self._get_site(domain)

            if use_overrides:
                db_name = config.DB_NAME_OVERRIDE or db_name
                db_host = config.DB_HOST_OVERRIDE or db_host

            connection = mysql.connector.connect(
                host=db_host,
                port=config.DB_PORT_OVERRIDE if use_overrides else 3306,
                user=config.DB_USERNAME,
                password=config.DB_PASSWORD,
                database=db_name,
                connection_timeout=config.DB_CONNECT_TIMEOUT,
            )     

            cursor = connection.cursor()
            self._connections[domain] = {'connection': connection, 'cursor': cursor}
            print(f"Database connection to {domain} opened.")
        else:
            print(f"Database connection to {domain} already exists.")

    def fetch(self, domain, query):
        cursor = self.execute_query(domain, query)    
        return cursor.fetchall()
    
    def fetch_one(self, domain, query):
        cursor = self.execute_query(domain, query)    
        return cursor.fetchone()    
        
    def execute_query(self, domain, query):
        """Execute a SQL query on the specified database."""
        if not domain in self._connections:
            self.open_connection(domain)
            # raise Exception(f"No active connection to {domain} found.")
        
        cursor = self._connections[domain]['cursor']
        cursor.execute(query)
        return cursor

    def close_connection(self, domain):
        """Close the database connection for the specified database."""
        if domain in self._connections:
            cursor = self._connections[domain]['cursor']
            connection = self._connections[domain]['connection']
            cursor.close()
            connection.close()
            del self._connections[domain]
            typer.secho(f"Database connection to {domain} closed.", fg=typer.colors.RED)
        else:
            print(f"No active connection to {domain} found.")

    def close_all_connections(self):
        """Close all open database connections."""
        for domain in list(self._connections.keys()):
            self.close_connection(domain)

    def __del__(self):
        """Ensure all connections are closed when the object is destroyed."""
        self.close_all_connections()

# Register the close_all_connections method to be called at script exit
db_manager = DBConnectionManager()
atexit.register(db_manager.close_all_connections)

# Usage Example
if __name__ == "__main__":
    # Open connections to multiple databases
    db_manager.open_connection("gadus.myspecies.info")
    # db_manager._get_site('tetracyclus.myspecies.info')
    # x = db_manager.fetch("gadus", f'SELECT value FROM variable where name="biological_vids"')
    # print(x)
    # db_manager.open_connection("example2.db")
