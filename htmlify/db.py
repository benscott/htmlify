import mysql.connector
import atexit
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import socket
import typer

from htmlify import config


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

class DBConnectionManager:
    def __init__(self):

        # Load the hosts data
        df = pd.read_csv(config.DATA_DIR / 'vhosts.csv', usecols=['domain', 'db_name', 'db_host'])
        self._sites = df.set_index('domain')[['db_name','db_host']]
        self._connections = {}  # Dictionary to hold multiple connections

    def _get_site(self, domain):
        site = self._sites.loc[domain, ['db_name', 'db_host']]

        if site.empty:
            raise Exception(f'No domain matching {domain}')          
        
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
