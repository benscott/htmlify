import mysql.connector
import atexit
import pandas as pd

from htmlify.config import DATA_DIR, DB_USERNAME, DB_PASSWORD

class DBConnectionManager:
    def __init__(self):

        # Load the hosts data
        df = pd.read_csv(DATA_DIR / 'sites-list.csv', usecols=['domain', 'db_name', 'db_host'])
        self._sites = df.set_index('domain')[['db_name','db_host']]
        self._connections = {}  # Dictionary to hold multiple connections

    def _get_site(self, domain):
        site = self._sites.loc[domain, ['db_name', 'db_host']]

        if site.empty:
            raise Exception(f'No domain matching {domain}')          
        
        return tuple(site)


    def open_connection(self, domain):
        if domain not in self._connections:

            db_name, db_host = self. _get_site(domain)

            DB_USERNAME = 'root'
            db_host = '127.0.0.1'            

            connection = mysql.connector.connect(
                host=db_host,
                port=3306,
                user=DB_USERNAME,
                # password=DB_PASSWORD,
                database=db_name
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
            print(f"Database connection to {domain} closed.")
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

