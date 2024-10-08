from pathlib import Path
import logging
import os
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent.parent.resolve()

load_dotenv(ROOT_DIR / '.env')

DATA_DIR = Path(ROOT_DIR / 'data')

ASSETS_DIR = Path(DATA_DIR / 'assets')

PROCESSING_DATA_DIR = Path(DATA_DIR / 'processing')
PROCESSING_DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(DATA_DIR / '.log')
LOG_DIR.mkdir(parents=True, exist_ok=True)

SITES_DIR = Path(os.getenv('SITES_DIR', '/Users/ben/Projects/Scratchpads/Sites'))
SITES_DIR.mkdir(parents=True, exist_ok=True)

APACHE_VHOSTS_DIR = Path('/etc/apache2/sites-available/')


PLATFORMS_ROOT_PATH = os.getenv('PLATFORMS_ROOT_PATH')
SCHEME = os.getenv('SCHEME', 'https')
USE_SELENIUM = True


DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')


# Set up logging - inherit from luigi so we use the same interface
logger = logging.getLogger('luigi-interface')

# Set up file logging for errors and warnings
file_handler = logging.FileHandler(LOG_DIR / 'error.log')
file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
)
# Log errors to files
file_handler.setLevel(logging.WARNING)
logger.addHandler(file_handler)

# Set up file logging for errors and warnings
debug_file_handler = logging.FileHandler(LOG_DIR / 'debug.log')
debug_file_handler.setLevel(logging.DEBUG)
logger.addHandler(debug_file_handler)
