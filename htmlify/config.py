from pathlib import Path
import logging
import os
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent.parent.resolve()

load_dotenv(ROOT_DIR / '.env')

DATA_DIR = Path(ROOT_DIR / 'data')

ASSETS_DIR = Path(DATA_DIR / 'assets')

TEMPLATE_DIR = Path(DATA_DIR / 'templates')

PROCESSING_DATA_DIR = Path(DATA_DIR / 'processing')
PROCESSING_DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(DATA_DIR / '.log')
LOG_DIR.mkdir(parents=True, exist_ok=True)

SITES_DIR = Path(os.getenv('SITES_DIR', '/Users/ben/Projects/Scratchpads/Sites'))
SITES_DIR.mkdir(parents=True, exist_ok=True)

APACHE_VHOSTS_DIR = Path(os.getenv('APACHE_VHOSTS_DIR', '/etc/apache2/sites-available/'))
APACHE_VHOSTS_DIR.mkdir(parents=True, exist_ok=True)

PLATFORMS_ROOT_PATH = os.getenv('PLATFORMS_ROOT_PATH')
if PLATFORMS_ROOT_PATH:
    PLATFORMS_ROOT_PATH = Path(PLATFORMS_ROOT_PATH)

MAX_PAGE_THRESHOLD = int(os.getenv('MAX_PAGE_THRESHOLD', 50000))

SCHEME = os.getenv('SCHEME', 'https')
USE_SELENIUM = True

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST_OVERRIDE = os.getenv('DB_HOST_OVERRIDE')
DB_PORT_OVERRIDE = int(os.getenv('DB_PORT_OVERRIDE', 3306))
DB_NAME_OVERRIDE = os.getenv('DB_NAME_OVERRIDE')
DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', 10))

# When set, requests for SOURCE_DOMAIN are sent to SOURCE_BASE_URL instead.
# Generated links continue to use SOURCE_DOMAIN, so a local Drupal instance can
# be used as the renderer without leaking its development hostname.
SOURCE_BASE_URL = os.getenv('SOURCE_BASE_URL')
SOURCE_DOMAIN = os.getenv('SOURCE_DOMAIN')
REQUEST_TIMEOUT = float(os.getenv('REQUEST_TIMEOUT', 30))
SOLR_BASE_URL = os.getenv('SOLR_BASE_URL')
FACET_INDEX_MAX_RECORDS = int(os.getenv('FACET_INDEX_MAX_RECORDS', 250000))
DRUPAL_FACET_CONFIG_PATH = os.getenv('DRUPAL_FACET_CONFIG_PATH')

# "symlink" is useful when htmlify and Drupal share a filesystem. "external"
# expects the Drupal sites directory to be copied separately (for example with
# rsync) and validated afterwards.
DRUPAL_FILES_MODE = os.getenv('DRUPAL_FILES_MODE', 'symlink')
WRITE_APACHE_VHOSTS = os.getenv('WRITE_APACHE_VHOSTS', '1').lower() not in {
    '0', 'false', 'no', 'off'
}

CRAWL_SITES = False


def configure_local_source(
    *,
    domain,
    base_url,
    db_host=None,
    db_port=None,
    db_name=None,
    db_username=None,
    db_password=None,
    solr_url=None,
    facet_config_path=None,
):
    """Configure this process to render one canonical site from a local origin."""
    global SOURCE_DOMAIN, SOURCE_BASE_URL
    global DB_HOST_OVERRIDE, DB_PORT_OVERRIDE, DB_NAME_OVERRIDE
    global DB_USERNAME, DB_PASSWORD, SOLR_BASE_URL, WRITE_APACHE_VHOSTS
    global DRUPAL_FACET_CONFIG_PATH

    SOURCE_DOMAIN = domain
    SOURCE_BASE_URL = base_url.rstrip('/')
    DB_HOST_OVERRIDE = db_host or DB_HOST_OVERRIDE
    DB_PORT_OVERRIDE = int(db_port or DB_PORT_OVERRIDE)
    DB_NAME_OVERRIDE = db_name or DB_NAME_OVERRIDE
    DB_USERNAME = db_username or DB_USERNAME
    DB_PASSWORD = db_password or DB_PASSWORD
    SOLR_BASE_URL = solr_url or SOLR_BASE_URL
    DRUPAL_FACET_CONFIG_PATH = str(facet_config_path) if facet_config_path else None
    WRITE_APACHE_VHOSTS = False

    # Luigi uses spawned worker processes on macOS. Module globals configured
    # by the CLI are not copied into those fresh Python interpreters, while
    # environment variables are inherited. Keep both representations in sync
    # so every page worker renders from DDEV rather than the public hostname.
    process_settings = {
        'SOURCE_DOMAIN': SOURCE_DOMAIN,
        'SOURCE_BASE_URL': SOURCE_BASE_URL,
        'DB_HOST_OVERRIDE': DB_HOST_OVERRIDE,
        'DB_PORT_OVERRIDE': DB_PORT_OVERRIDE,
        'DB_NAME_OVERRIDE': DB_NAME_OVERRIDE,
        'DB_USERNAME': DB_USERNAME,
        'DB_PASSWORD': DB_PASSWORD,
        'SOLR_BASE_URL': SOLR_BASE_URL,
        'DRUPAL_FACET_CONFIG_PATH': DRUPAL_FACET_CONFIG_PATH,
        'WRITE_APACHE_VHOSTS': '0',
    }
    for name, value in process_settings.items():
        if value is not None:
            os.environ[name] = str(value)
        elif name == 'DRUPAL_FACET_CONFIG_PATH':
            os.environ.pop(name, None)


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

# Set up file logging for errors and warnings
critical_file_handler = logging.FileHandler(LOG_DIR / 'critical.log')
critical_file_handler.setFormatter(
    logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
)
# Log errors to files
critical_file_handler.setLevel(logging.CRITICAL)
logger.addHandler(critical_file_handler)
