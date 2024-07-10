from pathlib import Path
import logging

ROOT_DIR = Path(__file__).parent.parent.resolve()

DATA_DIR = Path(ROOT_DIR / 'data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = Path(DATA_DIR / '.log')
LOG_DIR.mkdir(parents=True, exist_ok=True)

SITES_DIR = Path(DATA_DIR / 'sites')
SITES_DIR.mkdir(parents=True, exist_ok=True)

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
