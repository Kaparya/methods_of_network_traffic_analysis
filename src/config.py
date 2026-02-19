import logging
from pathlib import Path
import sys

# Константы путей
BASE_DIR = Path(__file__).parent.resolve()
PARSING_DIR = BASE_DIR.parent / "parsing"
RESOURCES_DIR = BASE_DIR.parent / "resources"
MODEL_PATH = RESOURCES_DIR / "salary_model.cbm"

# Данные для обучения
X_PATH = PARSING_DIR / "features.npy"
Y_PATH = PARSING_DIR / "target.npy"

def setup_logging():
    """
    Configure and return the root logger with stdout handler.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()
