import logging
import os
from datetime import datetime
from pathlib import Path

# Create logs directory
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Log file paths
today = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = LOG_DIR / f"app-{today}.log"
ERROR_LOG_FILE = LOG_DIR / f"error-{today}.log"

# Get log level from environment
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Create formatters
detailed_formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

simple_formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)

# Create handlers
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
console_handler.setFormatter(simple_formatter)

file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
file_handler.setLevel(LOG_LEVEL)
file_handler.setFormatter(detailed_formatter)

error_file_handler = logging.FileHandler(ERROR_LOG_FILE, mode="a", encoding="utf-8")
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(detailed_formatter)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)
root_logger.addHandler(error_file_handler)

# Create app logger
logger = logging.getLogger("ml-service")

def get_logger(name: str = "ml-service") -> logging.Logger:
    """Get a logger instance with the given name."""
    return logging.getLogger(name)
