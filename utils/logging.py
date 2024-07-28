import logging
import os

def setup_logging(log_file, logger_name):
    """Setup logging configuration."""
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger(logger_name)

    # Avoid adding handlers multiple times in case of multiple imports
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(log_file, mode='w')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger