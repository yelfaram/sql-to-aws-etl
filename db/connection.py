from psycopg2 import connect
from db.db_config import *
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/connection.py', mode='w'),
        logging.StreamHandler()
    ]
)

# Connect to database
try: 
    conn = connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    logging.info("Database connected successfully")
except Exception as e:
    logging.error(f"Error occured while connecting to database: {e}")
    raise
