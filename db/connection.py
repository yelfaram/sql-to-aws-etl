from psycopg2 import connect
from db_config import *
import logging

# Configure logging
logging.basicConfig(
    filename='logs/connection.py',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
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
