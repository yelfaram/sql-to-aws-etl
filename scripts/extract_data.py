import os
import sys
import pandas as pd
import logging

from dotenv import load_dotenv
load_dotenv()

# Set up the project root path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extract_data.log', mode='w'),
        logging.StreamHandler()
    ]
)

logging.info("Starting db extraction process...")

# Connect to the database
from db.connection import conn

# Query the database to retrieve the data
try:
    query = "SELECT * FROM covid_cases"
    df = pd.read_sql_query(query, conn)
    logging.info("Data fetched successfully into DataFrame")

    # Save DataFrame to CSV
    output_path = 'data/extracted_data.csv'
    
    df.to_csv(output_path, index=False)
    logging.info(f"Data saved to '{output_path}'")
except Exception as e:
    logging.error(f"Error occurred while trying to retrieve data from PostgreSQL: \n{e}")
    raise
finally:
    conn.close()
    logging.info("Database connection closed")

