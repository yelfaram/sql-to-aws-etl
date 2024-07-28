import pandas as pd

from config import settings
from utils.logging import setup_logging
from db.database import Database
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logger = setup_logging('logs/extract_data.log', 'extract_data')

# Query the database to retrieve the data
def extract(conn):
    """Extract data from the database and save to a CSV file."""
    try:
        query = "SELECT * FROM covid_cases"
        df = pd.read_sql_query(query, conn)
        logger.info("Data fetched successfully into DataFrame")

        # Save DataFrame to CSV        
        df.to_csv(settings.OUTPUT_PATH, index=False)
        logger.info(f"Data saved to '{settings.OUTPUT_PATH}'")
    except Exception as e:
        logger.exception(f"Error occurred while trying to retrieve data from PostgreSQL: \n{e}")
        raise


def main():
    logger.info("Starting db extraction process...")
    db = Database()

    try:
        conn_db = db.conn_db
        extract(conn_db)
    finally:
        db.close_connections()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()

