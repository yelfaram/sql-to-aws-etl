import numpy as np
import pandas as pd

from utils.data_cleaning import convert_point_format
from utils.logging import setup_logging
from config import settings
from .database import Database

# Configure logging
logger = setup_logging('logs/populate_db.log', 'populate_db')


# Create table
def create_table(cur):
    query = """
    DROP TABLE IF EXISTS covid_cases;

    CREATE TABLE IF NOT EXISTS covid_cases (
        case_type TEXT,
        cases INTEGER,
        difference INTEGER,
        date DATE,
        country_region TEXT,
        province_state TEXT,
        admin2 TEXT,
        combined_key TEXT,
        fips INTEGER,
        lat DECIMAL,
        long DECIMAL,
        location POINT,
        table_names TEXT,
        prep_flow_runtime TIMESTAMP
    );
    """

    try:
        cur.execute(query)
        logger.info("Table created successfully")
    except Exception as e:
        logger.error(f"Error occured while creating table covid_cases: \n{e}")
        raise

# preprocess CSV file to format POINT
def preprocess_csv():
    try:
        df = pd.read_csv(settings.RAW_DATA_PATH)

        # Convert `location` column from POINT(x y) format to (x, y) format
        if 'location' in df.columns:
            df['location'] = df['location'].apply(lambda point_str: convert_point_format(point_str, logger))

        # Convert `fips` to integer
        if 'fips' in df.columns:
            # coerce -> invalid parsing will be set as NaN || fillna -> convert NaN to 0 || astype -> cast to int
            df['fips'] = pd.to_numeric(df['fips'], errors='coerce').fillna(0).astype(np.int64)

        # save preprocessed file
        df.to_csv(settings.PREPROCESSED_CSV_FILE_PATH, index=False)
        logger.info("CSV file preprocessed successfully")
    except Exception as e:
        logger.error(f"Error occured while preprocessing CSV file: \n{e}")
        raise

# Insert data into the table
def insert_data(cur):
    try:
        with open(settings.PREPROCESSED_CSV_FILE_PATH, 'r') as f:
            cur.copy_expert(
                sql="COPY covid_cases (case_type, cases, difference, date, country_region, province_state, admin2, combined_key, fips, lat, long, location, table_names, prep_flow_runtime) FROM STDIN WITH CSV HEADER",
                file=f
            )
        logger.info("Data inserted successfully.")
    except Exception as e:
        logger.error(f"Error occurred while inserting data using copy_from: \n{e}")
        raise

def main():
    logger.info("Starting db population process...")
    db = Database()

    try:
        cur = db.conn_db.cursor()
        create_table(cur)
        preprocess_csv()
        insert_data(cur)
    finally:
        db.close_connections()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()