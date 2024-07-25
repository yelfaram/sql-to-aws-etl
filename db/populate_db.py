import numpy as np
import pandas as pd
import logging

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/populate_db.log', mode='w'),
        logging.StreamHandler()
    ]
)

logging.info("Starting db population process...")

# Connect to database
from connection import conn

# Create table
try:
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

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    logging.info("Table created successfully")
except Exception as e:
    logging.error(f"Error occured while creating table covid_cases: \n{e}")
    raise

# preprocess CSV file to format POINT
csv_file_path = 'data/fetched_data.csv'
preprocessed_csv_file_path = 'data/preprocessed_fetched_data.csv'

def convert_point_format(point_str):                                    # old format: POINT(57.5 -20.2)
    if pd.isna(point_str) or not point_str.startswith('POINT'):         # Checks if point_str is NaN or None or if doesn't start with POINT
        return point_str
    try:
        point_str = point_str.replace('POINT(', '').replace(')', '')    # 'POINT(57.5 -20.2)' becomes '57.5 -20.2'
        x, y = point_str.split()                                        # x, y = [57.5, -20.2] destructure
        return f'({x}, {y})'                                            # new format: (57.5, -20.2)
    except Exception as e:
        logging.error(f"Error occurred while converting point format: {e}")
        return point_str

try:
    df = pd.read_csv(csv_file_path)

    # Convert `location` column from POINT(x y) format to (x, y) format
    if 'location' in df.columns:
        df['location'] = df['location'].apply(convert_point_format)

    # Convert `fips` to integer
    if 'fips' in df.columns:
        # coerce -> invalid parsing will be set as NaN || fillna -> convert NaN to 0 || astype -> cast to int
        df['fips'] = pd.to_numeric(df['fips'], errors='coerce').fillna(0).astype(np.int64)

    # save preprocessed file
    df.to_csv(preprocessed_csv_file_path, index=False)
    logging.info("CSV file preprocessed successfully")
except Exception as e:
    logging.error(f"Error occured while preprocessing CSV file: \n{e}")
    raise

# Insert data into the table
try:
    with open(preprocessed_csv_file_path, 'r') as f:
        cur.copy_expert(
            sql="COPY covid_cases (case_type, cases, difference, date, country_region, province_state, admin2, combined_key, fips, lat, long, location, table_names, prep_flow_runtime) FROM STDIN WITH CSV HEADER",
            file=f
        )
    
    conn.commit()
    logging.info("Data inserted successfully.")
    print("Data insert to db complete")
except Exception as e:
    logging.error(f"Error occurred while inserting data using copy_from: \n{e}")
    raise
finally:
    cur.close()
    conn.close()
    logging.info("Database connection closed")