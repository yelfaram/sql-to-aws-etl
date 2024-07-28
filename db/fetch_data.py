import os
import datadotworld as dw

from pandas import DataFrame
from utils.logging import setup_logging
from config import settings
from dotenv import load_dotenv
load_dotenv()

# Set up data.world dataset and other config variables
dataset_key = settings.DATASET_KEY
table_name = settings.DATASET_TABLE_NAME

# Check if dataset variables were loaded correctly
if not all([dataset_key, table_name]):
    raise ValueError("Data.world auth and dataset details not found in environment variables")

# Configure logging
logger = setup_logging('logs/fetch_data.log', 'fetch_data')

# fetch dataset from datadotworld
def fetch_data_from_datadotworld(dataset_key: str, table_name: str) -> DataFrame:
    """Fetch data from data.world and return as DataFrame."""
    try: 
        query = f'SELECT * FROM {table_name} LIMIT 5000'
        results = dw.query(dataset_key, query)
        dataframe = results.dataframe
        logger.info("Data fetched successfully.")
        return dataframe
    except Exception as e:
        logger.exception(f"Error occurred while fetching data from data.world: \n{e}")
        raise

def save_dataframe_to_csv(dataframe: DataFrame, file_path: str) -> None:
    """Save DataFrame to a CSV file."""
    try:
        # ensure data directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        logger.info(f"Ensured directory {os.path.dirname(file_path)} exists")

        # save dataframe to a CSV file
        dataframe.to_csv('data/raw_data.csv', index=False)
        logger.info("Data saved to CSV successfully.")
    except Exception as e:
        logger.exception(f"Error occurred while saving data to CSV: \n{e}")
        raise

def main():
    logger.info("Starting data fetch process...")
    dataframe = fetch_data_from_datadotworld(dataset_key, table_name)
    print(dataframe.head())
    save_dataframe_to_csv(dataframe, settings.RAW_DATA_PATH)

if __name__ == "__main__":
    main()