import pandas as pd

from config import settings
from utils.logging import setup_logging
from utils.data_cleaning import validate_and_clean_data
from pandas import DataFrame
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logger = setup_logging('logs/transform_data.log', 'transform_data')

# Read CSV file
def read_csv(file_path: str) -> DataFrame:
    try:
        df = pd.read_csv(file_path)
        logger.info("Data read successfully.")
        return df
    except Exception as e:
        logger.error(f"Error occured while reading data from CSV: \n{e}")
        raise

# start transformation process / cleaning
def transform(df: DataFrame) -> DataFrame:
    """Transform and clean data."""
    try:
        # log initial data properties
        initial_rows, initial_columns = df.shape
        logger.info(f"Initial data shape: {initial_rows} rows, {initial_columns} columns")

        clean_df = validate_and_clean_data(df, logger)
        logger.info("Data transformation complete.")
        
        # Log final data properties
        final_rows, final_columns = clean_df.shape
        logger.info(f"Final data shape: {final_rows} rows, {final_columns} columns")

        return clean_df
    except Exception as e:
        logger.error(f"Error occured during data transformation: \n{e}")
        raise

def save_dataframe_to_csv(dataframe: DataFrame, file_path: str) -> None:
    """Save DataFrame to a CSV file."""
    # Save transformed DataFrame to CSV
    try:
        dataframe.to_csv(file_path, index=False)
        logger.info(f"Data saved to '{file_path}'")
    except Exception as e:
        logger.exception(f"Error occurred while saving data to CSV: \n{e}")
        raise

def main():
    logger.info("Starting data transformation process...")
    try:
        df = read_csv(settings.OUTPUT_PATH)
        clean_df = transform(df)
        save_dataframe_to_csv(clean_df, settings.TRANSFORMED_DATA_PATH)
        print(clean_df.head(15)) # Print sample of data
    except Exception as e:
        logger.error(f"Error occurred in the main transformation process: {e}")

if __name__ == "__main__":
    main()