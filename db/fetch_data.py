import os
import datadotworld as dw
import pandas as pd
import logging

from dotenv import load_dotenv
load_dotenv()

# Set up data world API token and other config variables
dw_auth_token = os.getenv('DW_AUTH_TOKEN')
dataset_key = os.getenv('DATASET_KEY')
table_name = os.getenv('TABLE_NAME')

# Check if the token/variables were loaded correctly
if not dw_auth_token:
    raise ValueError("DW_AUTH_TOKEN not found in environment variables")
if not dataset_key:
    raise ValueError("DATASET_KEY not found in environment variables")
if not table_name:
    raise ValueError("TABLE_NAME not found in environment variables")

# Configure logging
logging.basicConfig(
    filename='logs/fetch_data.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

logging.info("Starting data fetch process...")

# fetch dataset from dataworld
try: 
    query = f'SELECT * FROM {table_name} LIMIT 100'

    results = dw.query(dataset_key, query)
    dataframe = results.dataframe

    logging.info("Data fetched successfully.")
except Exception as e:
    logging.error(f"Error occurred while fetching data: {e}")
    raise

# print fetched data
print(dataframe.head())

try:
    # ensure data directory exists then save csv file to it
    if not os.path.exists('data'):
        os.makedirs('data')
        logging.info("Created 'data' directory.")

    # save dataframe to a CSV file
    dataframe.to_csv('data/fetched_data.csv')

    logging.info("Data saved to CSV successfully.")
except Exception as e:
    logging.error(f"Error occurred while saving data to CSV: {e}")
    raise