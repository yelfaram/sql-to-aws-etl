import pandas as pd
import numpy as np
import logging

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transform_data.log', mode='w'),
        logging.StreamHandler()
    ]
)

logging.info("Starting data transformation process...")

# Read CSV file
try:
    data_db_file_path = 'data/extracted_data.csv'

    df = pd.read_csv(data_db_file_path)
    logging.info("Data read successfully.")
except Exception as e:
    logging.error(f"Error occured while reading data from CSV: \n{e}")
    raise

# print read data
print(df.head())

# log initial data properties
initial_rows, initial_columns = df.shape
logging.info(f"Initial data shape: {initial_rows} rows, {initial_columns} columns")

# start transformation process / cleaning
def custom_title_cases(s: str) -> str:
    return s.upper() if s.upper() == 'US' else s.title()

def handle_negative_and_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Handling negative and missing values...")

    df = df[df['cases'] >= 0]                                       # ensure no negative values in 'cases' column
    df.fillna({'cases': 0, 'difference': 0}, inplace=True)          # handle missing values in 'cases' and 'difference' column
    df['admin2'].replace('Unassigned', np.nan, inplace=True)        # change 'Unassigned" to NaN in 'admin2' column
    
    return df

def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Normalizing data columns...")

    df['country_region'] = df['country_region'].apply(custom_title_cases).str.strip()
    df['province_state'] = df['province_state'].str.title().str.strip()
    df['admin2'] = df['admin2'].str.title().str.strip()

    # merge columns using a seperator for not null values    
    df['combined_key'] = df[['admin2', 'province_state', 'country_region']].agg(lambda x : ', '.join(x.dropna()), axis=1)
    
    return df

def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # remove duplicate rows
    df = df.drop_duplicates()                                                                                                   
    logging.info(f"Dropped duplicates: {initial_rows - df.shape[0]} row(s) removed")

    # ensure no negative and missing values
    df = handle_negative_and_missing_values(df)
    logging.info("Handling negative and missing values... COMPLETE")

    # normalize data                                                                               
    df = normalize_data(df)
    logging.info("Normalizing data columns... COMPLETE")                                                                                                     

    # remove repetitive columns
    df.drop(columns=['country_region', 'province_state', 'admin2', 'lat', 'long', 'prep_flow_runtime'], inplace=True)           
    logging.info(f"Dropped repetitive columns: {initial_columns - df.shape[1]} column(s) removed")
    
    # rename some columns
    df.rename(columns={"location": "geo_location", "combined_key": "location"}, inplace=True)                                   
    logging.info(f"Renamed columns")

    # verify 'date' column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # verify 'case_type' column
    if not set(df['case_type'].unique()).issubset({'Confirmed', 'Deaths'}):                                                     
        logging.warning("Unexpected values found in 'case_type' column")

    return df

try:
    clean_df = validate_and_clean_data(df)

    # Log final data properties
    final_rows, final_columns = clean_df.shape
    logging.info(f"Final data shape: {final_rows} rows, {final_columns} columns")

    # Save transformed DataFrame to CSV
    output_path = 'data/transformed_data.csv'
    
    clean_df.to_csv(output_path, index=False)
    logging.info(f"Data saved to '{output_path}'")
except Exception as e:
    logging.error(f"Error occured while cleaning read data: \n{e}")
    raise

# print transformed data
print(clean_df.head(15))