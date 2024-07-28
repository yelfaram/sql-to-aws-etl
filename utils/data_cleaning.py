import pandas as pd
import numpy as np
import time

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def convert_point_format(point_str: str, logger) -> str:
    """
    Convert POINT(x y) format to (x, y) format.
    """
    if pd.isna(point_str) or not point_str.startswith('POINT'):        
        return point_str
    try:
        point_str = point_str.replace('POINT(', '').replace(')', '')    
        x, y = point_str.split()
        return f'({x}, {y})'
    except Exception as e:
        logger.error(f"Error occurred while converting point format: {e}")
        return point_str
    
def custom_title_cases(s: str) -> str:
    """
    Convert strings to title case, with exception for 'US'.
    """
    return s.upper() if s.upper() == 'US' else s.title()

def handle_negative_and_missing_values(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Handle negative and missing values in DataFrame.
    """
    logger.info("Handling negative and missing values...")
    df = df[df['cases'] >= 0]                                                           # ensure no negative values in 'cases' column
    df.fillna({'cases': 0, 'difference': 0}, inplace=True)                              # handle missing values in 'cases' and 'difference' column
    df['admin2'].replace('Unassigned', np.nan, inplace=True)                            # change 'Unassigned" to NaN in 'admin2' column
    df.loc[df['admin2'].str.startswith('Out of', na=False), 'admin2'] = np.nan          # change 'Out of ...' entries to NaN in 'admin2' column
    return df

def normalize_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Normalize data columns in DataFrame.
    """
    logger.info("Normalizing data columns...")
    df['country_region'] = df['country_region'].apply(custom_title_cases).str.strip()
    df['province_state'] = df['province_state'].str.title().str.strip()
    df['admin2'] = df['admin2'].str.title().str.strip()
    # merge columns using a seperator for not null values    
    df['combined_key'] = df[['admin2', 'province_state', 'country_region']].agg(lambda x : ', '.join(x.dropna()), axis=1)
    return df

def handle_missing_geo_point_values(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Handle missing geo point coordinates in DataFrame.
    """
    logger.info("Handling missing geo point coordinates... This process may take 2-3 minutes to complete.")   
    geolocator = Nominatim(user_agent="sql_to_aws_etl", timeout=10)
    for index, row in df.iterrows():
        if row['location'] == "(0,0)":
            try:
                location = geolocator.geocode(row['combined_key'])
                if location:
                    df.at[index, 'location'] = f"({location.latitude}, {location.longitude})"
            except GeocoderTimedOut:
                time.sleep(1) # wait a bit before retrying
    return df

def validate_and_clean_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Validate and clean data in DataFrame.
    """
    initial_rows, initial_columns = df.shape

    # remove duplicate rows
    df = df.drop_duplicates()                                                                                                   
    logger.info(f"Dropped duplicates: {initial_rows - df.shape[0]} row(s) removed")

    # ensure no negative and missing values
    df = handle_negative_and_missing_values(df, logger)
    logger.info("Handling negative and missing values... COMPLETE")

    # normalize data                                                                               
    df = normalize_data(df, logger)
    logger.info("Normalizing data columns... COMPLETE") 

    # handle missing geo point values
    df = handle_missing_geo_point_values(df, logger)
    logger.info("Handling missing geo point coordinates... COMPLETE")                                                                                                    

    # remove repetitive columns
    df.drop(columns=['country_region', 'province_state', 'admin2', 'lat', 'long', 'prep_flow_runtime'], inplace=True)           
    logger.info(f"Dropped repetitive columns: {initial_columns - df.shape[1]} column(s) removed")
    
    # rename some columns
    df.rename(columns={"location": "geo_location", "combined_key": "location"}, inplace=True)                                   
    logger.info(f"Renamed columns")

    # verify 'date' column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # verify 'case_type' column
    if not set(df['case_type'].unique()).issubset({'Confirmed', 'Deaths'}):                                                     
        logger.warning("Unexpected values found in 'case_type' column")

    return df