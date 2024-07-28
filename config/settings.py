import os

from dotenv import load_dotenv
load_dotenv()

# data.world settings
DW_AUTH_TOKEN = os.getenv('DW_AUTH_TOKEN')
DATASET_KEY = os.getenv('DATASET_KEY')
DATASET_TABLE_NAME = os.getenv('DATASET_TABLE_NAME')

# db settings
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

# aws settings
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# file paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw_data.csv')
PREPROCESSED_CSV_FILE_PATH = os.path.join(PROJECT_ROOT, 'data', 'preprocessed_raw_data.csv')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'extracted_data.csv')
TRANSFORMED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'transformed_data.csv')