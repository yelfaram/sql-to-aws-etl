import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError
import logging

from dotenv import load_dotenv
load_dotenv()

# Set up config variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
s3_bucket_name = os.getenv('BUCKET_NAME')

if not all([aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name]):
    logging.error("AWS connection details not found in environment variables")
    raise ValueError("AWS connection details not found in environment variables")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/load_data.log', mode='w'),
        logging.StreamHandler()
    ]
)

logging.info("Starting data load to s3 process...")

# connect to aws s3, create bucket then upload file
def upload_to_s3(local_file_path, s3_bucket_name, s3_file_name):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        # create the bucket if it doesn't exist
        s3_client.create_bucket(Bucket=s3_bucket_name, CreateBucketConfiguration={'LocationConstraint': aws_region})

        # upload the file
        with open(local_file_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_bucket_name, s3_file_name)
        logging.info(f"Data successfully uploaded to {s3_bucket_name}/{s3_file_name}")
    except ClientError as e:
        logging.error(f"Error occurred while uploading data to S3: {e}")
        raise

# upload file to s3
s3_file_name = 'transformed_data.csv'
local_file_path = 'data/transformed_data.csv'
upload_to_s3(local_file_path, s3_bucket_name, s3_file_name)

print("Data upload to S3 complete.")