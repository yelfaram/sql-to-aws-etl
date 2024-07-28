import os
import boto3

from config import settings
from utils.logging import setup_logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logger = setup_logging('logs/load_data.log', 'load_data')


# connect to aws s3, create bucket then upload file
def upload_to_s3(local_file_path, s3_bucket_name, s3_file_name):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )

        # create the bucket (idempotent)
        s3_client.create_bucket(
            Bucket=s3_bucket_name, 
            CreateBucketConfiguration={'LocationConstraint': settings.AWS_REGION}
        )

        # upload the file
        with open(local_file_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_bucket_name, s3_file_name)
        logger.info(f"Data successfully uploaded to {s3_bucket_name}/{s3_file_name}")
    except ClientError as e:
        logger.error(f"Error occurred while uploading data to S3: {e}")
        raise

# upload file to s3
def main():
    logger.info("Starting data load to s3 process...")
    s3_file_name = os.path.basename(settings.TRANSFORMED_DATA_PATH)
    upload_to_s3(settings.TRANSFORMED_DATA_PATH, settings.BUCKET_NAME, s3_file_name)
    logger.info("Data upload to S3 complete.")

if __name__ == "__main__":
    main()