import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# AWS credentials from environment
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Paths
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
LISTINGS_FILE = PROCESSED_DIR / "listings_with_transit.csv"

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def upload_file_to_s3(file_path, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_path.name

    # Upload the file
    try:
        s3.upload_file(str(file_path), bucket_name, object_name)
        print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

if __name__ == "__main__":
    upload_file_to_s3(LISTINGS_FILE, BUCKET_NAME)