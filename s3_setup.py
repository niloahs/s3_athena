import time
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import json
import uuid

# Configuration Constants
REGION_NAME = 'us-east-1'

# Generating unique bucket names to avoid conflicts
DATA_BUCKET_NAME = f'medical-images-mekhailm-{uuid.uuid4()}'
ATHENA_OUTPUT_BUCKET = f'athena-query-results-{uuid.uuid4()}'

# Initialize Boto3 Clients and Resources
config = Config(
    region_name=REGION_NAME,
    signature_version='s3v4',
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)
s3_client = boto3.client('s3', config=config)
s3_resource = boto3.resource('s3', config=config)

def create_bucket(bucket_name):
    """Creates a new S3 bucket."""
    try:
        if REGION_NAME == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': REGION_NAME}
            )
        print(f"Bucket '{bucket_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{bucket_name}' already exists and is owned by you.")
        else:
            print(f"Error creating bucket '{bucket_name}': {e}")
            raise

def delete_bucket(bucket_name):
    """Deletes all objects (including versions) and the specified S3 bucket."""
    try:
        bucket = s3_resource.Bucket(bucket_name)
        # Delete all object versions
        bucket.object_versions.delete()
        # Delete the bucket
        bucket.delete()
        print(f"Bucket '{bucket_name}' deleted successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' does not exist. Skipping deletion.")
        else:
            print(f"Error deleting bucket '{bucket_name}': {e}")
            raise

def upload_file(file_name, bucket_name, object_name=None):
    """Uploads a file to the specified S3 bucket."""
    if object_name is None:
        object_name = file_name
    try:
        content_type = 'image/jpeg' if file_name.lower().endswith('.jpg') else 'text/csv'
        s3_client.upload_file(
            Filename=file_name,
            Bucket=bucket_name,
            Key=object_name,
            ExtraArgs={'ContentType': content_type}
        )
        print(f"File '{file_name}' uploaded successfully as '{object_name}'.")
    except ClientError as e:
        print(f"Error uploading file '{file_name}': {e}")
        raise

def enable_versioning(bucket_name):
    """Enables versioning on the specified S3 bucket."""
    try:
        bucket_versioning = s3_resource.BucketVersioning(bucket_name)
        bucket_versioning.enable()
        print(f"Versioning enabled on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error enabling versioning on bucket '{bucket_name}': {e}")
        raise

def set_lifecycle_policy(bucket_name):
    """Sets a lifecycle policy to transition objects to Glacier after 30 days and expire after 365 days."""
    lifecycle_configuration = {
        'Rules': [
            {
                'ID': 'Transition to Glacier after 30 days',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'GLACIER'
                    }
                ],
                'Expiration': {'Days': 365}  # Expire objects after a year
            }
        ]
    }
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print(f"Lifecycle policy set on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error setting lifecycle policy on bucket '{bucket_name}': {e}")
        raise

def set_bucket_policy(data_bucket, athena_output_bucket):
    """Sets a bucket policy to allow Athena to access the data bucket and write to the Athena output bucket."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAthenaAccessToDataBucket",
                "Effect": "Allow",
                "Principal": {
                    "Service": "athena.amazonaws.com"
                },
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{data_bucket}",
                    f"arn:aws:s3:::{data_bucket}/*"
                ]
            }
        ]
    }
    try:
        s3_client.put_bucket_policy(
            Bucket=data_bucket,
            Policy=json.dumps(policy)
        )
        print(f"Bucket policy set on '{data_bucket}' to allow Athena access.")
    except ClientError as e:
        print(f"Error setting bucket policy on '{data_bucket}': {e}")
        raise

    # Set policy for Athena output bucket
    output_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAthenaAccessToAthenaOutputBucket",
                "Effect": "Allow",
                "Principal": {
                    "Service": "athena.amazonaws.com"
                },
                "Action": [
                    "s3:PutObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{athena_output_bucket}",
                    f"arn:aws:s3:::{athena_output_bucket}/*"
                ]
            }
        ]
    }
    try:
        s3_client.put_bucket_policy(
            Bucket=athena_output_bucket,
            Policy=json.dumps(output_policy)
        )
        print(f"Bucket policy set on '{athena_output_bucket}' to allow Athena access.")
    except ClientError as e:
        print(f"Error setting bucket policy on '{athena_output_bucket}': {e}")
        raise

def list_bucket_contents(bucket_name):
    """Lists all objects in the specified S3 bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print(f"Contents of bucket '{bucket_name}':")
            for obj in response['Contents']:
                print(f"  - Object: {obj['Key']}, Size: {obj['Size']} bytes")
        else:
            print(f"No objects found in bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error listing contents of bucket '{bucket_name}': {e}")
        raise

def read_csv_contents(bucket_name, file_name):
    """Reads and prints the contents of a CSV file in S3."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        print(f"Contents of '{file_name}':\n{content}")
    except ClientError as e:
        print(f"Error reading file '{file_name}' from bucket '{bucket_name}': {e}")
        raise

def check_object_storage_class(bucket_name, file_name):
    """Checks and prints the storage class of a specific object."""
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
        storage_class = response.get('StorageClass', 'STANDARD')
        print(f"Storage Class of '{file_name}': {storage_class}")
    except ClientError as e:
        print(f"Error checking storage class of '{file_name}': {e}")
        raise

def verify_file_upload(bucket_name, file_name):
    """Verifies if a file exists in the specified S3 bucket."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        print(f"File '{file_name}' exists in bucket '{bucket_name}'.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File '{file_name}' does NOT exist in bucket '{bucket_name}'.")
        else:
            print(f"Error verifying file '{file_name}': {e}")
            raise

def list_object_versions(bucket_name, object_name):
    """Lists all versions of a specific object in the S3 bucket."""
    try:
        response = s3_client.list_object_versions(
            Bucket=bucket_name,
            Prefix=object_name
        )
        versions = response.get('Versions', [])
        print(f"Versions of '{object_name}':")
        for version in versions:
            print(f"  Version ID: {version['VersionId']}, Last Modified: {version['LastModified']}")
    except ClientError as e:
        print(f"Error listing versions of '{object_name}': {e}")
        raise

def main():
    """Main function to orchestrate S3 operations."""
    try:
        print("=== S3 Setup Script Started ===\n")
        
        # Step 1: Create Athena Output Bucket if it doesn't exist
        print(f"Creating Athena Output Bucket '{ATHENA_OUTPUT_BUCKET}'...")
        create_bucket(ATHENA_OUTPUT_BUCKET)
        time.sleep(2)
        
        # Step 2: Create Data Bucket
        print(f"Creating Data Bucket '{DATA_BUCKET_NAME}'...")
        create_bucket(DATA_BUCKET_NAME)
        time.sleep(2)
        
        # Step 3: Upload sample files
        print(f"Uploading 'sample_image.jpg' and 'patient_data.csv' to '{DATA_BUCKET_NAME}'...")
        upload_file('sample_image.jpg', DATA_BUCKET_NAME)
        upload_file('patient_data.csv', DATA_BUCKET_NAME)
        time.sleep(2)
        
        # Step 4: Enable versioning
        print(f"Enabling versioning on bucket '{DATA_BUCKET_NAME}'...")
        enable_versioning(DATA_BUCKET_NAME)
        time.sleep(2)
        
        # Step 5: Set lifecycle policy
        print(f"Setting lifecycle policy on bucket '{DATA_BUCKET_NAME}'...")
        set_lifecycle_policy(DATA_BUCKET_NAME)
        time.sleep(2)
        
        # Step 6: Set bucket policy to allow Athena access
        print(f"Setting bucket policies to allow Athena access...")
        set_bucket_policy(DATA_BUCKET_NAME, ATHENA_OUTPUT_BUCKET)
        time.sleep(2)

        config_data = {
            "data_bucket": DATA_BUCKET_NAME,
            "athena_output_bucket": ATHENA_OUTPUT_BUCKET
        }

        with open('config.json', 'w') as config_file:
            json.dump(config_data, config_file)

        print("Bucket names have been saved to 'config.json'.")
        
        # Step 7: List bucket contents
        print(f"Listing contents of bucket '{DATA_BUCKET_NAME}'...")
        list_bucket_contents(DATA_BUCKET_NAME)
        print()
        
        # Step 8: Read and print CSV contents
        print(f"Reading contents of 'patient_data.csv' from bucket '{DATA_BUCKET_NAME}'...")
        read_csv_contents(DATA_BUCKET_NAME, 'patient_data.csv')
        print()
        
        # Step 9: Verify file uploads
        print(f"Verifying existence of 'patient_data.csv' in bucket '{DATA_BUCKET_NAME}'...")
        verify_file_upload(DATA_BUCKET_NAME, 'patient_data.csv')
        print()
        
        # Step 10: Check object storage class
        print(f"Checking storage class of 'patient_data.csv' in bucket '{DATA_BUCKET_NAME}'...")
        check_object_storage_class(DATA_BUCKET_NAME, 'patient_data.csv')
        print("\n=== S3 Setup Script Completed ===")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

