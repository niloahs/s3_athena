import json
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from utils.helpers import load_config, save_config

REGION_NAME = 'us-east-1'


def get_s3_clients(region=REGION_NAME):
    """Initialize and return S3 client and resource."""
    config = Config(
        region_name=region,
        signature_version='s3v4',
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
    s3_client = boto3.client('s3', config=config)
    s3_resource = boto3.resource('s3', config=config)
    return s3_client, s3_resource


def create_bucket(bucket_name, region=REGION_NAME):
    """Creates a new S3 bucket if it doesn't already exist."""
    s3_client, _ = get_s3_clients(region)
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            if region == 'us-east-1':
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Error checking bucket '{bucket_name}': {e}")
            raise


def delete_bucket(bucket_name, region=None):
    """Delete a single S3 bucket"""
    s3_client = boto3.client('s3', region_name=region)
    try:
        # First, delete all objects and object versions in the bucket
        bucket = boto3.resource('s3', region_name=region).Bucket(bucket_name)
        bucket.object_versions.delete()

        # Afterwards, delete the bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        return f"Bucket '{bucket_name}' has been deleted."
    except ClientError as e:
        return f"Error deleting bucket '{bucket_name}': {str(e)}"


def delete_multiple_buckets(bucket_names, region=None):
    """Delete multiple S3 buckets"""
    results = []
    for bucket_name in bucket_names:
        result = delete_bucket(bucket_name, region)
        results.append(result)
    return results


def update_config_after_deletion(bucket_name):
    """Update the config file after deleting a bucket if it was one of the main buckets."""
    config_data = load_config()
    if bucket_name in config_data.values():
        for key, value in config_data.items():
            if value == bucket_name:
                config_data[key] = None
        save_config(config_data)
        return "Config file updated."
    return None


def upload_file(file_path, bucket_name, object_name=None):
    """Uploads a file to the specified S3 bucket."""
    s3_client, _ = get_s3_clients()
    file_name = os.path.basename(file_path)
    if object_name is None:
        object_name = file_name
    try:
        if file_name.lower().endswith('.jpg') or file_name.lower().endswith('.jpeg'):
            content_type = 'image/jpeg'
        elif file_name.lower().endswith('.csv'):
            content_type = 'text/csv'
        else:
            content_type = 'binary/octet-stream'
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=object_name,
            ExtraArgs={'ContentType': content_type}
        )
        print(f"File '{file_name}' uploaded successfully as '{object_name}'.")
    except ClientError as e:
        print(f"Error uploading file '{file_name}': {e}")
        raise


def enable_versioning(bucket_name, region=REGION_NAME):
    """Enables versioning on the specified S3 bucket."""
    s3_resource = boto3.resource('s3', region_name=region)
    try:
        bucket_versioning = s3_resource.BucketVersioning(bucket_name)
        bucket_versioning.enable()
        print(f"Versioning enabled on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error enabling versioning on bucket '{bucket_name}': {e}")
        raise


def set_lifecycle_policy(bucket_name, region=REGION_NAME):
    """Sets a lifecycle policy with multiple transitions."""
    s3_client, _ = get_s3_clients(region)
    lifecycle_configuration = {
        'Rules': [
            {
                'ID': 'Move to Intelligent-Tiering after 30 days',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'INTELLIGENT_TIERING'
                    },
                    {
                        'Days': 90,
                        'StorageClass': 'GLACIER'
                    },
                    {
                        'Days': 180,
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ],
            }
        ]
    }
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print(f"Enhanced lifecycle policy set on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error setting lifecycle policy on bucket '{bucket_name}': {e}")
        raise


def set_bucket_policy(data_bucket, region=REGION_NAME):
    """Sets bucket policy to allow Athena access to the data bucket."""
    s3_client, _ = get_s3_clients(region)
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


def list_bucket_contents(bucket_name, region=REGION_NAME):
    """Lists all objects in the specified S3 bucket."""
    s3_client, _ = get_s3_clients(region)
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


def list_buckets(nodate=False, collection=False):
    """List all S3 buckets in the AWS account"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_buckets()
        buckets = response['Buckets']
        if buckets:
            if collection:
                bucket_names = [bucket['Name'] for bucket in buckets]
                return ' '.join(bucket_names)
            else:
                result = ["Existing S3 buckets:"]
                for bucket in buckets:
                    if nodate:
                        result.append(f"- {bucket['Name']}")
                    else:
                        result.append(f"- {bucket['Name']} (Created: {bucket['CreationDate']})")
                return '\n'.join(result)
        else:
            return "No S3 buckets found in the account."
    except ClientError as e:
        return f"Error listing buckets: {str(e)}"


def read_csv_contents(bucket_name, file_name, region=REGION_NAME):
    """Reads and prints the contents of a CSV file in S3."""
    s3_client, _ = get_s3_clients(region)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')
        print(f"Contents of '{file_name}':\n{content}")
    except ClientError as e:
        print(f"Error reading file '{file_name}' from bucket '{bucket_name}': {e}")
        raise


def check_object_storage_class(bucket_name, file_name, region=REGION_NAME):
    """Checks and prints the storage class of a specific object."""
    s3_client, _ = get_s3_clients(region)
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
        storage_class = response.get('StorageClass', 'STANDARD')
        print(f"Storage Class of '{file_name}': {storage_class}")
    except ClientError as e:
        print(f"Error checking storage class of '{file_name}': {e}")
        raise


def enable_encryption(bucket_name, region=REGION_NAME):
    """Enable server-side encryption for the bucket during setup."""
    s3_client, _ = get_s3_clients(region)
    try:
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}
                ]
            }
        )
        print(f"Enabled server-side encryption for '{bucket_name}'.")
    except ClientError as e:
        print(f"Error enabling encryption for '{bucket_name}': {e}")
        raise
