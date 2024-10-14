"""
Author: Mark Mekhail
Date: 10/13/24
Description: This module provides functions to interact with AWS S3.
It includes functionalities to create, delete, and manage S3 buckets and objects,
with a focus on healthcare data management and compliance with PHIPA regulations.
"""

import json
import os
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from utils.helpers import load_config, save_config

REGION_NAME = 'us-east-1'


def get_s3_clients(region=REGION_NAME):
    """
    Initialize and return S3 client and resource.

    Args:
        region (str): The AWS region to connect to. Defaults to 'us-east-1'.

    Returns:
        tuple: A tuple containing the S3 client and S3 resource objects.
    """
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
    """
    Creates a new S3 bucket if it doesn't already exist.

    Args:
        bucket_name (str): The name of the bucket to create.
        region (str): The AWS region for the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue with bucket creation or checking.
    """
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
    """
    Delete a single S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to delete.
        region (str, optional): The AWS region of the bucket.

    Returns:
        str: A message indicating the result of the deletion attempt.
    """
    s3_client = boto3.client('s3', region_name=region)
    try:
        bucket = boto3.resource('s3', region_name=region).Bucket(bucket_name)
        bucket.object_versions.delete()
        s3_client.delete_bucket(Bucket=bucket_name)
        return f"Bucket '{bucket_name}' has been deleted."
    except ClientError as e:
        return f"Error deleting bucket '{bucket_name}': {str(e)}"


def delete_multiple_buckets(bucket_names, region=None):
    """
    Delete multiple S3 buckets.

    Args:
        bucket_names (list): A list of bucket names to delete.
        region (str, optional): The AWS region of the buckets.

    Returns:
        list: A list of messages indicating the result of each deletion attempt.
    """
    results = []
    for bucket_name in bucket_names:
        result = delete_bucket(bucket_name, region)
        results.append(result)
    return results


def update_config_after_deletion(bucket_name):
    """
    Update the config file after deleting a bucket if it was one of the main buckets.

    Args:
        bucket_name (str): The name of the deleted bucket.

    Returns:
        str or None: A message if the config was updated, None otherwise.
    """
    config_data = load_config()
    if bucket_name in config_data.values():
        for key, value in config_data.items():
            if value == bucket_name:
                config_data[key] = None
        save_config(config_data)
        return "Config file updated."
    return None


def upload_file(file_path, bucket_name, object_name=None):
    """
    Uploads a file to the specified S3 bucket.

    Args:
        file_path (str): The local path to the file to be uploaded.
        bucket_name (str): The name of the destination S3 bucket.
        object_name (str, optional): The S3 object name. If not specified, the file name is used.

    Raises:
        ClientError: If there's an issue with the file upload.
    """
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
    """
    Enables versioning on the specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to enable versioning on.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue enabling versioning.
    """
    s3_resource = boto3.resource('s3', region_name=region)
    try:
        bucket_versioning = s3_resource.BucketVersioning(bucket_name)
        bucket_versioning.enable()
        print(f"Versioning enabled on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error enabling versioning on bucket '{bucket_name}': {e}")
        raise


def set_lifecycle_policy(bucket_name, region=REGION_NAME):
    """
    Sets a comprehensive lifecycle policy for healthcare data.

    Args:
        bucket_name (str): The name of the bucket to set the lifecycle policy on.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue setting the lifecycle policy.
    """
    s3_client, _ = get_s3_clients(region)
    lifecycle_configuration = {
        'Rules': [
            {
                'ID': 'Healthcare data lifecycle policy',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Transitions': [
                    {
                        'Days': 60,
                        'StorageClass': 'INTELLIGENT_TIERING'
                    },
                    {
                        'Days': 365,
                        'StorageClass': 'GLACIER'
                    },
                    {
                        'Days': 2555,  # ~7 years
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ],
                'NoncurrentVersionTransitions': [
                    {
                        'NoncurrentDays': 60,
                        'StorageClass': 'INTELLIGENT_TIERING'
                    },
                    {
                        'NoncurrentDays': 365,
                        'StorageClass': 'GLACIER'
                    }
                ],
                'NoncurrentVersionExpiration': {
                    'NoncurrentDays': 2555  # ~7 years
                },
                'AbortIncompleteMultipartUpload': {
                    'DaysAfterInitiation': 7
                }
            }
        ]
    }
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print(f"Comprehensive lifecycle policy set on bucket '{bucket_name}'.")
    except ClientError as e:
        print(f"Error setting lifecycle policy on bucket '{bucket_name}': {e}")
        raise


def set_bucket_policy(data_bucket, region=REGION_NAME):
    """
    Sets bucket policy to allow Athena access to the data bucket.

    Args:
        data_bucket (str): The name of the data bucket to set the policy on.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue setting the bucket policy.
    """
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
    """
    Lists all objects in the specified S3 bucket.

    Args:
        bucket_name (str): The name of the bucket to list contents from.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue listing the bucket contents.
    """
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
    """
    List all S3 buckets in the AWS account.

    Args:
        nodate (bool): If True, omit creation dates from the output.
        collection (bool): If True, return bucket names as a space-separated string.

    Returns:
        str: A formatted string containing the list of buckets or an error message.
    """
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


def enable_encryption(bucket_name, region=REGION_NAME):
    """
    Enable server-side encryption for the bucket during setup.

    Args:
        bucket_name (str): The name of the bucket to enable encryption on.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue enabling encryption on the bucket.
    """
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


def download_file(bucket_name, object_name, file_path, region='us-east-1'):
    """
    Downloads a file from the specified S3 bucket and saves it with a 'dl' prefix and the current date.

    Args:
        bucket_name (str): The name of the S3 bucket to download from.
        object_name (str): The name of the object in the S3 bucket to download.
        file_path (str): The local directory path to save the downloaded file.
        region (str): The AWS region of the bucket. Defaults to 'us-east-1'.

    Raises:
        ClientError: If there's an issue downloading the file from S3.
    """
    s3_client = boto3.client('s3', region_name=region)
    try:
        file_name = os.path.basename(object_name)
        current_date = datetime.now().strftime('%Y%m%d')
        new_file_name = f"dl_{current_date}_{file_name}"
        new_file_path = os.path.join(file_path, new_file_name)

        s3_client.download_file(bucket_name, object_name, new_file_path)
        print(f"File '{object_name}' downloaded successfully to '{new_file_path}'.")
    except ClientError as e:
        print(f"Error downloading file '{object_name}': {e}")
        raise
