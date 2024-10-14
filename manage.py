"""
Author: Mark Mekhail
Date: 10/12/24
Description: A CLI tool for managing medical data storage using S3 and Athena.
The tool allows users to set up S3 buckets and Athena, upload and download files,
run Athena queries, and more.
"""

import os
import uuid

import boto3
import click
from botocore.exceptions import ClientError

from athena_module import *
from s3_module import *
from utils.helpers import load_config, save_config, get_default_bucket

# Configuration Constants
ATHENA_DATABASE = 'medical_db'
ATHENA_TABLE = 'patient_data'


def generate_bucket_names():
    """
    Generate unique bucket names.

    Returns:
        tuple: A tuple containing the names of the images and data buckets.
    """
    images_bucket = f'medical-images-{uuid.uuid4()}'
    data_bucket = f'patient-data-{uuid.uuid4()}'
    return images_bucket, data_bucket


@click.group()
def cli():
    """Medical Data Storage CLI"""
    pass


@cli.group()
def s3():
    """Commands related to S3 operations"""
    pass


@cli.group()
def athena():
    """Commands related to Athena operations"""
    pass


@s3.command('setup')
@click.option('--region', default=REGION_NAME, help='AWS region')
def s3_setup(region):
    """
    Set up S3 buckets and Athena.

    Args:
        region (str): The AWS region.
    """
    click.echo("Setting up S3 buckets and Athena...")
    images_bucket, data_bucket = generate_bucket_names()

    create_bucket(images_bucket, region)
    enable_versioning(images_bucket, region)

    create_bucket(data_bucket, region)
    enable_versioning(data_bucket, region)

    set_lifecycle_policy(images_bucket, region)
    set_lifecycle_policy(data_bucket, region)

    set_bucket_policy(images_bucket, region)
    set_bucket_policy(data_bucket, region)

    enable_encryption(images_bucket, region)
    enable_encryption(data_bucket, region)

    athena_output_bucket = f'athena-query-results-{uuid.uuid4()}'
    create_bucket(athena_output_bucket, region)
    enable_versioning(athena_output_bucket, region)

    set_lifecycle_policy(athena_output_bucket, region)
    set_bucket_policy(athena_output_bucket, region)
    enable_encryption(athena_output_bucket, region)

    config_data = {
        "images_bucket": images_bucket,
        "data_bucket": data_bucket,
        "athena_output_bucket": athena_output_bucket
    }
    save_config(config_data)

    click.echo("Setup complete. Bucket names saved to 'config.json'.")


@s3.command('delete-bucket')
@click.argument('bucket_names', nargs=-1, type=click.STRING, required=True)
def delete_bucket_command(bucket_names):
    """
    Delete one or more S3 buckets.

    Args:
        bucket_names (tuple): The names of the buckets to delete.
    """
    results = delete_multiple_buckets(bucket_names)
    for result in results:
        click.echo(result)
        if "has been deleted" in result:
            bucket_name = result.split("'")[1]  # Extract bucket name from the success message
            config_update = update_config_after_deletion(bucket_name)
            if config_update:
                click.echo(config_update)


@s3.command('list-buckets')
@click.option('--nodate', is_flag=True, help='List buckets without creation dates')
@click.option('--collection', is_flag=True, help='Output bucket names as a space-separated list')
def s3_list_buckets(nodate, collection):
    """
    List all S3 buckets in the AWS account.

    Args:
        nodate (bool): Flag to list buckets without creation dates.
        collection (bool): Flag to output bucket names as a space-separated list.
    """
    result = list_buckets(nodate, collection)
    click.echo(result)


@s3.command('list-contents')
@click.argument('bucket_name')
def list_contents(bucket_name):
    """
    List contents of the specified bucket.

    Args:
        bucket_name (str): The name of the bucket.
    """
    list_bucket_contents(bucket_name)


@s3.command('upload')
@click.argument('filename')
@click.option('--bucket', help='Specify a bucket to override automatic selection')
def upload(filename, bucket):
    """
    Upload a file to the appropriate bucket.

    Args:
        filename (str): The name of the file to upload.
        bucket (str): The name of the bucket to upload to.
    """
    config_data = load_config()
    if not bucket:
        bucket = get_default_bucket(filename, config_data)

    upload_file(filename, bucket)
    click.echo(f"Uploaded {filename} to {bucket}")


@s3.command('delete-file')
@click.argument('filename')
def delete_file(filename):
    """
    Delete a file from the data bucket (creates a delete marker).

    Args:
        filename (str): The name of the file to delete.
    """
    config_data = load_config()
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket=config_data['data_bucket'], Key=filename)
        click.echo(f"Created delete marker for {filename} in {config_data['data_bucket']}")
    except ClientError as e:
        click.echo(f"Error creating delete marker for {filename}: {e}")


@s3.command('restore-version')
@click.argument('filename')
@click.argument('version_id')
def restore_version(filename, version_id):
    """
    Restore a specific version of a file.

    Args:
        filename (str): The name of the file.
        version_id (str): The version ID to restore.
    """
    config_data = load_config()
    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']
    try:
        # List object versions to check if the specified version exists
        versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=filename)

        # Check if the specified version exists
        version_exists = any(v['VersionId'] == version_id for v in versions.get('Versions', []))
        if not version_exists:
            click.echo(f"Version {version_id} of {filename} does not exist.")
            return

        # Check if there's a delete marker
        delete_marker = next((m for m in versions.get('DeleteMarkers', []) if m['IsLatest']), None)
        if delete_marker:
            # If there's a delete marker, remove it
            s3_client.delete_object(Bucket=bucket_name, Key=filename,
                                    VersionId=delete_marker['VersionId'])
            click.echo(f"Removed delete marker for {filename}")

        # Copy the specified version to make it the latest
        copy_source = {'Bucket': bucket_name, 'Key': filename, 'VersionId': version_id}
        s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=filename)
        click.echo(f"Restored version {version_id} of {filename}")
    except ClientError as e:
        click.echo(f"Error restoring version {version_id} of {filename}: {str(e)}")


@s3.command('list-versions')
@click.argument('filename')
def list_versions(filename):
    """
    List all versions of a specific file.

    Args:
        filename (str): The name of the file.
    """
    config_data = load_config()
    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']
    try:
        versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=filename)
        click.echo(f"Versions of {filename}:")
        for version in versions.get('Versions', []):
            click.echo(
                f"Version ID: {version['VersionId']}, Last Modified: {version['LastModified']}")
        for marker in versions.get('DeleteMarkers', []):
            click.echo(
                f"Delete Marker: {marker['VersionId']}, Last Modified: {marker['LastModified']}")
    except ClientError as e:
        click.echo(f"Error listing versions of {filename}: {e}")


@s3.command('generate-presigned-url')
@click.argument('filename')
@click.option('--expiration', default=3600, help='Expiration time in seconds')
@click.option('--bucket', help='Specify a bucket to override automatic selection')
def generate_presigned_url(filename, expiration, bucket):
    """
    Generate a presigned URL for a file.

    Args:
        filename (str): The name of the file.
        expiration (int): The expiration time in seconds.
        bucket (str): The name of the bucket.
    """
    config_data = load_config()
    s3_client = boto3.client('s3')

    if not bucket:
        bucket = get_default_bucket(filename, config_data)

    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': filename},
            ExpiresIn=expiration
        )
        click.echo(
            f"Presigned URL for {filename} in bucket {bucket} (expires in {expiration} seconds):\n{url}")
    except ClientError as e:
        click.echo(f"Error generating presigned URL: {str(e)}")


@athena.command('performance-test')
@click.argument('query')
@click.option('--iterations', default=5, help='Number of iterations for performance testing')
def athena_performance_test(query, iterations):
    """
    Run performance tests on Athena SELECT queries.

    Args:
        query (str): The SQL query to test.
        iterations (int): The number of times to run the query.
    """
    config_data = load_config()
    results = performance_test_select_query(query, ATHENA_DATABASE,
                                            config_data['athena_output_bucket'], iterations)

    click.echo(f"Query: {query}")
    click.echo(f"Average execution time: {results['average_execution_time']:.2f} seconds")
    click.echo(f"Average data scanned: {results['average_scanned_bytes'] / 1024:.2f} KB")


@athena.command('run-query')
@click.argument('query')
def run_query(query):
    """
    Run an Athena query.

    Args:
        query (str): The SQL query to run.
    """
    config_data = load_config()
    run_athena_query(query, ATHENA_DATABASE, config_data['athena_output_bucket'])
    click.echo(f"Athena query executed.")


@s3.command('download')
@click.argument('filename')
@click.option('--bucket', help='Specify a bucket to override automatic selection')
@click.option('--region', default='us-east-1', help='AWS region')
def download(filename, bucket, region):
    """
    Download a file from the appropriate bucket.

    Args:
        filename (str): The name of the file to download.
        bucket (str): The name of the bucket to download from.
        region (str): The AWS region.
    """
    config_data = load_config()
    if not bucket:
        bucket = get_default_bucket(filename, config_data)

    current_directory = os.getcwd()
    download_file(bucket, filename, current_directory, region)
    click.echo(f"Downloaded {filename} from {bucket}")


@athena.command('setup')
@click.option('--region', default='us-east-1', help='AWS region')
def setup_athena(region):
    """
    Set up Athena database and table.

    Args:
        region (str): The AWS region.
    """
    config_data = load_config()
    data_bucket = config_data['data_bucket']

    click.echo("Setting up Athena database and table...")
    create_athena_database_and_table(ATHENA_DATABASE, ATHENA_TABLE, data_bucket, region)
    click.echo(f"Athena database '{ATHENA_DATABASE}' and table '{ATHENA_TABLE}' have been set up.")


if __name__ == '__main__':
    cli()
