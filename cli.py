import time
import os
import click
import json
import boto3
from botocore.exceptions import ClientError
import uuid

# Import functions from s3_setup
from s3_setup import (
    create_bucket, delete_bucket, upload_file, enable_versioning,
    set_lifecycle_policy, set_bucket_policy, list_bucket_contents,
    read_csv_contents, check_object_storage_class, verify_file_upload,
    REGION_NAME
)

# Import functions from athena_query
from athena_query import (
    create_athena_database, create_athena_table, describe_table,
    run_athena_query, list_s3_contents
)

# Configuration Constants
ATHENA_DATABASE = 'medical_data'
ATHENA_TABLE = 'patient_data'

# Generate unique bucket names
DATA_BUCKET_NAME = f'medical-images-mekhailm-{uuid.uuid4()}'
ATHENA_OUTPUT_BUCKET = f'athena-query-results-{uuid.uuid4()}'

@click.group()
def cli():
    """Medical Image Storage CLI"""
    pass

@cli.command()
@click.option('--region', default=REGION_NAME, help='AWS region')
def setup(region):
    """Set up S3 buckets and Athena"""
    click.echo("Setting up S3 buckets and Athena...")
    create_bucket(ATHENA_OUTPUT_BUCKET)
    create_bucket(DATA_BUCKET_NAME)
    upload_file('sample_image.jpg', DATA_BUCKET_NAME)
    upload_file('patient_data.csv', DATA_BUCKET_NAME)
    enable_versioning(DATA_BUCKET_NAME)
    set_lifecycle_policy(DATA_BUCKET_NAME)
    set_bucket_policy(DATA_BUCKET_NAME, ATHENA_OUTPUT_BUCKET)

    config_data = {
        "data_bucket": DATA_BUCKET_NAME,
        "athena_output_bucket": ATHENA_OUTPUT_BUCKET
    }
    with open('config.json', 'w') as config_file:
        json.dump(config_data, config_file)

    click.echo("Setup complete. Bucket names saved to 'config.json'.")

@cli.command()
def list_contents():
    """List contents of the data bucket"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    list_bucket_contents(config_data['data_bucket'])

@cli.command()
@click.argument('filename')
def upload(filename):
    """Upload a file to the data bucket"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    upload_file(filename, config_data['data_bucket'])

@click.command()
def setup_athena():
    """Set up Athena database and table"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
        create_athena_database(ATHENA_DATABASE)
        create_athena_table(ATHENA_DATABASE, ATHENA_TABLE, config_data['data_bucket'])
        describe_table(ATHENA_DATABASE, ATHENA_TABLE)

@cli.command()
@click.argument('query')
def run_query(query):
    """Run an Athena query"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    run_athena_query(query, ATHENA_DATABASE, config_data['athena_output_bucket'])

@cli.command()
@click.argument('filename')
def delete_file(filename):
    """Delete a file from the data bucket"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    s3_client = boto3.client('s3')
    try:
        s3_client.delete_object(Bucket=config_data['data_bucket'], Key=filename)
        click.echo(f"Deleted {filename} from {config_data['data_bucket']}")
    except ClientError as e:
        click.echo(f"Error deleting {filename}: {e}")

@cli.command()
@click.argument('filename')
@click.argument('version_id')
def restore_version(filename, version_id):
    """Restore a specific version of a file"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

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
            s3_client.delete_object(Bucket=bucket_name, Key=filename, VersionId=delete_marker['VersionId'])
            click.echo(f"Removed delete marker for {filename}")

        # Now copy the specified version to make it the latest
        copy_source = {
            'Bucket': bucket_name,
            'Key': filename,
            'VersionId': version_id
        }
        s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=filename)
        click.echo(f"Restored version {version_id} of {filename}")

    except ClientError as e:
        click.echo(f"Error restoring version {version_id} of {filename}: {str(e)}")

@cli.command()
@click.argument('filename')
def list_versions(filename):
    """List all versions of a specific file"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']

    try:
        versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=filename)

        click.echo(f"Versions of {filename}:")
        for version in versions.get('Versions', []):
            click.echo(f"Version ID: {version['VersionId']}, Last Modified: {version['LastModified']}")

        for marker in versions.get('DeleteMarkers', []):
            click.echo(f"Delete Marker: {marker['VersionId']}, Last Modified: {marker['LastModified']}")

    except ClientError as e:
        click.echo(f"Error listing versions of {filename}: {str(e)}")

@cli.command()
@click.argument('filename')
@click.argument('storage_class', type=click.Choice(['STANDARD', 'STANDARD_IA', 'ONEZONE_IA', 'GLACIER', 'DEEP_ARCHIVE']))
def set_storage_class(filename, storage_class):
    """Set the storage class for a specific file"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']

    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': filename},
            Key=filename,
            StorageClass=storage_class
        )
        click.echo(f"Set storage class of {filename} to {storage_class}")
    except ClientError as e:
        click.echo(f"Error setting storage class for {filename}: {str(e)}")

@cli.command()
def enable_encryption():
    """Enable server-side encryption for the bucket"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']

    try:
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}
                ]
            }
        )
        click.echo(f"Enabled server-side encryption for {bucket_name}")
    except ClientError as e:
        click.echo(f"Error enabling encryption: {str(e)}")

@cli.command()
@click.argument('filename')
@click.option('--expiration', default=3600, help='Expiration time in seconds')
def generate_presigned_url(filename, expiration):
    """Generate a presigned URL for a file"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']

    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': filename},
            ExpiresIn=expiration
        )
        click.echo(f"Presigned URL for {filename} (expires in {expiration} seconds):\n{url}")
    except ClientError as e:
        click.echo(f"Error generating presigned URL: {str(e)}")

@cli.command()
@click.argument('filename')
@click.option('--iterations', default=10, help='Number of iterations for each operation')
def performance_test(filename, iterations):
    """Run performance tests on various S3 operations"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    s3_client = boto3.client('s3')
    bucket_name = config_data['data_bucket']

    def measure_operation(operation, *args):
        start_time = time.time()
        for _ in range(iterations):
            operation(*args)
        end_time = time.time()
        return (end_time - start_time) / iterations

    upload_time = measure_operation(s3_client.upload_file, filename, bucket_name, filename)
    download_time = measure_operation(s3_client.download_file, bucket_name, filename, f"downloaded_{filename}")
    delete_time = measure_operation(s3_client.delete_object, Bucket=bucket_name, Key=filename)

    click.echo(f"Average upload time: {upload_time:.4f} seconds")
    click.echo(f"Average download time: {download_time:.4f} seconds")
    click.echo(f"Average delete time: {delete_time:.4f} seconds")

@cli.command()
@click.argument('expression')
def s3_select(expression):
    """Run an S3 Select query on the CSV file"""
    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)
    bucket_name = config_data['data_bucket']
    file_name = 'patient_data.csv'

    s3_client = boto3.client('s3')

    try:
        response = s3_client.select_object_content(
            Bucket=bucket_name,
            Key=file_name,
            ExpressionType='SQL',
            Expression=expression,
            InputSerialization={'CSV': {"FileHeaderInfo": "USE"}},
            OutputSerialization={'JSON': {}},
        )

        for event in response['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                for record in records.splitlines():
                    click.echo(json.loads(record))

    except ClientError as e:
        click.echo(f"Error during S3 Select: {e}")

if __name__ == '__main__':
    cli()