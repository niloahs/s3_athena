import time

import boto3
from botocore.exceptions import ClientError

from utils.helpers import load_config, generate_filename


def get_athena_client(region='us-east-1'):
    """Initialize and return Athena client."""
    return boto3.client('athena', region_name=region)


def get_s3_client(region='us-east-1'):
    """Initialize and return S3 client."""
    return boto3.client('s3', region_name=region)


def run_athena_query(query, database_name, athena_output_bucket, region='us-east-1'):
    """Executes a given SQL query in Athena and stores the results if applicable."""
    athena_client = get_athena_client(region)
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': f's3://{athena_output_bucket}/'}
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Executing Athena query: {query_execution_id}")

        wait_for_query_to_complete(query_execution_id, athena_client)

        # Check if the query is a SELECT statement
        if query.strip().lower().startswith('select'):
            query_results = get_query_results(query_execution_id, athena_client)

            if query_results:  # Only store results if the query returns data
                file_name = generate_filename(query)
                store_query_results(query_execution_id, athena_output_bucket, file_name)
                print(f"Results stored with filename: {file_name}.csv")
            else:
                print("No data returned. Query results not stored.")
        else:
            print("Non-SELECT query executed successfully. No results to store.")

    except ClientError as e:
        print(f"Error executing Athena query: {e}")
        raise


def wait_for_query_to_complete(query_execution_id, athena_client, sleep_time=2):
    """Waits for the Athena query to complete."""
    while True:
        try:
            status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                print(f"Athena query '{query_execution_id}' ended with state: {state}.")
                if 'StateChangeReason' in status['QueryExecution']['Status']:
                    print(f"Reason: {status['QueryExecution']['Status']['StateChangeReason']}")
                break
            print(f"Waiting for Athena query '{query_execution_id}' to complete...")
            time.sleep(sleep_time)
        except ClientError as e:
            print(f"Error while checking status of query '{query_execution_id}': {e}")
            raise


def get_query_results(query_execution_id, athena_client):
    """Retrieves and returns the results of a completed Athena query."""
    try:
        paginator = athena_client.get_paginator('get_query_results')
        pages = paginator.paginate(QueryExecutionId=query_execution_id)
        results = []
        print("Athena Query Results:")
        for page in pages:
            for row in page['ResultSet']['Rows']:
                row_data = [col.get('VarCharValue', '') for col in row['Data']]
                results.append(row_data)
                print(row_data)
        return results
    except ClientError as e:
        print(f"Error retrieving results for query '{query_execution_id}': {e}")
        raise


def store_query_results(query_execution_id, athena_output_bucket, clean_name):
    """Store the Athena query results with a cleaner name."""
    s3_client = get_s3_client()
    copy_source = f"{athena_output_bucket}/{query_execution_id}.csv"

    try:
        # Copy the query results to a new name (cleaner format)
        s3_client.copy_object(
            Bucket=athena_output_bucket,
            CopySource=copy_source,
            Key=f"{clean_name}.csv"
        )
    except ClientError as e:
        print(f"Error storing query results: {e}")
        raise


def create_athena_database_and_table(database_name, table_name, data_bucket, region='us-east-1'):
    """Creates a new database and table in Athena, dropping the table if it already exists."""
    config = load_config()

    # Create database query
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    run_athena_query(create_db_query, database_name, config['athena_output_bucket'], region=region)

    # Drop table if it exists
    drop_table_query = f"DROP TABLE IF EXISTS {database_name}.{table_name}"
    run_athena_query(drop_table_query, database_name, config['athena_output_bucket'], region=region)

    # Create table query
    create_table_query = f"""
    CREATE EXTERNAL TABLE {database_name}.{table_name} (
        patient_id STRING,
        name STRING,
        age INT,
        gender STRING,
        condition STRING,
        admission_date STRING,
        doctor STRING,
        blood_type STRING,
        weight_kg DOUBLE,
        height_cm DOUBLE
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'skip.header.line.count' = '1'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{data_bucket}/'
    TBLPROPERTIES ('has_encrypted_data'='true')
    """
    run_athena_query(create_table_query, database_name, config['athena_output_bucket'],
                     region=region)


def performance_test_select_query(query, database, output_bucket, iterations=5):
    """Run performance tests on Athena SELECT queries"""
    athena_client = boto3.client('athena')

    total_execution_time = 0
    total_scanned_bytes = 0

    for i in range(iterations):
        start_time = time.time()

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': f's3://{output_bucket}/'}
        )

        query_execution_id = response['QueryExecutionId']

        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        end_time = time.time()
        execution_time = end_time - start_time

        if status == 'SUCCEEDED':
            statistics = query_status['QueryExecution']['Statistics']
            total_execution_time += execution_time
            total_scanned_bytes += statistics['DataScannedInBytes']

    avg_execution_time = total_execution_time / iterations
    avg_scanned_bytes = total_scanned_bytes / iterations

    return {
        'average_execution_time': avg_execution_time,
        'average_scanned_bytes': avg_scanned_bytes
    }