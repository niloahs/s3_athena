import time
import boto3
import os
from botocore.exceptions import ClientError

# Initialize Boto3 Athena and S3 Clients
athena_client = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3')

ATHENA_DATABASE = os.getenv('ATHENA_DATABASE')
ATHENA_OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')

def run_s3_select_query(bucket_name, object_key, query):
    """Runs an S3 SELECT query using Athena."""
    athena_query = f"SELECT * FROM s3object s WHERE {query}"
    full_query = f"""
    SELECT * FROM TABLE(
        SPECTRUM.QUERY_S3OBJECT(
            's3://{bucket_name}/{object_key}',
            '{athena_query}'
        )
    )
    """
    return run_athena_query(full_query)

def run_athena_query(query, database_name=ATHENA_DATABASE):
    """Executes a given SQL query in Athena and returns the results."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/'}
        )
        query_execution_id = response['QueryExecutionId']
        print(f"Executing Athena query: {query_execution_id}")
        wait_for_query_to_complete(query_execution_id)
        return get_query_results(query_execution_id)
    except ClientError as e:
        print(f"Error executing Athena query: {e}")
        raise

def wait_for_query_to_complete(query_execution_id, sleep_time=2):
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

def get_query_results(query_execution_id):
    """Retrieves and prints the results of a completed Athena query."""
    try:
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        print("Athena Query Results:")
        for row in result['ResultSet']['Rows']:
            row_data = [col.get('VarCharValue', '') for col in row['Data']]
            print(row_data)
    except ClientError as e:
        print(f"Error retrieving results for query '{query_execution_id}': {e}")
        raise

def create_athena_database(database_name):
    """Creates a new database in Athena."""
    run_athena_query(f"CREATE DATABASE IF NOT EXISTS {database_name}")

def create_athena_table(database_name, table_name, data_bucket):
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
        name STRING,
        age INT,
        condition STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'skip.header.line.count' = '1'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{data_bucket}/'
    TBLPROPERTIES ('has_encrypted_data'='false');
    """
    run_athena_query(query, database_name)

def describe_table(database_name, table_name):
    run_athena_query(f"DESCRIBE {database_name}.{table_name}")

def list_s3_contents(bucket_name):
    print(f"Listing contents of S3 bucket: {bucket_name}")
    response = s3.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        print(f"- {obj['Key']}")