import json
import time
from athena_query import (
    create_athena_database,
    create_athena_table,
    describe_table,
    run_athena_query,
    list_s3_contents
)

# Configuration Constants
ATHENA_DATABASE = 'medical_data'
ATHENA_TABLE = 'patient_data'

def main(data_bucket_name, athena_output_bucket):
    try:
        print("=== Athena Query Script Started ===\n")
        
        # Step 1: Create Athena Database
        print(f"Creating Athena database '{ATHENA_DATABASE}'...")
        create_athena_database(ATHENA_DATABASE, athena_output_bucket)
        time.sleep(2)
        
        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE};"
        print(f"Dropping existing table: {drop_query}")
        run_athena_query(drop_query, ATHENA_DATABASE, athena_output_bucket)
        time.sleep(2)
        
        # Step 2: Create Athena Table
        print(f"Creating Athena table '{ATHENA_TABLE}' pointing to data bucket...")
        create_athena_table(ATHENA_DATABASE, ATHENA_TABLE, data_bucket_name, athena_output_bucket)
        time.sleep(2)
        
        # New Step: Describe the table
        describe_table(ATHENA_DATABASE, ATHENA_TABLE, athena_output_bucket)
        time.sleep(2)

        list_s3_contents(data_bucket_name)
        
        # Step 3: Perform Athena SELECT Query
        select_query = f"SELECT * FROM {ATHENA_DATABASE}.{ATHENA_TABLE} LIMIT 2;"
        print(f"Running Athena SELECT query: {select_query}")
        run_athena_query(select_query, ATHENA_DATABASE, athena_output_bucket)
        print("\n=== Athena Query Script Completed ===")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    try:
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
            data_bucket_name = config_data['data_bucket']
            athena_output_bucket = config_data['athena_output_bucket']
    except Exception as e:
        print(f"Error loading configuration: {e}")
        exit(1)
    
    main(data_bucket_name, athena_output_bucket)