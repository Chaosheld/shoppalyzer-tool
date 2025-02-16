import credentials
import duckdb
import os.path
import boto3
import time


s3_client = boto3.client(
    "s3",
    aws_access_key_id=credentials.AWS_KEY,
    aws_secret_access_key=credentials.AWS_SECRET,
    region_name=credentials.AWS_REGION,
)

athena_client = boto3.client(
    "athena",
    aws_access_key_id=credentials.AWS_KEY,
    aws_secret_access_key=credentials.AWS_SECRET,
    region_name="us-east-1"
)

def query_athena(list_of_urls, index_list, database_index="./cc.parquet"):
    print(f"🚀 Running Athena queries...")

    for index_value in index_list:
        list_string = f"'{list_of_urls[0]}'"
        for i in range(1, len(list_of_urls)):
            list_string += f", '{list_of_urls[i]}'"

        # building the actual query to AWS Athena
        fields = f"url_host_registered_domain AS domain, crawl, url_path, warc_filename, warc_record_offset, warc_record_length"
        table = '"ccindex"."ccindex"'
        conditions = f"crawl = 'CC-MAIN-{index_value}' AND subset = 'warc' AND url_host_registered_domain IN ({list_string})"

        sql = (f"SELECT {fields} "
               f"FROM {table} "
               f"WHERE {conditions};")

        s3_bucket = credentials.AWS_BUCKET.replace("s3://", "").strip("/")
        s3_prefix = "cc_index_results"

        print(f"Athena connected and querying 'CC-MAIN-{index_value}.")
        s3_path = run_athena_query(sql, s3_bucket, s3_prefix)

        # storing Athena result from S3 in local Parquet
        store_index_pq(database_index, s3_path)

    print(f"📈💯 All queries are done. Happy crawling!")


def run_athena_query(sql, s3_bucket, s3_prefix):
    """
    Executes an Athena query and waits for it to be completed.
    Returns the S3 path to the result file.
    """
    # execute query and get the execution id
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "ccindex"},
        ResultConfiguration={"OutputLocation": f"s3://{s3_bucket}/{s3_prefix}/"},
    )

    query_execution_id = response["QueryExecutionId"]
    print(f"🚀 Athena Query gestartet: {query_execution_id}")

    # waiting for query to finish
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        print("Athena query still running, waiting for 5 seconds...")
        time.sleep(5)

    if status != "SUCCEEDED":
        raise ValueError(f"Athena query failed: {status}")

    print(f"Athena query successfully completed: {query_execution_id}")

    s3_result_path = f"s3://{s3_bucket}/{s3_prefix}/{query_execution_id}.csv"
    print(f"Result is available under: {s3_result_path}")

    return s3_result_path


def store_index_pq(database_index, s3_path):
    """
    Gets the latest Athena result file from S3 (CSV) and stores the content in Parquet using DuckDB.
    """
    count_query = f"SELECT COUNT(*) FROM read_parquet('{database_index}')"

    # configuring access to S3 for DuckDB
    duckdb.execute(f"""
        SET s3_region='{credentials.AWS_REGION}';
        SET s3_access_key_id='{credentials.AWS_KEY}';
        SET s3_secret_access_key='{credentials.AWS_SECRET}';
    """)

    df = duckdb.read_csv(s3_path)
    print(f"Access to S3 successful! {len(df)} rows loaded.")

    # in case no Parquet file exists
    if not os.path.isfile(database_index):
        duckdb.execute(f"COPY df TO '{database_index}' (FORMAT PARQUET)")
        row_count = duckdb.query(count_query).fetchone()[0]
        print(f"New Parquet file created: {database_index} with {row_count} rows")

    # else upsert using UNION statement
    else:
        row_count = duckdb.query(count_query).fetchone()[0]
        # loading already existing data to temp table
        duckdb.execute(f"CREATE OR REPLACE TEMP TABLE existing_data AS SELECT * FROM read_parquet('{database_index}')")

        # loading new csv data to temp table
        duckdb.execute("CREATE OR REPLACE TEMP TABLE new_data AS SELECT domain, crawl, url_path, warc_filename, warc_record_offset, warc_record_length FROM df")

        duckdb.execute(f"""
                    COPY (
                        SELECT * FROM existing_data
                        UNION 
                        SELECT * FROM new_data
                    ) TO '{database_index}' (FORMAT PARQUET)
                """)

        row_count_updated = duckdb.query(count_query).fetchone()[0] - row_count
        print(f"Parquet file updated: {row_count_updated} rows added.")