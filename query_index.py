import pyathena
import credentials
import pandas as pd
from pyathena.pandas.util import as_pandas
import os.path

athena_conn = pyathena.connect(
    aws_access_key_id=credentials.AWS_KEY,
    aws_secret_access_key=credentials.AWS_SECRET,
    s3_staging_dir=credentials.AWS_BUCKET,
    region_name=credentials.AWS_REGION).cursor()


def store_index_pq(result_df, database_index=r"./cc.parquet"):
    # for every domain in df replace the set of found index records for given index block
    print(result_df)
    print(result_df.columns)

    # Create an empty DataFrame with columns
    if not os.path.isfile(database_index):
        columns = ['domain', 'crawl', 'url_path', 'warc_filename', 'warc_record_offset', 'warc_record_length']
        df_empty = pd.DataFrame(columns=columns)
        # Save the empty DataFrame with columns to a Parquet file
        df_empty.to_parquet(database_index)

    # add index addresses to DB
    index_df = pd.read_parquet(database_index, engine='fastparquet')
    index_df = pd.concat([index_df, result_df], ignore_index=True)
    index_df = index_df.drop_duplicates(keep="last")
    index_df = index_df.reset_index(drop=True)

    print(index_df.head())
    print(index_df.tail())

    index_df.to_parquet(database_index, index=False)


def query_athena(list_of_urls, index_list):
    # creating single strings from lists
    list_string = ", ".join(f"'{url}'" for url in list_of_urls)
    index_string = ", ".join(f"'CC-MAIN-{index_value}'" for index_value in index_list)

    # running SQL query in single scan across all index values
    fields = "url_host_registered_domain AS domain, url_path, warc_filename, warc_record_offset, warc_record_length, crawl"
    table = '"ccindex"."ccindex"'
    conditions = f"crawl IN ({index_string}) AND subset = 'warc' AND url_host_registered_domain IN ({list_string})"

    sql = (f"SELECT {fields} "
           f"FROM {table} "
           f"WHERE {conditions};")

    print(sql)  # Debugging

    # executing query and store to pandas df
    df = as_pandas(athena_conn.execute(sql))
    print(f"Athena query for {len(list_of_urls)} done.")

    store_index_pq(df)
    print(f"Query result with {len(df)} rows stored in parquet file.")