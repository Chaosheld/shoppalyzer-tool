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
    for index_value in index_list:
        list_string = f"'{list_of_urls[0]}'"
        for i in range(1, len(list_of_urls)):
            list_string += f", '{list_of_urls[i]}'"

        # building the actual query to AWS Athena
        fields = "url_host_registered_domain As domain, url_path, warc_filename, warc_record_offset, warc_record_length"
        table = '"ccindex"."ccindex"'
        conditions = f"crawl = 'CC-MAIN-{index_value}' AND subset = 'warc' AND url_host_registered_domain IN ({list_string})"

        sql = (f"SELECT {fields} "
               f"FROM {table} "
               f"WHERE {conditions};")

        print(sql)

        # run query and store results in pandas df
        athena_conn.execute(sql)
        df = as_pandas(athena_conn)
        print(f"Athena connected and querying 'CC-MAIN-{index_value}.")
        df["crawl"] = f"CC-MAIN-{index_value}"
        store_index_pq(df)
