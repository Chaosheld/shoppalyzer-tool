import pyathena
import credentials
import pandas as pd
from pyathena.pandas.util import as_pandas
import main

athena_conn = pyathena.connect(
    aws_access_key_id=credentials.AWS_KEY,
    aws_secret_access_key=credentials.AWS_SECRET,
    s3_staging_dir=credentials.AWS_BUCKET,
    region_name=credentials.AWS_REGION).cursor()


def store_index_query(result_df):
    # for every domain in df replace the set of found index records for given index block
    print(result_df)
    print(len(result_df))
    print(result_df.columns)

    database = r"./cc.sqlite"
    conn = main.create_connection(database)

    # add meta information to DB
    meta_df = pd.read_sql("SELECT * FROM cc_meta", conn)

    res_meta_df = result_df.groupby(["domain", "crawl"]).size().reset_index().rename(columns={0: "count"})
    res_meta_df = res_meta_df.reset_index(drop=True)

    meta_df = pd.concat([meta_df, res_meta_df[["domain", "crawl", "count"]]], ignore_index=True)
    meta_df = meta_df.drop_duplicates(subset=["domain", "crawl"], keep="last")
    meta_df = meta_df.reset_index(drop=True)

    print(meta_df.head())
    print(meta_df.tail())

    meta_df.to_sql("cc_meta", con=conn, if_exists="replace", chunksize=1000, index=False)

    # add index addresses to DB
    index_df = pd.read_sql("SELECT * FROM cc_index", conn)

    index_df = pd.concat([index_df, result_df], ignore_index=True)
    index_df = index_df.drop_duplicates(keep="last")
    index_df = index_df.reset_index(drop=True)

    print(index_df.head())
    print(index_df.tail())

    index_df.to_sql("cc_index", con=conn, if_exists="replace", chunksize=1000, index=False)

    conn.close()

def query_athena(list_of_urls, index_list):
    for index_value in index_list:
        list_string = f"'{list_of_urls[0]}'"
        for i in range(1, len(list_of_urls)):
            list_string += f", '{list_of_urls[i]}'"

        # building the actual query to AWS Athena
        fields = "url_host_registered_domain As domain, url_path, warc_filename, warc_record_offset, warc_record_length"
        table = '"ccindex"."ccindex"'
        conditions = f"crawl = 'CC-MAIN-{index_value}' AND subset = 'warc' AND url_host_registered_domain IN ({list_string})"
        limit = 10000

        sql = (f"SELECT {fields} "
               f"FROM {table} "
               f"WHERE {conditions};")
               #f"LIMIT {limit};")

        print(sql)

        # run query and store results in pandas df
        athena_conn.execute(sql)
        df = as_pandas(athena_conn)
        print(f"Athena connected and querying 'CC-MAIN-{index_value}.")
        df["crawl"] = f"CC-MAIN-{index_value}"
        store_index_query(df)
