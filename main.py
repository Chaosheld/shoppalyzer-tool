import os
import sqlite3
import sys
from sqlite3 import Error
import pandas as pd
import query_index
import crawl_index

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def init_db():
    database = r"./cc.sqlite"

    if os.path.exists(database):
        os.remove(database)

    sql_create_meta_table = """ CREATE TABLE IF NOT EXISTS cc_meta (
                                        domain text NOT NULL,
                                        crawl text NOT NULL,
                                        count integer NOT NULL
                                    ); """

    sql_create_index_table = """CREATE TABLE IF NOT EXISTS cc_index (
                                    domain text NOT NULL,
                                    crawl text NOT NULL,
                                    url_path text NOT NULL,
                                    warc_filename text NOT NULL,
                                    warc_record_offset integer NOT NULL,
                                    warc_record_length integer NOT NULL
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_meta_table)
        create_table(conn, sql_create_index_table)
        conn.close()
    else:
        print("Error, cannot create the database connection.")


def prepare_query(year_value):
    year_value = int(year_value)
    crawl_index_dict = {
        2019: ['2019-04', '2019-09', '2019-13', '2019-18', '2019-22', '2019-26', '2019-30', '2019-35', '2019-39',
               '2019-43', '2019-47', '2019-51'],
        2020: ['2020-05', '2020-10', '2020-16', '2020-24', '2020-29', '2020-34', '2020-40', '2020-45', '2020-50'],
        2021: ['2021-04', '2021-10', '2021-17', '2021-21', '2021-25', '2021-31', '2021-39', '2021-43', '2021-49'],
        2022: ['2022-05', '2022-21', '2022-27', '2022-33', '2022-40', '2022-49'],
        2023: ['2023-06', '2023-14', '2023-23', '2023-40', '2023-50'],
        2024: ['2024-10', '2024-18', '2024-22', '2024-26', '2024-30', '2024-33', '2024-38']
    }
    if year_value in crawl_index_dict.keys():
        return crawl_index_dict[year_value]
    else:
        return []


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "init":
            init_db()
        elif sys.argv[1] == "query":
            """
                assumes a csv file called input.csv with domains row by row
                second argument optional year value with latest bucket as default
            """
            url_df = pd.read_csv("./files/input/input.csv", header=None)
            url_list = url_df[url_df.columns[0]].tolist()
            if len(url_list) > 0:
                index_list = []
                if len(sys.argv) == 3:
                    query_year = sys.argv[2]
                    index_list = prepare_query(query_year)
                if len(index_list) == 0:
                    index_list = prepare_query(2023)
                query_index.query_athena(url_list, index_list)
            else:
                print("No domains provided. Please check input file.")
        elif sys.argv[1] == "crawl":
            """
            assumes a csv file called input.csv with domains row by row
            second argument optional year value with latest bucket as default
            """
            url_df = pd.read_csv("./files/input/input.csv", header=None)
            url_list = url_df[url_df.columns[0]].tolist()
            if len(url_list) > 0:
                index_list = []
                if len(sys.argv) == 3:
                    query_year = sys.argv[2]
                    index_list = prepare_query(query_year)
                if len(index_list) == 0:
                    index_list = prepare_query(2023)
                crawl_index.crawl_common_crawl(url_list, index_list, limit=100)
            else:
                print("No domains provided. Please check input file.")
        else:
            print("Please provide more detailed instructions.")