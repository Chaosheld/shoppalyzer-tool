import pandas as pd
import numpy as np
import main
import zlib
import requests
from datetime import datetime
import time
import random
import sqlite3
import langid
import asyncio
from prediction import get_prediction
from translation import get_translation
from technologies import get_technology
from download_crawl import download_all
from extraction import get_markups, get_metadata, get_additional_data, get_external_links, extract_external_links, extract_follow_links

def query_db(url, index_list):
    database = r"./cc.sqlite"
    with main.create_connection(database) as conn:
        table = "cc_index"
        record_list = []
        for index in index_list:
            conditions = f"domain = '{url}' and crawl = 'CC-MAIN-{index}'"
            sql = (f"SELECT * "
                   f"FROM {table} "
                   f"WHERE {conditions};")
            df = pd.read_sql(sql, conn)
            for ix, row in df.iterrows():
                record_list.append(row)

        return record_list

def query_pq(url, index_list, database_index=r"./cc.parquet"):
    bucket = ["CC-MAIN-" + str(index) for index in index_list]
    df = pd.read_parquet(database_index, engine='fastparquet')
    df = df[(df['domain'] == url) & (df['crawl'].isin(bucket))]
    df = df.drop_duplicates(subset=['domain', 'url_path'], keep='last')
    df = df[~df['url_path'].str.endswith(('.pdf', '.docx', '.csv', '.xlsx'), na=False)]
    record_list = df.to_dict(orient='records')
    print('[*] %d unique entries found for %s' % (len(df), url))
    return record_list


def download_page(record):
    data_url = "https://data.commoncrawl.org/" + record["warc_filename"]

    headers = {
        "Range": f'bytes={int(record["warc_record_offset"])}-{int(record["warc_record_offset"]) + int(record["warc_record_length"])}'}

    r = requests.get(data_url, headers=headers)
    try:
        data = zlib.decompress(r.content, wbits=zlib.MAX_WBITS | 16)
    except:
        data = []

    response = ""

    if len(data):
        try:
            warc, header, response = data.decode("utf-8").strip().split("\r\n\r\n", 2)
        except:
            pass

    return response


def check_nones(dictionary, minimum):
    nots = 0
    for key in dictionary:
        if dictionary[key] is not None:
            nots += 1
    if nots >= minimum:
        return True
    else:
        return False


def parse_header(header_str):
    header_dict = {}
    lines = header_str.strip().splitlines()

    for line in lines:
        if ': ' in line:
            key, value = line.split(': ', 1)
            # if key already exists (e.g. Set-Cookie) make a list
            if key in header_dict:
                if isinstance(header_dict[key], list):
                    header_dict[key].append(value)
                else:
                    header_dict[key] = [header_dict[key], value]
            else:
                header_dict[key] = value
        else:
            if 'HTTP/' in line:
                header_dict['Status'] = line
    return header_dict


def create_output_file(date_string):
    update_conn = sqlite3.connect(f'./files/output/{date_string}_cc_result.sqlite')
    cursor = update_conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS cc_summary (
        'domain' TEXT,
        'archiveYear' TEXT,
        'totalRecords' INTEGER,
        'uniqueRecords' INTEGER)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS cc_metadata (
        'domain' TEXT,
        'url' TEXT,
        'archiveYear' TEXT,
        'detectedLanguage' TEXT,
        'productTitle' TEXT,
        'productDescription' TEXT,
        'brand' TEXT,
        'price' REAL,
        'currency' TEXT,
        'predictedCategory' INTEGER,
        'probability' REAL)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS external_links (
        'domain' TEXT,
        'archiveYear' TEXT,
        'externalLink' TEXT,
        'count' INTEGER)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS social_links (
        'domain' TEXT,
        'archiveYear' TEXT,
        'socialPlatform' TEXT,
        'socialLink' Text)''')

    update_conn.commit()
    update_conn.close()


def crawl_common_crawl(url_list, index_list, limit=0):

    print('[*] Started to crawl %d domains from %d indices' % (len(url_list), len(index_list)))

    for url in url_list:
        start_time = time.time()
        today = str(datetime.today().strftime('%Y%m%d'))

        update_conn = sqlite3.connect(f'./files/output/{today}_cc_result.sqlite')
        cursor = update_conn.cursor()

        create_output_file(today)
        detected_tech = {}

        record_list = query_pq(url, index_list)
        random.shuffle(record_list)
        if limit > 0:
            record_list = record_list[:limit]
        link_list = []

        # TODO: create settings with cool defaults
        batch_size = 10
        for c in range(0, len(record_list), batch_size):
            batch = record_list[c:c + batch_size]
            dump = asyncio.run(download_all(batch))
            for record in dump:
                if record is not None:
                    html_content = record['response']
                    header = record['header']

                    # experimental technology lookup
                    detected_tech.update(get_technology(url, html_content, parse_header(header)))
                    print(detected_tech)

                    if html_content:
                        metadata = get_markups(html_content)
                        corpus_data = get_metadata(url, record['url_path'], metadata)
                        if check_nones(corpus_data, 3):
                            # now that there is useful metadata we can add page title, description and lang code
                            additional_data = get_additional_data(html_content)
                            for key in additional_data:
                                corpus_data[key] = additional_data[key]
                            # TODO: make year instead of index
                            corpus_data['archiveYear'] = 2023

                            # building string and get translation
                            title = str(corpus_data.get('productTitle', '')).strip().lower()
                            description = str(corpus_data.get('productDescription', '')).strip().lower()

                            invalid_values = {'none', 'null', 'undefined', ''}

                            if title not in invalid_values:
                                if description not in invalid_values:
                                    product_string = corpus_data['productTitle'] + '. ' + corpus_data['productDescription']
                                else:
                                    product_string = corpus_data['productTitle']
                            else:
                                product_string = corpus_data.get('productDescription', '')

                            if product_string is not None and len(product_string) > 0:
                                detected_lang = langid.classify(product_string)[0]
                                print(detected_lang)
                                corpus_data['detectedLanguage'] = detected_lang
                                if not detected_lang == "en":
                                    # get_translation can return non if language is not possible to translate
                                    product_string = get_translation(detected_lang, product_string)
                                if product_string is not None:
                                    # get prediction
                                    prediction = get_prediction(product_string)
                                    print(prediction)
                                    corpus_data['predictedCategory'] = prediction['id']
                                    corpus_data['probability'] = prediction['probability']

                                    # write line to result file with
                                    print('[*] Write metadata for %s' % url)

                                    print(corpus_data)

                                    meta_fields = ['domain', 'url', 'archiveYear', 'detectedLanguage', 'productTitle',
                                                   'productDescription', 'brand', 'price', 'currency', 'predictedCategory',
                                                   'probability']
                                    res_val = [corpus_data[key] for key in meta_fields]
                                    sql = f'''INSERT INTO cc_metadata ({', '.join(meta_fields)}) 
                                              VALUES ({', '.join(['?'] * len(meta_fields))})'''
                                    cursor.execute(sql, res_val)
                                    update_conn.commit()

                        link_list = extract_external_links(url, html_content, link_list)

        uniq_externals = get_external_links(url, link_list)
        for key in uniq_externals:
            res = [url, 2023, key, uniq_externals[key]]
            sql = f'''INSERT INTO external_links ({', '.join(['domain', 'archiveYear', 'externalLink', 'count'])})
                      VALUES ({', '.join(['?'] * len(res))})'''
            cursor.execute(sql, res)
        update_conn.commit()
        print('[*] Total uniq external links in output: %d' % len(uniq_externals))

        follow_links = extract_follow_links(link_list)
        for res_tuple in follow_links:
            res = [url, 2023, res_tuple[0], res_tuple[1]]
            sql = f'''INSERT INTO social_links ({', '.join(['domain', 'archiveYear', 'socialPlatform', 'socialLink'])})
                      VALUES ({', '.join(['?'] * len(res))})'''
            cursor.execute(sql, res)
        update_conn.commit()
        print('[*] Total social links to follow in output: %d' % len(follow_links))

        update_conn.close()
        print("[*] Finished %s in %s seconds." % (url, time.time() - start_time))
