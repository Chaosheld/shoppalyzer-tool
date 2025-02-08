import pandas as pd
import os
import zlib
import requests
from datetime import datetime
import time
import random
import langid
import asyncio
from technologies import get_technology
from download_crawl import download_all
from extraction import get_markups, get_metadata, get_additional_data, get_external_links, extract_external_links, extract_follow_links


def query_pq(url, index_list, database_index=r"./cc.parquet"):
    bucket = ["CC-MAIN-" + str(index) for index in index_list]
    df = pd.read_parquet(database_index, engine='fastparquet')
    df = df[(df['domain'] == url) & (df['crawl'].isin(bucket))]
    df = df.drop_duplicates(subset=['domain', 'url_path'], keep='last')
    # TODO: could be extended by more invalid types/documents
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


def crawl_common_crawl(url_list, index_list, query_year, limit=0):

    print('[*] Started to crawl %d domains from %d indices' % (len(url_list), len(index_list)))

    for url in url_list:
        start_time = time.time()
        update_date = datetime.today().strftime('%Y-%m-%d')

        base_path = f"files/output/{url}/{query_year}"
        os.makedirs(base_path, exist_ok=True)

        product_list = []
        product_meta_fields = ['domain', 'url', 'archive_year', 'detected_language', 'product_title',
                               'product_description', 'brand', 'price', 'currency', 'last_update']
        detected_technology = {}

        record_list = query_pq(url, index_list)
        random.shuffle(record_list)
        if limit > 0:
            record_list = record_list[:limit]
        link_list = []
        target = len(record_list)
        counter = 0

        # TODO: create settings with cool defaults
        # TODO: create sliding window and rules as break criteria
        # TODO: make a cleaning of currencies and prices
        # TODO: regex and NER for brands and tagging
        batch_size = 50

        # technology lookup for dom is expensive and stops earlier
        # TODO: move limit for counter into settings
        dom_enabled = True
        dom_limit = 10
        dom_change_counter = 0

        for c in range(0, len(record_list), batch_size):
            batch = record_list[c:c + batch_size]
            dump = asyncio.run(download_all(batch))
            for record in dump:
                if record is not None:
                    html_content = record['response']
                    header = record['header']

                    # storing count of technologies found to compare
                    previous_count = len(detected_technology)

                    #TODO: detected technology should return category too
                    detected_technology.update(get_technology(url, html_content, parse_header(header), dom_enabled))

                    # checking if new technology was found
                    if len(detected_technology) > previous_count:
                        dom_change_counter = 0
                        dom_enabled = True  # reactivating dom search
                    else:
                        dom_change_counter += 1  # no new technology found
                        if dom_change_counter > dom_limit:
                            dom_enabled = False  # deactivating dom search

                    if html_content:
                        metadata = get_markups(html_content)
                        corpus_data = get_metadata(url, record['url_path'], metadata)

                        if check_nones(corpus_data, 3):
                            # now that there is useful metadata we can add page title, description and lang code
                            additional_data = get_additional_data(html_content)
                            for key in additional_data:
                                corpus_data[key] = additional_data[key]
                            corpus_data['archive_year'] = query_year
                            corpus_data['last_update'] = update_date

                            # building string for later translation/classification task
                            title = str(corpus_data.get('product_title', '')).strip()
                            description = str(corpus_data.get('product_description', '')).strip()

                            invalid_values = {None, 'none', 'null', 'undefined', ''}

                            if title not in invalid_values:
                                if description not in invalid_values:
                                    product_string = title + '. ' + description
                                else:
                                    product_string = description
                            else:
                                product_string = description

                            if product_string is not None and len(product_string) > 0:
                                detected_lang = langid.classify(product_string)[0]
                                corpus_data['detected_language'] = detected_lang

                                print(f'[*] Found product for {url}: {title}')
                                product_list.append(corpus_data)

                        link_list = extract_external_links(url, html_content, link_list)

                # some tracking
                counter += 1
                progress = (counter / target * 100) if target else 0
                print(f'[*] Progress at {progress:.1f}%')


        ### Detected Products of Shop
        if len(product_list) > 0:
            output_file_products = f'{base_path}/products.pq'
            product_df = pd.DataFrame(product_list)
            product_df = product_df[product_meta_fields]

            if os.path.exists(output_file_products):
                existing_df = pd.read_parquet(output_file_products)
                combined_df = pd.concat([existing_df, product_df])
                combined_df = combined_df.sort_values('last_update').drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = product_df

            combined_df.to_parquet(output_file_products, index=False)
            print(f'[*] Total number of products detected: {len(product_list)}')

        ### External Links
        uniq_externals = get_external_links(url, link_list)
        if uniq_externals:
            data_list = [[url, query_year, key, uniq_externals[key], update_date] for key in uniq_externals]
            res_df = pd.DataFrame(data_list,
                                  columns=['domain', 'archive_year', 'external_link', 'count', 'last_update'])
            output_file_links = f'{base_path}/links.pq'

            if os.path.exists(output_file_links):
                existing_df = pd.read_parquet(output_file_links)
                combined_df = pd.concat([existing_df, res_df])
                combined_df = combined_df.sort_values('last_update').drop_duplicates(subset=['external_link'], keep='last')
            else:
                combined_df = res_df

            combined_df.to_parquet(output_file_links, index=False)
            print(f'[*] Total unique external links to follow in output: {len(uniq_externals)}')

        ### Social Media Accounts
        follow_links = extract_follow_links(link_list)
        if follow_links:
            data_list = [[url, query_year, social_platform, social_link, update_date] for social_platform, social_link in follow_links]
            res_df = pd.DataFrame(data_list,
                                  columns=['domain', 'archive_year', 'social_platform', 'social_link', 'last_update'])
            output_file_social = f'{base_path}/social.pq'

            if os.path.exists(output_file_social):
                existing_df = pd.read_parquet(output_file_social)
                combined_df = pd.concat([existing_df, res_df])
                combined_df = combined_df.sort_values('last_update').drop_duplicates(
                    subset=['social_platform', 'social_link'], keep='last')
            else:
                combined_df = res_df

            combined_df.to_parquet(output_file_social, index=False)
            print(f'[*] Total social media links in output: {len(follow_links)}')

        ### Technology
        if detected_technology:
            data_list = []

            for key, value in detected_technology.items():
                v = max(value['versions']) if value['versions'] else None
                c = value['categories']
                data_list.append([url, query_year, key, v, c, update_date])

            res_df = pd.DataFrame(data_list,
                                  columns=['domain', 'archive_year', 'technology', 'version', 'categories', 'last_update'])
            output_file_technology = f'{base_path}/technology.pq'

            if os.path.exists(output_file_technology):
                existing_df = pd.read_parquet(output_file_technology)
                combined_df = pd.concat([existing_df, res_df])
                combined_df = combined_df.sort_values('last_update').drop_duplicates(subset=['technology'], keep='last')
            else:
                combined_df = res_df

            combined_df.to_parquet(output_file_technology, index=False)
            print(f'[*] Total technologies detected and stored in output: {len(detected_technology)}')


        print(f'[*] Finished {url} in {time.time() - start_time:.1f} seconds.')
