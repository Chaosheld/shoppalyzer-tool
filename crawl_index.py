import pandas as pd
import main
import zlib
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
import extruct
import lxml
import html
from datetime import datetime
from os.path import exists
from csv import writer
import random
import gspread as gs


def remove_duplicates(list):
    """
    :param list: all records that are each unique by address in archive
    :return: list that contains only unique url paths to avoid having same products more than one time
    """
    uniq_list = []
    seen_list = []
    for entry in list:
        if not entry["url_path"] in seen_list:
            seen_list.append(entry["url_path"])
            uniq_list.append(entry)
    return uniq_list

def strip_html(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)

def clean_string(str):
    str = strip_html(str)
    str = re.sub(' +', ' ', str.strip().replace("\n", " "))
    return html.unescape(str)

def clean_price(str):
    return str.replace(',', '.')

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


# Extract links from the HTML

def extract_external_links(url, html_content, link_list):
    parser = BeautifulSoup(html_content, "html.parser")

    links = parser.find_all("a")

    if links:

        for link in links:
            href = link.attrs.get("href")

            if href is not None:

                if url not in href:
                    if href not in link_list and href.startswith("http"):
                        print("[*] Discovered external link: %s" % href)
                        link_list.append(href)

    return link_list


def get_external_links(domain, link_list):
    dict = {}
    for link in link_list:
        res = urlparse(link).netloc
        res = re.sub("^ww*.\.", "", res)
        if res != domain:
            if res in dict.keys():
                dict[res] += 1
            else:
                dict[res] = 1
    return dict


def extract_metadata(html):
    metadata = extruct.extract(html,
                               uniform=True,
                               syntaxes=['json-ld',
                                         'microdata',
                                         'opengraph'])
    return metadata


def get_description(metadata):
    key_trigger = ['og:description', 'description', 'Description']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                res = clean_string(metadata.get(trigger))
                return html.unescape(res)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    res = clean_string(metadata['offers'].get(trigger))
                    return html.unescape(res)


def get_title(metadata):
    key_trigger = ['title', 'Title', 'og:title', 'name']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                res = clean_string(metadata.get(trigger))
                return html.unescape(res)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    res = clean_string(metadata['offers'].get(trigger))
                    return html.unescape(res)


def get_brand(metadata):
    key_trigger = ['brand', 'Brand', 'product:brand']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                if type(metadata.get(trigger)) == dict:
                    return clean_string(metadata.get(trigger).get('name'))
                else:
                    return clean_string(metadata.get(trigger))
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return clean_string(metadata['offers'].get(trigger))

def get_category(metadata):
    key_trigger = ['category', 'Category']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                return clean_string(metadata.get(trigger))
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return clean_string(metadata['offers'].get(trigger))


def get_breadcrumb(metadata):
    key_trigger = ['breadcrumb']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                return clean_string(metadata.get(trigger))
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return clean_string(metadata['offers'].get(trigger))


def get_currency(metadata):
    if metadata.get('offers') is not None:
        if type(metadata.get('offers')) == dict:
            return metadata.get('offers').get('priceCurrency')
        elif type(metadata.get('offers')) == list:
            return metadata.get('offers')[0].get('priceCurrency')
    key_trigger = ['priceCurrency', 'product:price:currency']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                return metadata.get(trigger)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return metadata['offers'].get(trigger)


def get_price(metadata):
    if metadata.get('offers') is not None:
        if type(metadata.get('offers')) == dict:
            priceCurrency = metadata.get('offers').get('priceCurrency')
            if priceCurrency is not None:
                res = metadata.get('offers').get('price')
                return clean_price(res)
        elif type(metadata.get('offers')) == list:
            priceCurrency = metadata.get('offers')[0].get('priceCurrency')
            if priceCurrency is not None:
                res = metadata.get('offers')[0].get('price')
                return clean_price(res)
    key_trigger = ['price', 'product:price:amount', 'product:price']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                res = metadata.get(trigger)
                return clean_price(res)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    res = metadata['offers'].get(trigger)
                    return clean_price(res)


def get_lowPrice(metadata):
    if metadata.get('offers') is not None:
        if type(metadata.get('offers')) == dict:
            priceCurrency = metadata.get('offers').get('priceCurrency')
            if priceCurrency is not None:
                return metadata.get('offers').get('lowPrice')
        elif type(metadata.get('offers')) == list:
            priceCurrency = metadata.get('offers')[0].get('priceCurrency')
            if priceCurrency is not None:
                return metadata.get('offers')[0].get('lowPrice')
    key_trigger = ['lowPrice']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                return metadata.get(trigger)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return metadata['offers'].get(trigger)


def get_highPrice(metadata):
    if metadata.get('offers') != None:
        if type(metadata.get('offers')) == dict:
            priceCurrency = metadata.get('offers').get('priceCurrency')
            if priceCurrency != None:
                return metadata.get('offers').get('highPrice')
        elif type(metadata.get('offers')) == list:
            priceCurrency = metadata.get('offers')[0].get('priceCurrency')
            if priceCurrency != None:
                return metadata.get('offers')[0].get('highPrice')
    key_trigger = ['highPrice']
    for key in metadata.keys():
        for trigger in key_trigger:
            if key == trigger:
                return metadata.get(trigger)
    if 'offers' in metadata.keys():
        for key in metadata['offers']:
            for trigger in key_trigger:
                if key == trigger:
                    return metadata['offers'].get(trigger)


def scrape_metadata(schema, metadata, target):
    metadata_container = metadata[schema]
    if len(metadata_container) > 0:
        if len(metadata_container) > 1:
            for datablock in metadata_container:
                if (type(datablock) == list) and ('@graph' in datablock[0].keys()):
                    datablock = datablock[0].get('@graph')
                elif (type(datablock) == dict) and ('@graph' in datablock.keys()):
                    datablock = datablock.get('@graph')
                if type(datablock) == list:
                    datablock = datablock[0]
                    if datablock['@type'] == target:
                        result_metadata = {
                            'title': get_title(datablock),
                            'description': get_description(datablock),
                            'brand': get_brand(datablock),
                            'category': get_category(datablock),
                            'breadcrumb': get_breadcrumb(datablock),
                            'price': get_price(datablock),
                            'lowPrice': get_lowPrice(datablock),
                            'highPrice': get_highPrice(datablock),
                            'currency': get_currency(datablock)
                        }
                        return result_metadata
        if (type(metadata_container) == list) and ('@graph' in metadata_container[0].keys()):
            metadata_container = metadata_container[0].get('@graph')
        elif (type(metadata_container) == dict) and ('@graph' in metadata_container.keys()):
            metadata_container = metadata_container.get('@graph')
        for entry in metadata_container:
            if entry['@type'] == target:
                result_metadata = {
                    'title': get_title(entry),
                    'description': get_description(entry),
                    'brand': get_brand(entry),
                    'category': get_category(entry),
                    'breadcrumb': get_breadcrumb(entry),
                    'price': get_price(entry),
                    'lowPrice': get_lowPrice(entry),
                    'highPrice': get_highPrice(entry),
                    'currency': get_currency(entry)
                }
                return result_metadata


def get_metadata(domain, url, metadata):
    schemas = ['opengraph', 'microdata', 'json-ld']
    schema_types = ['product', 'offer', 'Product', 'Book']

    result_metadata = {
        'domain': domain,
        'url': url,
        'title': None,
        'description': None,
        'brand': None,
        'category': None,
        'breadcrumb': None,
        'price': None,
        'lowPrice': None,
        'highPrice': None,
        'currency': None
    }

    for schema in schemas:
        for schema_type in schema_types:
            scraped_metadata = scrape_metadata(schema, metadata, schema_type)
            # print(scraped_metadata)
            if scraped_metadata is not None:
                # combining information
                for meta_key in scraped_metadata.keys():
                    if scraped_metadata.get(meta_key) != None:
                        if result_metadata.get(meta_key) == None:
                            result_metadata[meta_key] = scraped_metadata.get(meta_key)
                        elif len(scraped_metadata.get(meta_key)) > len(result_metadata.get(meta_key)):
                            result_metadata[meta_key] = scraped_metadata.get(meta_key)

    return result_metadata


def check_nones(dictionary, minimum):
    nots = 0
    for key in dictionary:
        if dictionary[key] != None:
            nots += 1
    if nots >= minimum:
        return True
    else:
        return False


def extract_follow_links(link_list):
    facebook_pattern = '([^developers]|[^docs]).facebook\.com(?!\/share|\/ads\/|\/legal\/|\/about\/|\/groups\/|\/policy\.php|\/business\/|\/settings\?)'
    twitter_pattern = 'twitter\.com(?!(\/share|\/intent|\/personalization|(\/)(home)(\/)*(\?)))'
    instagram_pattern = '([^help]).instagram\.com(?!\/share\/|\/about\/|\/explore\/|\/p\/|\/tv\/|\/vp\/|\/oauth)'
    pinterest_pattern = '(pinterest\.)[a-z]{1,}(\.[a-z]{1,})*(\/)+?(?!pin\/create|js\/|search\/|pin\/|\/source\/)'
    youtube_pattern = '(.*(youtube\.com(\/.*?){1,2})(\/|$))|(.*(youtube\.com\/.*?)(\/|$))'
    tiktok_pattern = 'tiktok\.com\/@'

    playstore_pattern = 'play\.google\.com'
    appstore_pattern = 'itunes\.apple\.com'

    # list storing tuples to return as result
    res_list = []

    # list to keep track of already found links to avoid too many duplicates
    track_list = []

    # search for patterns and storing in found stuff
    for link in link_list:
        if bool(re.search(facebook_pattern, str(link))):
            facebook_link = re.search(r'.*(facebook\.com\/.*?)(\/|$)', link).group(0)
            facebook_link = re.sub(r'\/$', '', facebook_link)
            if facebook_link not in track_list:
                track_list.append(facebook_link)
                res_list.append(('facebook', facebook_link))

        if bool(re.search(twitter_pattern, str(link))):
            twitter_link = re.search(r'(.*(twitter\.com\/.*?)(\/|$))|(.*(twitter\.com\/.*?)(\/|$))', link).group(0)
            twitter_link = re.sub(r'\/$', '', twitter_link)
            if twitter_link not in track_list:
                track_list.append(twitter_link)
                res_list.append(('twitter', twitter_link))

        if bool(re.search(instagram_pattern, str(link))):
            instagram_link = re.search(r'.*(instagram\.com\/.*?)(\/|$)', link).group(0)
            instagram_link = re.sub(r'\/$', '', instagram_link)
            if instagram_link not in track_list:
                track_list.append(instagram_link)
                res_list.append(('instagram', instagram_link))

        if bool(re.search(youtube_pattern, str(link))):
            youtube_link = re.search(r'(.*(youtube\.com\/((user\/.*?)|(channel\/.*?)|(c\/.*?)|.+?))(\/|$|\n|\t|\r))',
                                     link).group(0)
            youtube_link = re.sub(r'\/$', '', youtube_link)
            if youtube_link not in track_list:
                track_list.append(youtube_link)
                res_list.append(('youtube', youtube_link))

        if bool(re.search(tiktok_pattern, str(link))):
            tiktok_link = link
            tiktok_link = re.sub(r'\/$', '', tiktok_link)
            if tiktok_link not in track_list:
                track_list.append(tiktok_link)
                res_list.append(('tiktok', tiktok_link))

        if bool(re.search(playstore_pattern, str(link))):
            playstore_link = link
            playstore_link = re.sub(r'\/$', '', playstore_link)
            if playstore_link not in track_list:
                track_list.append(playstore_link)
                res_list.append(('playstore', playstore_link))

        if bool(re.search(appstore_pattern, str(link))):
            appstore_link = link
            appstore_link = re.sub(r'\/$', '', appstore_link)
            if appstore_link not in track_list:
                track_list.append(appstore_link)
                res_list.append(('appstore', appstore_link))

    return res_list


def get_additional_data(html_content):
    dict = {
        'language': None,
        'page_title': None,
        'page_description': None,
        'page_keywords': None
    }

    tree = lxml.html.fromstring(html_content)

    language_construct = tree.xpath("//html/@lang")
    if language_construct:
        dict['language'] = language_construct[0].split('-')[0].split('_')[0]

    pagetitle = tree.xpath('//head/title/text()')
    if pagetitle:
        dict['page_title'] = clean_string(pagetitle[0])

    description = tree.xpath('//head/meta[@name="description"]/@content')
    if description:
        dict['page_description'] = clean_string(description[0])

    metaKeywords = tree.xpath('//head/meta[@name="keywords"]/@content')
    if metaKeywords:
        dict['page_keywords'] = clean_string(metaKeywords[0])

    return dict


def crawl_common_crawl(url_list, index_list, limit=0):
    # generating names of output files
    today = str(datetime.today().strftime('%Y%m%d'))
    meta_output = './output/%s_metadata.csv' % today
    external_links_output = './output/%s_external_links.csv' % today
    follow_links_output = './output/%s_follow_links.csv' % today
    print('[*] Started to crawl %d domains from %d indices' % (len(url_list), len(index_list)))

    # creating output files if not already existing to write header
    if not exists(meta_output):
        with open(meta_output, 'a') as output:
            writer_object = writer(output)
            writer_object.writerow(
                ['Domain', 'URL', 'Product Title', 'Product Description', 'Brand', 'Category', 'Breadcrumb', 'Price',
                 'Low Price', 'High Price', 'Currency', 'Language Code', 'Page Title', 'Page Description',
                 'Page Keywords'])
            output.close()

    if not exists(external_links_output):
        with open(external_links_output, 'a') as output:
            writer_object = writer(output)
            writer_object.writerow(['Domain', 'External', 'Occurrences'])
            output.close()

    if not exists(follow_links_output):
        with open(follow_links_output, 'a') as output:
            writer_object = writer(output)
            writer_object.writerow(['Domain', 'Platform', 'Link'])
            output.close()

    for url in url_list:
        record_list = query_db(url, index_list)
        if limit > 0:
            random.shuffle(record_list)
            record_list = record_list[:limit]
        link_list = []
        for record in record_list:
            html_content = download_page(record)
            print('[*] Retrieved %d bytes for %s' % (len(html_content), record['domain']))
            try:
                if html_content != "":
                    metadata = extract_metadata(html_content)
                    corpus_data = get_metadata(url, record['domain'], metadata)
                    if check_nones(corpus_data, 3):
                        # now that there is useful metadata we can add pagetitle, description and lang code
                        additional_data = get_additional_data(html_content)
                        for key in additional_data:
                            corpus_data[key] = additional_data[key]

                        # write line to result file with
                        print('[*] Write metadata for %s' % url)
                        with open(meta_output, 'a') as output:
                            writer_object = writer(output)
                            writer_object.writerow(corpus_data.values())
                            output.close()
            except:
                pass

            link_list = extract_external_links(url, html_content, link_list)

        print('[*] Total external links discovered: %d' % len(link_list))

        uniq_externals = get_external_links(url, link_list)
        # write result in another file
        with open(external_links_output, 'a') as output:
            writer_object = writer(output)
            for key in uniq_externals:
                writer_object.writerow([url, key, uniq_externals[key]])
            output.close()
        print('[*] Total uniq external links in output: %d' % len(uniq_externals))

        follow_links = extract_follow_links(link_list)
        with open(follow_links_output, 'a') as output:
            writer_object = writer(output)
            for res_tuple in follow_links:
                writer_object.writerow([url, res_tuple[0], res_tuple[1]])
            output.close()
        print('[*] Total social links to follow in output: %d' % len(follow_links))


def crawl_cc_google(url_list, index_list, limit=0):
    print('[*] Started to crawl %d domains from %d indices' % (len(url_list), len(index_list)))

    gc = gs.service_account(filename='sacred-alliance-367913-c5d49aaa22d2.json')

    sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1CKZZcIX252qmMKyGxjOQnmgBU6C4jRzdA_u8GG6pR_k/")
    ws = sh.worksheet('Productmeta')

    meta_columns = ['Domain', 'URL', 'Product Title', 'Product Description', 'Brand', 'Category', 'Breadcrumb', 'Price',
                 'Low Price', 'High Price', 'Currency', 'Language Code', 'Page Title', 'Page Description',
                 'Page Keywords']

    for url in url_list:
        df = pd.DataFrame(columns=meta_columns)
        record_list = query_db(url, index_list)
        # part to deduplicate the records while in a bucket links might be crawled wy more often than once
        record_list = remove_duplicates(record_list)
        if limit > 0:
            random.shuffle(record_list)
            record_list = record_list[:limit]
        for record in record_list:
            html_content = download_page(record)
            print('[*] Retrieved %d bytes for %s' % (len(html_content), record['domain']))
            try:
                if html_content != "":
                    metadata = extract_metadata(html_content)
                    corpus_data = get_metadata(url, record['domain'], metadata)
                    if check_nones(corpus_data, 3):
                        # now that there is useful metadata we can add page title, description and language code
                        additional_data = get_additional_data(html_content)
                        for key in additional_data:
                            corpus_data[key] = additional_data[key]
                        print('[*] Add metadata for %s' % url)
                        prod_df = pd.DataFrame([list(corpus_data.values())], columns=meta_columns)
                        print(prod_df['Price'])
                        df = pd.concat([df, prod_df], ignore_index=True)

            except:
                pass

        if len(df) > 0:
            df_product_meta = pd.DataFrame(ws.get_all_records())
            df_product_meta = pd.concat([df_product_meta, df], ignore_index=True)
            ws.update([df_product_meta.columns.values.tolist()] + df_product_meta.values.tolist())