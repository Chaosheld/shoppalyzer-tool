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
import time
import random
import sqlite3
import langid
import asyncio
from prediction import get_prediction
from translation import get_translation
from download_crawl import download_all

# TODO: move to cleaning pipe
def remove_duplicates(listicle):
    """
    :param listicle: all records that are each unique by address in archive
    :return: list that contains only unique url paths to avoid having same products more than one time
    """
    uniq_list = []
    seen_list = []
    for entry in listicle:
        if not entry["url_path"] in seen_list:
            seen_list.append(entry["url_path"])
            uniq_list.append(entry)
    return uniq_list

# TODO: move to cleaning pipe
def strip_html(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)

def clean_string(string):
    string = strip_html(string)
    string = re.sub(' +', ' ', string.strip().replace("\n", " "))
    return html.unescape(string)

def clean_price(price):
    if isinstance(price, list) or isinstance(price, tuple):
        price = price[0]
    if price is None:
        return None
    if isinstance(price, str):
        price = price.replace(' ', '').replace(chr(160), '').replace('\'', '')
    if re.search(r'(\D+\.)|(^\.)', price):
        return None
    cleaned_price = re.search(
        r'[+-]?((\d+)+([\,\.]\d+)+)([eE][+-]?\d+)?|((\d+[\,\.]\d{2})|(\d+))([eE][+-]?\d+)?', price)
    if cleaned_price:
        cleaned_price = cleaned_price[0]
        if ',' in cleaned_price or '.' in cleaned_price or '\'' in cleaned_price:
            if re.search(r'(\,\d{2}$)', cleaned_price):
                cleaned_price = cleaned_price.replace(',','.')
            if re.search(r'([\,\.]\d{3}$)', cleaned_price) or re.search(r'([\,\.]\d{3}\D)', cleaned_price):
                cleaned_price = re.sub(r'([\.\,])(\d{3}$)', r'\2', cleaned_price)# keep only the group 2
                cleaned_price = re.sub(r'([\.\,])(\d{3}\D)', r'\2', cleaned_price)# keep only the group 2
            try:
                cleaned_price = float(cleaned_price)
                return cleaned_price
            except:
                return None
        # no delimiters in str, test if is just integer
        try:
            cleaned_price = float(cleaned_price)
            return cleaned_price
        except:
            return None
    else:
        # no pattern of price-like numbers found
        return None


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
    if metadata.get('offers') is not None:
        if type(metadata.get('offers')) == dict:
            priceCurrency = metadata.get('offers').get('priceCurrency')
            if priceCurrency is not None:
                return metadata.get('offers').get('highPrice')
        elif type(metadata.get('offers')) == list:
            priceCurrency = metadata.get('offers')[0].get('priceCurrency')
            if priceCurrency is not None:
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
                            'productTitle': get_title(datablock),
                            'productDescription': get_description(datablock),
                            'brand': get_brand(datablock),
                            'price': get_price(datablock),
                            'currency': get_currency(datablock)
                        }
                        return result_metadata
        if (type(metadata_container) == list) and ('@graph' in metadata_container[0].keys()):
            metadata_container = metadata_container[0].get('@graph')
        elif (type(metadata_container) == dict) and ('@graph' in metadata_container.keys()):
            metadata_container = metadata_container.get('@graph')
        for entry in metadata_container:
            if entry is not None:
                if '@type' in entry:
                    if entry['@type'] == target:
                        result_metadata = {
                            'productTitle': get_title(entry),
                            'productDescription': get_description(entry),
                            'brand': get_brand(entry),
                            'price': get_price(entry),
                            'currency': get_currency(entry)
                        }
                        return result_metadata

def get_metadata(domain, url, metadata):
    schemas = ['opengraph', 'microdata', 'json-ld']
    schema_types = ['product', 'offer', 'Product', 'Book']

    result_metadata = {
        'domain': domain,
        'url': url,
        'productTitle': None,
        'productDescription': None,
        'brand': None,
        'price': None,
        'currency': None
    }

    for schema in schemas:
        for schema_type in schema_types:
            scraped_metadata = scrape_metadata(schema, metadata, schema_type)
            # print(scraped_metadata)
            if scraped_metadata is not None:
                # combining information
                for meta_key in scraped_metadata.keys():
                    if scraped_metadata.get(meta_key) is not None:
                        if result_metadata.get(meta_key) is None:
                            result_metadata[meta_key] = scraped_metadata.get(meta_key)
                        elif len(str(scraped_metadata.get(meta_key))) > len(str(result_metadata.get(meta_key))):
                            result_metadata[meta_key] = scraped_metadata.get(meta_key)

    return result_metadata


def check_nones(dictionary, minimum):
    nots = 0
    for key in dictionary:
        if dictionary[key] is not None:
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

        # TODO: filter record list to remove files like pdf (not useful anyway and saves time)
        record_list = query_db(url, index_list)
        random.shuffle(record_list)
        if limit > 0:
            record_list = record_list[:limit]
        link_list = []

        # TODO: create settings with cool defaults
        batch_size = 100
        for c in range(0, len(record_list), batch_size):
            batch = record_list[c:c + batch_size]
            dump = asyncio.run(download_all(batch))
            for record in dump:
                if record is not None:
                    html_content = record['response']
                    if html_content:
                        metadata = extract_metadata(html_content)
                        corpus_data = get_metadata(url, record['url_path'], metadata)
                        if check_nones(corpus_data, 3):
                            # now that there is useful metadata we can add page title, description and lang code
                            additional_data = get_additional_data(html_content)
                            for key in additional_data:
                                corpus_data[key] = additional_data[key]
                            # TODO: make year instead of index
                            corpus_data['archiveYear'] = 2023

                            # get translation
                            product_string = corpus_data['productTitle'] + ' ' + corpus_data['productDescription']
                            detected_lang = langid.classify(product_string)[0]
                            print(detected_lang)
                            corpus_data['detectedLanguage'] = detected_lang
                            if not detected_lang == "en":
                                product_string = get_translation(product_string)

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
