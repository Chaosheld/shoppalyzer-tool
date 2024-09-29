from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse
import extruct
import lxml
import html
import json
from cleaning import clean_string, clean_price

# Extract links from HTML
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


def extract_metadata(html_data):
    metadata = extruct.extract(html_data,
                               uniform=True,
                               syntaxes=['json-ld',
                                         'microdata',
                                         'opengraph'])
    return metadata

def get_markups(html_data):
    try:
        metadata = extract_metadata(html_data)
    except:
        metadata = {'microdata': [], 'json-ld': [], 'opengraph': []}

    # adding metadata that is infused by script tag
    soup = BeautifulSoup(html_data, 'html.parser')
    for json_element in soup.find_all('script', type='application/ld+json'):
        try:
            product_script = json.loads(json_element.text)
            if type(product_script) == dict:
                if "Product" in product_script.values():
                    metadata['json-ld'].append(product_script)
            else:
                if "Product" in product_script[0].values():
                    metadata['json-ld'].append(product_script)
        except:
            pass
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
    return None


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
    return None


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
    return None

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
    return None


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


def extract_follow_links(link_list):
    facebook_pattern = '([^developers]|[^docs]).facebook\.com(?!\/share|\/ads\/|\/legal\/|\/about\/|\/groups\/|\/policy\.php|\/business\/|\/settings\?)'
    twitter_pattern = 'twitter\.com(?!(\/share|\/intent|\/personalization|(\/)(home)(\/)*(\?)))'
    instagram_pattern = '([^help]).instagram\.com(?!\/share\/|\/about\/|\/explore\/|\/p\/|\/tv\/|\/vp\/|\/oauth)'
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
            facebook_link = re.search(r'.*(facebook\.com\/.*?)(\/|$)', link)
            if facebook_link:
                facebook_link = re.sub(r'\/$', '', facebook_link.group(0))
                if facebook_link not in track_list:
                    track_list.append(facebook_link)
                    res_list.append(('facebook', facebook_link))

        if bool(re.search(twitter_pattern, str(link))):
            twitter_link = re.search(r'(.*(twitter\.com\/.*?)(\/|$))|(.*(twitter\.com\/.*?)(\/|$))', link)
            if twitter_link:
                twitter_link = re.sub(r'\/$', '', twitter_link.group(0))
                if twitter_link not in track_list:
                    track_list.append(twitter_link)
                    res_list.append(('twitter', twitter_link))

        if bool(re.search(instagram_pattern, str(link))):
            instagram_link = re.search(r'.*(instagram\.com\/.*?)(\/|$)', link)
            if instagram_link:
                instagram_link = re.sub(r'\/$', '', instagram_link.group(0))
                if instagram_link not in track_list:
                    track_list.append(instagram_link)
                    res_list.append(('instagram', instagram_link))

        if bool(re.search(youtube_pattern, str(link))):
            youtube_link = re.search(r'(.*(youtube\.com\/((user\/.*?)|(channel\/.*?)|(c\/.*?)|.+?))(\/|$|\n|\t|\r))',
                                     link)
            if youtube_link:
                youtube_link = re.sub(r'\/$', '', youtube_link.group(0))
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