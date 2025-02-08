import re
import html

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

def strip_html(data):
    p = re.compile(r'<.*?>')
    return p.sub('', str(data))

def clean_string(string):
    if type(string) == list:
        if len(string) > 0:
            string = string[0]
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
        if price is None or re.search(r'(\D+\.)|(^\.)', price):
            return None
    cleaned_price = re.search(
        r'[+-]?((\d+)+([\,\.]\d+)+)([eE][+-]?\d+)?|((\d+[\,\.]\d{2})|(\d+))([eE][+-]?\d+)?', str(price))
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