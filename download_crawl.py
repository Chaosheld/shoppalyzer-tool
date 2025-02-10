import asyncio
import aiohttp
import zlib
import random
import chardet
from random import randint
from tqdm.asyncio import tqdm
from bs4 import BeautifulSoup


def detect_encoding(data_bytes):
    """Detects the best character encoding for HTML pages."""
    detected_encoding = chardet.detect(data_bytes)["encoding"] or "ISO-8859-1"

    # check with bs4 if HTML itself defines encoding (from <meta charset>)
    try:
        soup = BeautifulSoup(data_bytes, "html.parser")
        meta_tag = soup.find("meta", charset=True) or soup.find("meta", attrs={"http-equiv": "Content-Type"})
        if meta_tag:
            if meta_tag.has_attr("charset"):
                detected_encoding = meta_tag["charset"]
            elif "charset=" in meta_tag.get("content", ""):
                detected_encoding = meta_tag["content"].split("charset=")[-1].strip()
    except Exception:
        pass

    return detected_encoding


async def safe_download(i, c, sem):
    async with sem:  # semaphore limits num of simultaneous downloads
        return await download_crawl(i, c)

async def download_crawl(record, session):
    # throttling the speed to be polite
    await asyncio.sleep(randint(10, 50)/1000)
    base_wait = 2 #seconds
    max_retries = 10
    count_retries = 0

    data_url = "https://data.commoncrawl.org/" + record["warc_filename"]
    headers = {
        "Range": f'bytes={int(record["warc_record_offset"])}-{int(record["warc_record_offset"]) + int(record["warc_record_length"])}'}

    while count_retries < max_retries:
        async with session.get(data_url, headers=headers) as r:
            r_status = r.status
            if r_status == 206: # 206 means that the request has succeeded and the body contains the requested ranges of
                # data, as described in the Range header of the request
                stream = r.content
                try:
                    d = await stream.read()
                except aiohttp.client_exceptions.ClientPayloadError:
                    print('Cannot read the content of the record')
                    return
                try:
                    data = zlib.decompress(d, wbits=zlib.MAX_WBITS | 16)
                except:
                    data = b""  # empty bytes as fallback

                if len(data):
                    try:
                        try:
                            warc, header, response = data.decode("utf-8").strip().split("\r\n\r\n", 2)
                        except ValueError as e:
                            print(f"Value error: {e}")
                            response = ""
                    except UnicodeDecodeError:
                        # non-standard UTF8 encoding
                        encoding = detect_encoding(data)
                        print(f"Weird encoding for HTML content detected: {encoding}")

                        try:
                            warc, header, response = data.decode(encoding, errors="replace").strip().split("\r\n\r\n", 2)
                        except Exception as e:
                            print(f"Error decoding the HTML content with {encoding}: {e}")
                            response = ""

                    if len(response):
                        return {'domain': record['domain'], 'crawl': record['crawl'], 'url_path': record['url_path'],
                                'response': response, 'header': header}
                    else:
                        print(f"There is no response for this url: {record['domain']}{record['url_path']}")
                        return
                else:
                    return

            elif r.status == 503: # slow down response which is handled by an exponential backoff algo
                wait_time = base_wait * (2 ** count_retries) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
                count_retries += 1

            else:
                print('An error occurred')
                print([record['domain'], record['url_path'], r_status])
                return


async def download_all(records):
    result = []
    sem = asyncio.Semaphore(8)
    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.ensure_future(safe_download(record, session, sem))
            for record
            in records
        ]
        result += await tqdm.gather(*tasks)
    print(f"Batch of {len(result)} records downloaded")
    return result