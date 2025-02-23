import re
import settings
import tldextract
from playwright.async_api import async_playwright
from src.patterns import link_exclusions
from bs4 import BeautifulSoup


def start_links(r, domain, html):
    follow_links = []
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a", href=True)  # find all elements with the tag <a>
    for link in links:
        link = link.get("href")
        if link.startswith(("mailto:", "tel:", "javascript:", "data:", "#")):
            continue
        if link.startswith("http"):
            if domain in link:
                link = re.sub(r"(\?.+)|(#.+)", "", link).replace(" ", "")
                follow_links.append(link)
        else:
            link = re.sub(r"(\?.+)|(#.+)", "", (r + link)).replace(" ", "")
            follow_links.append(link)
    follow_links = [page_link for page_link in follow_links if not page_link.endswith(tuple(link_exclusions))]
    return list(set(follow_links))


def collect_links(domain, html):
    follow_links = []
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all("a", href=True)  # find all elements with the tag <a>
    for link in links:
        link = link.get("href")
        if link.startswith("http") and domain in link:
            link = re.sub(r"(\?.+)|(#.+)", "", link).replace(" ", "")
            follow_links.append(link)
    follow_links = [page_link for page_link in follow_links if not page_link.endswith(tuple(link_exclusions))]
    return list(set(follow_links))


def parse_header(header_dict):
    """ converting header format matching the one from Common Crawl """
    header_str = "\n".join(f"{key}: {value}" for key, value in header_dict.items())
    return header_str


async def crawl_live(domain):
    records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        page = await context.new_page()
        page.set_default_timeout(60000)
        response = await page.goto(f'http://{domain}/', timeout=60000, wait_until="networkidle")
        extracted = tldextract.extract(response.url)
        extracted_domain = extracted.registered_domain

        pattern = r"^(https?://(?:[^/]+\.)?{0})(?:/.*)?$".format(re.escape(domain))
        match = re.match(pattern, response.url)
        if match:
            r = match.group(1)
        else:
            r = response.url.rstrip("/")
        print(r)

        if domain in extracted_domain and response.status == 200:
            link = re.sub(r"(\?.+)|(#.+)", "", r).replace(" ", "")

            # let's start collecting html content recursively
            html = await page.content()
            header_raw = response.headers  # header as dict
            header = parse_header(header_raw)

            link_bucket = start_links(r, domain, html)
            done_bucket = [link]
            records = [{'link': link, 'content': html, 'header': header}]

            # recursive crawl and returning the html
            while (len(records) < settings.MAX_LIVE_RECORDS) and (len(link_bucket) > 0):
                lk = link_bucket.pop(0)
                print(lk)

                try:
                    response = await page.goto(lk)
                    if response and response.status == 200:
                        html = await page.content()
                        header_raw = response.headers  # header as dict
                        header = parse_header(header_raw)

                        records.append({'link': lk, 'content': html, 'header': header})
                        link_bucket += collect_links(domain, html)
                        link_bucket = list(set(link_bucket))
                        print(len(link_bucket))
                except Exception as e:
                    print(f"Error requesting url: {lk} → {str(e)}")

                done_bucket.append(lk)
                print(len(done_bucket))

                # clean list_bucket if already known links where collected
                link_bucket = [page_link for page_link in link_bucket if page_link not in done_bucket]
                print(len(link_bucket))

            print(len(records))

        await browser.close()
    return records


async def crawl_url(url, link_bucket):

    print(f'[*] Started to crawl {url} live and in colour to boost results.')

    # connect to website and collect html content
    records = await crawl_live(url)

    # removing records which were already part of Common Crawl
    records = [record for record in records if record["link"] not in link_bucket]

    return records