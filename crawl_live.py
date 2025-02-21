import asyncio
import re
import settings
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
                link = re.sub(r"(\?.+)|(#.+)", "", link)
                follow_links.append(link)
        else:
            link = re.sub(r"(\?.+)|(#.+)", "", (r + link))
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
            link = re.sub(r"(\?.+)|(#.+)", "", link)
            follow_links.append(link)
    follow_links = [page_link for page_link in follow_links if not page_link.endswith(tuple(link_exclusions))]
    return list(set(follow_links))


async def crawl_live(domain):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        response = await page.goto(f'https://{domain}/')
        r = response.url.rstrip("/")

        if domain in r and response.status == 200:
            link = re.sub(r"(\?.+)|(#.+)", "", r)

            # let's start collecting html content recursively
            html = await page.content()
            link_bucket = start_links(r, domain, html)
            done_bucket = [link]
            records = [{'link': link, 'content': html}]

            # recursive crawl and returning the html
            while (len(records) < settings.MAX_LIVE_RECORDS) and (len(link_bucket) > 0):
                lk = link_bucket.pop(0)
                print(lk)

                response = await page.goto(lk)
                if response and response.status == 200:
                    html = await page.content()
                    records.append({'link': lk, 'content': html})
                    link_bucket += collect_links(domain, html)
                    link_bucket = list(set(link_bucket))
                    print(len(link_bucket))

                done_bucket.append(lk)
                print(len(done_bucket))

                # clean list_bucket if already known links where collected
                link_bucket = [page_link for page_link in link_bucket if page_link not in done_bucket]
                print(len(link_bucket))

            print(len(records))

        await browser.close()
    return records


async def crawl_url(url):

    print(f'[*] Started to crawl {url} live and in colour to boost results.')

    # connect to website and collect html content
    records = await crawl_live(url)
    return records