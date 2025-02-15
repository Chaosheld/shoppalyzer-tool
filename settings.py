########### Settings for Common Crawl ###########

BATCH_SIZE = 50     # size of download batch
DOM_LIMIT = 8       # after n pages with new entries, disable expensive DOM check

MAX_PAGES = 5000    # top limit of pages to be checked at all
MAX_PRODUCTS = 500  # limit of products that are sufficient

RELEVANCE_CHECK = 500  # after the number of pages a check if a relevant share of products is actually found
RELEVANCE_THRESHOLD = 2 # in percent share