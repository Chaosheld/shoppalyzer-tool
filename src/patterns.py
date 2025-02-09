patterns_product_pages = [
    # obvious product pages with language variants
    '/product/',
    '/products/',
    '/productDetails/',
    '/produkt/',
    '/produkte/',
    '/produkter/',
    '/produkto/',
    '/prodotto/'
    r'\.product\.',

    # some specific indicators
    '/p/',
    r'\/.*-p-.*\.html',
    '/pro/',
    '/pdp/',
    '/dp/',
    '/ip/',
    '/item/',
    '/item-detail/',
    '/detail/'
]

patterns_prices = [
    (r"([^:\-_?!\w]*)(product:price:amount)[^:\-_?!\w]*content[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 3),
    (r"([^:\-_?!\w]*)(regularprice)[^:\-_?!\w]*content[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 3),
    (r"[^:\-_?!\w]*(price(01|02)*)[^:\-_?!\w]*[=:{'\"><_\s]*amount([=:{'\"><_\s]*)(\d+([.,]\d+)*)", 4),
    (r"[^:\-_?!\w]*(regularprice)[^:\-_?!\w]*[=:{'\"><_\s]*amount([=:{'\"><_\s]*)(\d+([.,]\d+)*)", 3),
    (r"price-amount amount[><:='\"\s]*<bdi>(\d+([.,]\d+)*)",1),
    (r"\"price\" style=\"display:none;\">(\d+([.,]\d+)*)",1),
    (r"([^:\-_?!\w]price)[^\-_\w]current([^\-_\w]*)[:=]([^\-_\w]*)(\d+([.,]\d+)*)", 4),
    (r"[^:\-_?!\w](price(01|02)*)[^\-_\w]*[:=][^\-_\w]*(\d+([.,]\d+)*)", 3),
    (r"[^:\-_?!\w](regularprice)[^\-_\w]*[:=][^\-_\w]*(\d+([.,]\d+)*)", 2),
    (r"[^:\-_?!\w](price(01|02)*)[^\-_\w]*(content)[^\-_\w]*(\d+([.,]\d+)*)", 4),
    (r"[^:\-_?!\w](price(01|02)*)[^\-_\w]*(data-product-price)[^\-_\w]*(\d+([.,]\d+)*)", 4),
    (r"[^:\-_?!\w](regularprice)[^\-_\w]*(value)[^\-_\w]*(\d+([.,]\d+)*)", 3),
    (r"[\"'><_\s]*formattedvalue([\"\'\s:=]*)[$£¥円€a-z\s]*(\s)*(\d+([.,]\d+)*)", 3),
    (r"[\"'><_\s]*price_formatted([\"\'\s:=]*)[$£¥円€a-z\s]*(\s)*(\d+([.,]\d+)*)", 3),
    (r"['\"><_\s]*price(01|02)*([\"\'\s:=]*)[$£¥円€a-z\s]*(\d+([.,]\d+)*)", 3),
    (r"([^:\-_?!\w]pricetotal:)[^:\-_?!\w](initialize\()(\d+([.,]\d+)*)(\))", 3),
    (r"[^:\-_?!\w](price(01|02)*)([^:\-_?!\w]*)[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 4),
    (r"[^:\-_?!\w](regularprice)([^:\-_?!\w]*)[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 3),
    (r"([^:\-_?!\w]*)(price(01|02)*)[^:\-_?!\w]*content[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 4),
    (r"(price(01|02)*)([^:\-_?!\w]*)[:=]*[^:\-_?!\w]*(\d+([.,]\d+)*)", 4),  # this accepts even e.g. abracadabraPrice = $12
    (r"[^:\-_?!\w](pricecol)['\"\s><]*span['\"\s<>]*[\D]*(\d+([.,]\d+)*)",2)
]

patterns_currencies = [
    (r"[^:\-_?!\w](currency)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](price(\s*)currency)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](price(\s*)currency)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s)*code)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s*)symbol)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency_code)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](currencyiso)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](currency(\s*)type)['\"\s]*[:=]['\"\s]*([\w$€£¥円]*)",3),

    (r"[^:\-_?!\w](product(\s)*currency)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](price(\s*)currency)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s)*code)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s*)symbol)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency_code)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](product:price:currency)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](og:price:currency)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](currency)['\"\s]*content[:='\"\s]*([\w$€£¥円]*)",2),

    (r"[^:\-_?!\w](product(\s)*currency)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](price(\s*)currency)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s)*code)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency(\s*)symbol)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",3),
    (r"[^:\-_?!\w](currency_code)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](product:price:currency)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](og:price:currency)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",2),
    (r"[^:\-_?!\w](currency)['\"\s]*value[:='\"\s]*([\w$€£¥円]*)",2),

    (r"[^:\-_?!\w](product(\s)*currency)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",5),
    (r"[^:\-_?!\w](price(\s*)currency)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",5),
    (r"[^:\-_?!\w](currency(\s)*code)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",5),
    (r"[^:\-_?!\w](currency(\s*)symbol)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",4),
    (r"[^:\-_?!\w](currency)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",4),
    (r"[^:\-_?!\w](currency_code)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",4),
    (r"[^:\-_?!\w](price_currency)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",4),
    (r"[^:\-_?!\w](product:price:amount)['\"\s]*(\&quot;:)*['\"\s:=]*([\w$€£¥円]*(\&quot;:)*)",4),

    (r"[\W](price(01|02)*)['\"><_\s:=]*([\w$€£¥円]+)",3),
    (r"[\W](regularprice)['\"><_\s:=]*([\w$€£¥円]+)", 2),
    (r"[\W](regularpriceamount)['\"><_\s:=]*([\w$€£¥円]+)", 2),
    (r"[\W](pricetotal)['\"><_\s:=]*([\w$€£¥円]+)", 2),
    (r"[\W](formattedvalue)['\"><_\s:=]*([\w$€£¥円]+)", 2),
    (r"[^:\-_?!\w](pricecol)['\"\s]*<span>['\"\s]*([\w$€£¥円]*)",2),
    (r"[\W](price(01|02)*)['\"><_\s:=]*(\d+([.,]\d*)*)(\s)*(<span>)*([\w$€£¥円]+)",7),
    (r"[\W](product_price)['\"><_\s:=]*(\d+([.,]\d*)*)(\s)*(<span>)*([\w$€£¥円]+)", 6),
    (r"_amount__currency[><:='\"\s]*([\w$€£¥円])+",1),
    (r"pricecurrency\" style=\"display:none;\">([\w$€£¥円]*)",1),
    (r"currencysymbol[><:='\"\s]*([\w$€£¥円]*)",1),
    (r"[^:\-_?!\w](pricecol)['\"\s><]*span['\"\s<>]*([\D]*)(\d+([.,]\d+)*)",2)
]