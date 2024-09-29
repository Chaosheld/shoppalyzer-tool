from bs4 import BeautifulSoup

# TODO: store as maintainable lookup elsewhere
TECHNOLOGIES = {
    'Shopsoftware': {
        'Shopify': ['cdn.shopify.com', 'shopify_pay', 'myshopify.com'],
        'Magento': ['Mage.Cookies', 'frontend/base/default/', 'Mage/Customer/'],
        'WooCommerce': ['woocommerce', 'wp-content/plugins/woocommerce', 'wc-ajax'],
        'PrestaShop': ['PrestaShop-', 'prestashop.com'],
        'OpenCart': ['index.php?route=', 'catalog/view/theme/'],
        'BigCommerce': ['bigcommerce.com', 'cdn.bigcommerce.com']
    },
    'Marketing Tools': {
        'Google Analytics': ['www.google-analytics.com/analytics.js', '_ga', '_gid'],
        'Facebook Pixel': ['fbevents.js', 'fbq(\'init\''],
        'Hotjar': ['static.hotjar.com/c/hotjar-', 'h._hjSettings'],
        'Klaviyo': ['klaviyo.js', 'a.klaviyo.com']
    },
    'Payment Gateways': {
        'PayPal': ['paypal.com', 'www.paypalobjects.com'],
        'Stripe': ['js.stripe.com/v3/'],
        'Square': ['squareup.com', 'square.js'],
        'Klarna': ['js.klarna.com', 'cdn.klarna.com']
    },
    'Shipping Integrationen': {
        'ShipStation': ['shipstation.com', 'ssapi.shipstation.com'],
        'AfterShip': ['aftership.com', 'cdn.aftership.com'],
        'FedEx': ['fedex.com', 'www.fedex.com/apps/fedextrack'],
        'UPS': ['ups.com/track', 'ups.com/assets/']
    }
}

def get_technology(html_content, headers):
    found_technologies = {}
    soup = BeautifulSoup(html_content, 'html.parser')

    for topic, content in TECHNOLOGIES.items():
        found_technologies[topic] = []

    if html_content:
        print('html_content is checked')
        for tag in soup.find_all(['script', 'link']):
            if tag.get('src'):
                for topic, content in TECHNOLOGIES.items():
                    for tech, patterns in content.items():
                        if any(pattern in tag['src'] for pattern in patterns):
                            found_technologies[topic].append(tech)
            if tag.get('href'):
                for topic, content in TECHNOLOGIES.items():
                    for tech, patterns in content.items():
                        if any(pattern in tag['href'] for pattern in patterns):
                            found_technologies[topic].append(tech)

    if headers:
        print('headers are checked')
        for topic, content in TECHNOLOGIES.items():
            for tech, patterns in content.items():
                if any(pattern in str(headers) for pattern in patterns):
                    found_technologies[topic].append(tech)

    if found_technologies:
        print(f"Technologies detected: {found_technologies}")
    else:
        print(f"No technologies detected.")