import os
import re
import time
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
from clickhouse_connect import get_client

# List of category URLs to scan.
category_urls = [
    "https://www.motomarket-shop.gr/eksoplismos-anabath/kranh-endoep-nies-kameres?pn=2&taxqid=-45&pszid=120",
    "https://www.motomarket-shop.gr/eksoplismos-anabath/endysh",
    "https://www.motomarket-shop.gr/eksoplismos-anabath/mpotes",
    "https://www.motomarket-shop.gr/eksoplismos-anabath/aksesoyar-anabath",
    "https://www.motomarket-shop.gr/eksoplismos-anabath/paidika",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/balitses",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/soft-bags",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/systhmata-prosarmoghs",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/eksarthmata-aksesoyar",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/antikleptika",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/prostateytika-motosikletas",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/kalymmata-motosikletas",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/eksatmiseis",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/filtra",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/anabatoria-orthostates-motolift",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/karines-mudguards-xoyftes",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/zelatines",
    "https://www.motomarket-shop.gr/eksoplismos-motosikletas/plates-moto",
    "https://www.motomarket-shop.gr/off-road/off-road-anabaths",
    "https://www.motomarket-shop.gr/off-road/off-road-motosikleta",
    "https://www.motomarket-shop.gr/lipantika/moto-4t",
    "https://www.motomarket-shop.gr/lipantika/moto-2t",
    "https://www.motomarket-shop.gr/lipantika/scooter",
    "https://www.motomarket-shop.gr/lipantika/fork-oils",
    "https://www.motomarket-shop.gr/lipantika/chain-lubes",
    "https://www.motomarket-shop.gr/lipantika/chemicals",
]


def derive_category(url):
    """Derive a simple category name from the URL (last non-empty path segment)."""
    parts = url.rstrip("/").split("/")

    return parts[-1]


def parse_price(price_str):
    """Convert a price string (e.g. '€709,00') to a float."""

    if not price_str:
        return 0.0
    clean_str = price_str.replace("€", "").strip().replace(",", ".")
    try:
        return float(clean_str)
    except Exception as e:
        return 0.0


def build_page_url(base_url, page):
    """
    Given a base URL and a page number, return a URL with the proper 'pn=' parameter.
    If the URL already contains a pn parameter, replace it; otherwise, add it.
    """

    if "pn=" in base_url:
        new_url = re.sub(r"pn=\d+", f"pn={page}", base_url)
    else:
        sep = "&" if "?" in base_url else "?"
        new_url = base_url + f"{sep}pn={page}"

    return new_url


def scrape_products_from_page(page_url, driver):
    """
    Load the page using Selenium, wait until at least one product is loaded,
    and return a list of product dictionaries.
    """
    driver.get(page_url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul.img_o_v > li"))
        )
    except Exception:
        pass
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("ul.img_o_v > li")
    page_products = []

    for li in items:
        link_tag = li.select_one("div.img a")

        if link_tag:
            detail_url = link_tag.get("href")

            if detail_url and not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url
            image_tag = link_tag.find("img")
            image_url = image_tag.get("src") if image_tag else None

            if image_url and not image_url.startswith("http"):
                image_url = BASE_URL + image_url
        else:
            detail_url = None
            image_url = None

        title_tag = li.select_one("p.title a")
        title = title_tag.text.strip() if title_tag else None

        price_tag = li.select_one("p.price span.product-price-final")
        price_str = price_tag.text.strip() if price_tag else None
        price = parse_price(price_str)

        page_products.append(
            {
                "title": title,
                "detail_url": detail_url,
                "image_url": image_url,
                "price": price,
                "sku": None,
            }
        )

    print("Found", len(page_products), "products on page", page_url)

    return page_products


def get_sku(detail_url):
    """Fetch the product detail page and extract the SKU."""
    HEADERS = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(detail_url, headers=HEADERS)

    if resp.status_code != 200:
        return None
    detail_soup = BeautifulSoup(resp.text, "html.parser")
    sku_text = detail_soup.find(string=lambda t: "ΚΩΔΙΚΟΣ ΠΡΟΪΟΝΤΟΣ:" in t)

    if sku_text:
        parts = sku_text.split("ΚΩΔΙΚΟΣ ΠΡΟΪΟΝΤΟΣ:")

        if len(parts) > 1:
            return parts[1].strip()

    return None


def query_sku_by_url(detail_url, client):
    """Query the ClickHouse product_metadata table for a SKU using the detail URL."""
    query = "SELECT sku FROM product_metadata WHERE url = {url:String} LIMIT 1"
    result = client.query(query, parameters={"url": detail_url})

    if result.result_rows:
        return result.result_rows[0][0]

    return None


# Connect to ClickHouse.
client = get_client(host=os.getenv("CLICKHOUSE_HOST", "localhost"))

# Set up a single headless Selenium driver.
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-setuid-sandbox")
chrome_options.add_argument("--window-size=1280,800")


driver = webdriver.Chrome(options=chrome_options)

# Process each category.

for base_url in category_urls:
    category = derive_category(base_url)
    page = 1

    while True:
        page_url = build_page_url(base_url, page)
        print(f"Scanning page {page}: {page_url}")
        page_products = scrape_products_from_page(page_url, driver)

        if not page_products:
            print(
                f"No products found on page {page}. Ending scan for category: {base_url}"
            )

            break
        # For each product, check for SKU in ClickHouse; if not found, fetch it.

        for prod in page_products:
            if prod["detail_url"]:
                existing_sku = query_sku_by_url(prod["detail_url"], client)

                if existing_sku:
                    prod["sku"] = existing_sku
                else:
                    prod["sku"] = get_sku(prod["detail_url"])
            else:
                prod["sku"] = None

        # Compute UTC timestamp for today at midnight.
        now = datetime.now(timezone.utc)
        now_date = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

        # Prepare rows for insertion.
        metadata_rows = [
            (
                prod["sku"],
                prod["title"],
                prod["detail_url"],
                prod["image_url"],
            )
            for prod in page_products
            if prod["sku"]
        ]
        price_rows = [
            (prod["sku"], prod["price"], now_date)
            for prod in page_products
            if prod["sku"]
        ]

        if metadata_rows:
            client.insert("product_metadata", metadata_rows)
            print(
                f"Inserted {len(metadata_rows)} rows into product_metadata for page {page}."
            )

        if price_rows:
            client.insert(
                "products", price_rows, column_names=["sku", "price", "timestamp"]
            )
            print(f"Inserted {len(price_rows)} rows into products for page {page}.")

        page += 1

driver.quit()
