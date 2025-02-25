import os
import time
import logging
import requests
import clickhouse_connect
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional

REQUEST_DELAY = 2  # Delay between requests in seconds

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse client
CLICKHOUSE_TABLE_PRODUCTS = "products"
CLICKHOUSE_TABLE_METADATA = "product_metadata"

client = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "localhost"))


def fetch_page(url: str) -> Optional[str]:
    """Fetch a page using requests."""
    try:
        response = requests.get(url)
        response.raise_for_status()

        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch page: {url} - {e}")

        return None


def clean_price(raw_price: str) -> Optional[float]:
    """Convert raw price text to a Decimal-friendly float."""

    if not raw_price:
        return None
    try:
        cleaned_price = raw_price.replace("€", "").replace(",", "").strip()

        return float(cleaned_price) / 100
    except ValueError:
        logger.warning(f"Skipping invalid price: {raw_price}")

        return None


def parse_product_data(soup: BeautifulSoup):
    """Parse product data and insert into ClickHouse."""
    product_items = soup.find_all("div", class_="product-item")
    logger.info(f"Found {len(product_items)} products on the page.")

    batch_products = []
    batch_metadata = []

    now = datetime.now(timezone.utc)
    now_date = datetime(now.year, now.month, now.day)  # , tzinfo=timezone.utc)

    for item in product_items:
        try:
            name_tag = item.select_one(".product-name a")
            name = name_tag.get_text(strip=True) if name_tag else "Unknown"
            url = name_tag["href"] if name_tag else "#"

            sku_tag = item.find("div", class_="product-sku")
            sku = (
                sku_tag.get_text(strip=True).replace("SKU:", "").strip()
                if sku_tag
                else "unknown"
            )

            price_tag = item.find("span", class_="price")
            raw_price = price_tag.get_text(strip=True) if price_tag else None
            price = clean_price(raw_price) or 0.00

            image_tag = item.find("img", class_="product-image-photo")
            image_url = ""

            if image_tag:
                image_url = image_tag.get("src", "")
                # Check if the URL contains "lazy.svg" regardless of the full URL

                if "lazy.svg" in image_url:
                    image_url = image_tag.get("data-src", image_url)

            batch_products.append((sku, price, now_date))
            batch_metadata.append((sku, name, url, image_url))
        except Exception as e:
            logger.error(f"Error parsing product: {e}")

    # Insert batch into ClickHouse

    if batch_products:
        client.insert(
            f"{CLICKHOUSE_TABLE_PRODUCTS}",
            batch_products,
            column_names=["sku", "price", "timestamp"],
        )

    if batch_metadata:
        client.insert(
            f"{CLICKHOUSE_TABLE_METADATA}",
            batch_metadata,
            column_names=["sku", "name", "url", "image_url"],
        )

    logger.info(
        f"Inserted {len(batch_products)} product records and {len(batch_metadata)} metadata records into ClickHouse."
    )


def scrape_category(url: str):
    """Scrape all pages for a given category."""
    page = 1

    while True:
        logger.info(f"Scraping {url} - Page {page}...")
        page_url = f"{url}?p={page}&product_list_limit=45"
        page_content = fetch_page(page_url)

        if not page_content:
            logger.error(f"Failed to fetch page {page}. Stopping scraping.")

            break

        soup = BeautifulSoup(page_content, "html.parser")
        parse_product_data(soup)

        # Check if there are more pages
        next_page = soup.select_one("li.next a, a.next")

        if not next_page or "href" not in next_page.attrs:
            logger.info("No more pages to scrape.")

            break

        page += 1
        time.sleep(REQUEST_DELAY)


def main():
    category_urls = [
        "https://motokinisi.gr/gr/krani.html",
        "https://motokinisi.gr/gr/axesoyar-anabati.html",
        "https://motokinisi.gr/gr/axesoyar-moto.html",
        "https://motokinisi.gr/gr/endysi.html",
        "https://motokinisi.gr/gr/off-road.html",
        "https://motokinisi.gr/gr/analosima.html",
        "https://motokinisi.gr/gr/casual-lifestyle.html",
        "https://motokinisi.gr/gr/eidi-camping.html",
    ]

    for category_url in category_urls:
        scrape_category(category_url)


if __name__ == "__main__":
    main()
