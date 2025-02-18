import json
import os
import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# File path
PRODUCT_LIST_FILE = "product_list.json"

# Set up Chrome in headless mode
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Start ChromeDriver
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Load existing product data and ensure it's a dictionary

if os.path.exists(PRODUCT_LIST_FILE):
    with open(PRODUCT_LIST_FILE, "r", encoding="utf-8") as file:
        try:
            product_data = json.load(file)

            if not isinstance(product_data, dict):
                logging.warning(
                    "⚠️ `product_list.json` was a list, converting to dictionary."
                )
                product_data = {p["sku"]: p for p in product_data if "sku" in p}
        except json.JSONDecodeError:
            logging.error("Failed to decode product_list.json. Starting fresh.")
            product_data = {}
else:
    product_data = {}


def save_product_data():
    """Save updated product list with price history."""
    with open(PRODUCT_LIST_FILE, "w", encoding="utf-8") as file:
        json.dump(product_data, file, ensure_ascii=False, indent=4)
    logging.info(f"✅ Product data saved: {len(product_data)} products tracked.")


def update_price_history(sku, new_price):
    """Update the product price history, only adding new price entries when price changes."""
    today = datetime.today().strftime("%Y-%m-%d")

    if "price_history" not in product_data[sku]:
        product_data[sku]["price_history"] = []

    history = product_data[sku]["price_history"]

    # Only add price if it's different from the last entry

    if not history or history[-1]["price"] != new_price:
        history.append({"date": today, "price": new_price})


def fetch_page_content(url, page):
    """Fetch HTML content using Selenium, wait for products, and dump to file."""
    logging.info(f"🚀 Fetching: {url}")
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-item"))
        )
    except:
        logging.warning("⚠️ Page loaded but no products found. Possibly last page.")

    time.sleep(3)

    # Save HTML snapshot for debugging
    html_content = driver.page_source
    debug_filename = f"debug_page_{page}.html"
    with open(debug_filename, "w", encoding="utf-8") as f:
        f.write(html_content)
    logging.info(f"📄 Saved page HTML to {debug_filename}")

    return html_content


import re


def parse_products(html_content, category):
    """Parses product details from HTML using BeautifulSoup and updates product_data."""
    soup = BeautifulSoup(html_content, "html.parser")
    products = soup.find_all("div", class_="product-item")

    logging.info(f"🔍 Products found: {len(products)} on this page.")

    for product in products:
        try:
            name_tag = product.select_one(".product-name a")
            name = name_tag.get_text(strip=True) if name_tag else None
            product_url = name_tag["href"] if name_tag else None

            sku_tag = product.select_one(".product-sku")
            sku = (
                sku_tag.get_text(strip=True).replace("SKU:", "").strip()
                if sku_tag
                else None
            )

            price_tag = product.select_one(".price-box .price")
            raw_price = price_tag.get_text(strip=True) if price_tag else None

            # 🛠 FIX: Correct price parsing (remove thousands separator)

            if raw_price:
                cleaned_price = re.sub(
                    r"[^\d,]", "", raw_price
                )  # Remove non-numeric except commas
                cleaned_price = cleaned_price.replace(
                    ",", "."
                )  # Convert comma to dot for decimals
                new_price = float(cleaned_price) if cleaned_price else None
            else:
                new_price = None

            image_tag = product.select_one("img.product-image-photo")
            image_url = (
                image_tag["data-src"]
                if image_tag and "data-src" in image_tag.attrs
                else (
                    image_tag["src"] if image_tag else "https://via.placeholder.com/150"
                )
            )

            if not name or not product_url or not sku or new_price is None:
                continue

            # Ensure `product_data` is a dictionary and update product

            if sku not in product_data:
                product_data[sku] = {
                    "sku": sku,
                    "category": category,
                    "name": name,
                    "url": product_url,
                    "image_url": image_url,
                    "price_history": [],
                }

            update_price_history(sku, new_price)  # Update price history

        except Exception as e:
            logging.error("❌ Error parsing product details", exc_info=True)


def scrape_category(category_url):
    """Scrapes all pages of a category and logs detected pages."""
    category_name = category_url.split("/")[-1].replace(".html", "")
    logging.info(f"📂 Scraping category: {category_name}")

    page = 1

    while True:
        page_url = f"{category_url}?p={page}&product_list_limit=45"
        html_content = fetch_page_content(page_url, page)
        parse_products(html_content, category_name)  # Parse before saving

        # Save JSON after parsing the page
        save_product_data()

        # Check if there is a next page
        soup = BeautifulSoup(html_content, "html.parser")
        next_page_btn = soup.select_one(".pages-item-next")

        if not next_page_btn:
            logging.info(
                f"✅ No 'Next Page' button found. Stopping pagination for {category_name}."
            )

            break

        logging.info(f"➡️ 'Next Page' button found. Moving to page {page + 1}.")

        page += 1
        time.sleep(2)


def main():
    categories = [
        "https://motokinisi.gr/gr/krani.html",
        "https://motokinisi.gr/gr/axesoyar-anabati.html",
        "https://motokinisi.gr/gr/axesoyar-moto.html",
        "https://motokinisi.gr/gr/endysi.html",
        "https://motokinisi.gr/gr/off-road.html",
        "https://motokinisi.gr/gr/analosima.html",
        "https://motokinisi.gr/gr/casual-lifestyle.html",
        "https://motokinisi.gr/gr/eidi-camping.html",
    ]

    for category in categories:
        scrape_category(category)

    driver.quit()


if __name__ == "__main__":
    main()
