import json
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# List of categories to scrape
CATEGORIES = [
    "https://motokinisi.gr/gr/krani.html",
    "https://motokinisi.gr/gr/axesoyar-anabati.html",
    "https://motokinisi.gr/gr/axesoyar-moto.html",
    "https://motokinisi.gr/gr/endysi.html",
    "https://motokinisi.gr/gr/off-road.html",
    "https://motokinisi.gr/gr/analosima.html",
    "https://motokinisi.gr/gr/casual-lifestyle.html",
    "https://motokinisi.gr/gr/eidi-camping.html"
]

JSON_FILE = "product_list.json"

# Set up Chrome in **full** headless mode
options = Options()
options.add_argument("--headless=new")  # Proper headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-extensions")
options.add_argument("--disable-software-rasterizer")
options.add_argument("--log-level=3")

# Start ChromeDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Storage for all product details
all_products = []

def load_existing_data():
    """Load previously saved data to resume from where it was left."""
    if os.path.exists(JSON_FILE):
        try:
            with open(JSON_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
        except json.JSONDecodeError:
            logging.warning("JSON file is corrupted, starting fresh.")
            return []
    return []

def save_progress():
    """Save scraped products to JSON file after each category."""
    with open(JSON_FILE, "w", encoding="utf-8") as file:
        json.dump(all_products, file, ensure_ascii=False, indent=4)
    logging.info(f"Progress saved: {len(all_products)} products stored.")

def fetch_page_content(url):
    """Fetches HTML content from a given URL using Selenium."""
    logging.info(f"Fetching: {url}")
    driver.get(url)
    time.sleep(5)  # Allow time for JavaScript to render
    return driver.page_source

def parse_products(html_content, category):
    """Parses product details from HTML using BeautifulSoup."""
    soup = BeautifulSoup(html_content, "html.parser")
    products = soup.find_all("div", class_="mb-7")
    
    if not products:
        logging.warning(f"No products found in {category}. This might be the last page.")
        return None  # Signals to stop pagination

    extracted_products = []
    
    for product in products:
        try:
            name_tag = product.select_one(".product-name a")
            name = name_tag.get_text(strip=True) if name_tag else "Name not found"
            product_url = name_tag["href"] if name_tag else "#"

            sku_tag = product.select_one(".product-sku")
            sku = sku_tag.get_text(strip=True).replace("SKU:", "").strip() if sku_tag else "SKU not found"

            price_tag = product.select_one(".price-box .price")
            price = price_tag.get_text(strip=True) if price_tag else "Price not found"

            image_tag = product.select_one("img.product-image-photo")
            image_url = (
                image_tag["data-src"]
                if image_tag and "data-src" in image_tag.attrs
                else (image_tag["src"] if image_tag else "Image URL not found")
            )

            extracted_products.append({
                "category": category,
                "name": name,
                "url": product_url,
                "sku": sku,
                "price": price,
                "image_url": image_url
            })
        except Exception as e:
            logging.error("Error parsing product details", exc_info=True)

    return extracted_products

def scrape_category(category_url):
    """Scrapes a category by paginating through all pages."""
    category_name = category_url.split("/")[-1].replace(".html", "")
    
    # Skip if category was already scraped
    existing_categories = {product["category"] for product in all_products}
    if category_name in existing_categories:
        logging.info(f"Skipping {category_name}, already scraped.")
        return

    logging.info(f"Starting scrape for category: {category_name}")
    
    page = 1
    while True:
        url = f"{category_url}?p={page}"
        html_content = fetch_page_content(url)
        products = parse_products(html_content, category_name)

        if not products:
            logging.info(f"No more products found in {category_name}. Stopping pagination.")
            break  # Stop if no more products found

        all_products.extend(products)
        save_progress()  # Save progress after scraping each page
        page += 1  # Move to the next page

    logging.info(f"Scraped {len([p for p in all_products if p['category'] == category_name])} products from {category_name}.")

def scrape_all_categories():
    """Loops through all categories and scrapes each one."""
    for category in CATEGORIES:
        scrape_category(category)

def save_to_html():
    """Saves extracted products to an HTML file."""
    with open("product_list.html", "w", encoding="utf-8") as file:
        file.write("<html><body><h1>Product List</h1><ul>")
        
        for product in all_products:
            file.write(f"""
                <li>
                    <img src="{product['image_url']}" alt="{product['name']}" style="width:100px;"><br>
                    <a href="{product['url']}">{product['name']}</a><br>
                    SKU: {product['sku']}<br>
                    Price: {product['price']}<br>
                    Category: {product['category']}
                </li>
            """)

        file.write("</ul></body></html>")
    
    logging.info("Product list saved to product_list.html")

def main():
    global all_products
    all_products = load_existing_data()  # Load existing data for resume functionality
    scrape_all_categories()
    save_progress()  # Ensure final save
    save_to_html()
    driver.quit()  # Close Selenium driver

if __name__ == "__main__":
    main()
