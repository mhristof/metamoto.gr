import clickhouse_connect
import requests
import os
import sys
import time
from PIL import Image
from io import BytesIO
import numpy as np
from urllib.parse import urlparse
import hashlib
import tensorflow as tf
from tensorflow.keras.applications.resnet50 import ResNet50, preprocess_input
from tensorflow.keras.preprocessing import image
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import traceback
import json

# Set up global variables
IMAGE_CACHE_DIR = "image_cache"
FEATURE_CACHE_DIR = "feature_cache"
PROCESSED_CACHE_FILE = "processed_products.json"
SIMILARITY_THRESHOLD = 0.85  # Cosine similarity threshold for matching images

# Get ClickHouse connection details from environment variables
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'default')

# Create necessary directories
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)
os.makedirs(FEATURE_CACHE_DIR, exist_ok=True)

# Initialize the ResNet50 model
def get_model():
    """Load and return the pre-trained ResNet50 model for feature extraction"""
    print("Loading ResNet50 model...")
    model = ResNet50(weights='imagenet', include_top=False, pooling='avg')
    print("Model loaded successfully.")
    return model

def extract_domain(url):
    """Extract domain from URL"""
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        # Remove www. prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ""

def get_image_hash(url):
    """Create a hash of the image URL to use as a filename"""
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def download_image(image_url, product_id):
    """Download image from URL and save to cache directory"""
    image_hash = get_image_hash(image_url)
    cache_path = os.path.join(IMAGE_CACHE_DIR, f"{image_hash}.jpg")
    
    # If image already exists in cache, return the path
    if os.path.exists(cache_path):
        return cache_path
    
    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code == 200:
            # Save image to cache
            with open(cache_path, 'wb') as f:
                f.write(response.content)
            return cache_path
        else:
            print(f"Failed to download image for product {product_id}: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading image for product {product_id}: {e}")
        return None

def extract_image_features(image_path, model):
    """Extract features from an image using the ResNet50 model"""
    try:
        img = image.load_img(image_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        features = model.predict(x, verbose=0)  # Disable verbose output
        return features.flatten()
    except Exception as e:
        print(f"Error extracting features from {image_path}: {e}")
        return None

def init_clickhouse_tables(client):
    """Initialize ClickHouse tables for storing image features and product groups"""
    # Create table for image features if it doesn't exist
    client.command("""
    CREATE TABLE IF NOT EXISTS product_image_features (
        product_id String,
        shop_domain String,
        image_url String,
        image_hash String,
        features Array(Float32),
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (shop_domain, product_id)
    """)
    
    # Create table for product groups if it doesn't exist
    client.command("""
    CREATE TABLE IF NOT EXISTS product_similarity_groups (
        group_id UInt32,
        product_id String,
        shop_domain String,
        name String,
        url String,
        image_url String,
        similarity Float32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (group_id, shop_domain, product_id)
    """)
    
    print("ClickHouse tables initialized successfully.")

def get_processed_products(client):
    """Get the list of already processed products from ClickHouse"""
    try:
        result = client.query("SELECT product_id FROM product_image_features")
        processed_ids = set()
        
        # Try different methods to access data based on API version
        try:
            # Newer versions
            for row in result.result_rows:
                processed_ids.add(row[0])
        except AttributeError:
            try:
                # Some versions
                for row in result.named_results():
                    processed_ids.add(row['product_id'])
            except AttributeError:
                # Older versions
                for row in result.rows:
                    processed_ids.add(row[0])
        
        print(f"Found {len(processed_ids)} already processed products.")
        return processed_ids
    except Exception as e:
        print(f"Error getting processed products: {e}")
        return set()

def process_product_images(products, clickhouse_client, batch_size=100):
    """Process product images and store their features in ClickHouse"""
    # Get model for feature extraction
    model = get_model()
    
    # Get already processed products
    processed_ids = get_processed_products(clickhouse_client)
    
    # Filter out already processed products
    products_to_process = [p for p in products if p[0] not in processed_ids]
    print(f"Processing {len(products_to_process)} new products out of {len(products)} total products")
    
    # Process products in batches
    total_batches = (len(products_to_process) + batch_size - 1) // batch_size
    features_batch = []
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(products_to_process))
        batch = products_to_process[start_idx:end_idx]
        
        print(f"Processing batch {batch_idx + 1}/{total_batches} ({start_idx} to {end_idx})")
        
        for product in batch:
            product_id = product[0]
            name = product[1]
            url = product[2]
            image_url = product[3]
            shop_domain = extract_domain(url)
            
            # Skip if already processed
            if product_id in processed_ids:
                continue
            
            print(f"Processing product: {product_id} - {name[:30]}...")
            
            # Download image
            image_path = download_image(image_url, product_id)
            if not image_path:
                print(f"Skipping product {product_id} due to image download failure")
                continue
            
            # Extract features
            image_hash = get_image_hash(image_url)
            features = extract_image_features(image_path, model)
            if features is None:
                print(f"Skipping product {product_id} due to feature extraction failure")
                continue
            
            # Add to batch for insertion
            features_batch.append({
                'product_id': product_id,
                'shop_domain': shop_domain,
                'image_url': image_url,
                'image_hash': image_hash,
                'features': features.tolist()
            })
            
            # Mark as processed
            processed_ids.add(product_id)
        
        # Insert batch into ClickHouse
        if features_batch:
            try:
                # Extract column names and values
                columns = list(features_batch[0].keys())
                data = [[item[col] for col in columns] for item in features_batch]
                
                # Insert data
                clickhouse_client.insert('product_image_features', data, column_names=columns)
                print(f"Inserted {len(features_batch)} product features into ClickHouse")
                
                # Clear batch
                features_batch = []
            except Exception as e:
                print(f"Error inserting batch into ClickHouse: {e}")
                traceback.print_exc()
    
    print("Finished processing all product images")

def create_product_groups(clickhouse_client):
    """Create product groups based on image similarity"""
    print("Creating product groups based on image similarity...")
    
    # Retrieve all product features from ClickHouse
    try:
        result = clickhouse_client.query("""
        SELECT 
            product_id,
            shop_domain,
            features
        FROM product_image_features
        """)
        
        # Access data based on API version
        product_features = {}
        shop_domains = {}
        
        try:
            # Newer versions
            for row in result.result_rows:
                product_id = row[0]
                shop_domain = row[1]
                features = np.array(row[2])
                product_features[product_id] = features
                shop_domains[product_id] = shop_domain
        except AttributeError:
            try:
                # Some versions
                for row in result.named_results():
                    product_id = row['product_id']
                    shop_domain = row['shop_domain']
                    features = np.array(row['features'])
                    product_features[product_id] = features
                    shop_domains[product_id] = shop_domain
            except AttributeError:
                # Older versions
                for row in result.rows:
                    product_id = row[0]
                    shop_domain = row[1]
                    features = np.array(row[2])
                    product_features[product_id] = features
                    shop_domains[product_id] = shop_domain
        
        print(f"Retrieved features for {len(product_features)} products")
        
        # Group products by shop domain
        shop_products = defaultdict(list)
        for product_id, domain in shop_domains.items():
            shop_products[domain].append(product_id)
        
        print(f"Found products from {len(shop_products)} different shops")
        
        # Create product groups
        product_groups = []
        processed_products = set()
        
        # For each product
        product_ids = list(product_features.keys())
        for i, product_id in enumerate(product_ids):
            if product_id in processed_products:
                continue
            
            if i % 100 == 0:
                print(f"Processing product {i}/{len(product_ids)}...")
            
            current_domain = shop_domains[product_id]
            current_features = product_features[product_id]
            
            # Find similar products from different shops
            group = [(product_id, 1.0)]  # (product_id, similarity)
            processed_products.add(product_id)
            
            # For each shop (except the current one)
            for other_domain, other_products in shop_products.items():
                if other_domain == current_domain:
                    continue
                
                # Find most similar product from this shop
                max_similarity = 0
                most_similar_product = None
                
                for other_id in other_products:
                    if other_id in processed_products:
                        continue
                    
                    other_features = product_features[other_id]
                    similarity = cosine_similarity(
                        current_features.reshape(1, -1),
                        other_features.reshape(1, -1)
                    )[0][0]
                    
                    if similarity > max_similarity:
                        max_similarity = similarity
                        most_similar_product = other_id
                
                # If a similar product is found and it's above threshold
                if most_similar_product and max_similarity >= SIMILARITY_THRESHOLD:
                    group.append((most_similar_product, float(max_similarity)))
                    processed_products.add(most_similar_product)
            
            # If we found cross-shop matches
            if len(group) > 1:
                product_groups.append(group)
        
        print(f"Created {len(product_groups)} product groups")
        
        # Get product metadata for the grouped products
        product_metadata = {}
        grouped_product_ids = set()
        for group in product_groups:
            for product_id, _ in group:
                grouped_product_ids.add(product_id)
        
        if not grouped_product_ids:
            print("No product groups found")
            return 0
            
        # Create IN clause for SQL query
        ids_str = "'" + "','".join(grouped_product_ids) + "'"
        
        metadata_query = clickhouse_client.query(f"""
        SELECT 
            p.*, f.shop_domain
        FROM product_metadata p
        JOIN product_image_features f ON p.sku = f.product_id
        WHERE p.sku IN ({ids_str})
        """)
        
        try:
            # Access metadata based on API version
            rows = metadata_query.result_rows
        except AttributeError:
            try:
                rows = list(metadata_query.named_results())
            except AttributeError:
                rows = metadata_query.rows
        
        # Map product ID to metadata
        for row in rows:
            if isinstance(row, dict):
                product_id = row['sku']
                product_metadata[product_id] = row
            else:
                product_id = row[0]  # Assuming sku is the first column
                product_metadata[product_id] = {
                    'sku': row[0],
                    'name': row[1],
                    'url': row[2],
                    'image_url': row[3],
                    'shop_domain': row[4]
                }
        
        # Insert groups into ClickHouse
        group_data = []
        for group_id, group in enumerate(product_groups, 1):
            for product_id, similarity in group:
                if product_id in product_metadata:
                    metadata = product_metadata[product_id]
                    
                    # Prepare data for insertion
                    if isinstance(metadata, dict):
                        group_data.append({
                            'group_id': group_id,
                            'product_id': product_id,
                            'shop_domain': metadata.get('shop_domain', ''),
                            'name': metadata.get('name', ''),
                            'url': metadata.get('url', ''),
                            'image_url': metadata.get('image_url', ''),
                            'similarity': similarity
                        })
                    else:
                        group_data.append({
                            'group_id': group_id,
                            'product_id': product_id,
                            'shop_domain': shop_domains.get(product_id, ''),
                            'name': metadata[1] if len(metadata) > 1 else '',
                            'url': metadata[2] if len(metadata) > 2 else '',
                            'image_url': metadata[3] if len(metadata) > 3 else '',
                            'similarity': similarity
                        })
        
        # Clear existing groups
        clickhouse_client.command("TRUNCATE TABLE product_similarity_groups")
        
        # Insert new groups in batches
        batch_size = 1000
        for i in range(0, len(group_data), batch_size):
            batch = group_data[i:i+batch_size]
            
            # Extract column names and values
            columns = list(batch[0].keys())
            data = [[item[col] for col in columns] for item in batch]
            
            # Insert data
            clickhouse_client.insert('product_similarity_groups', data, column_names=columns)
            print(f"Inserted batch of {len(batch)} product groups")
        
        print(f"Successfully created and stored {len(product_groups)} product groups")
        return len(product_groups)
    
    except Exception as e:
        print(f"Error creating product groups: {e}")
        traceback.print_exc()
        return 0

def display_product_groups(clickhouse_client, limit=10):
    """Display product groups from ClickHouse"""
    try:
        # Get total number of groups
        count_result = clickhouse_client.query("SELECT COUNT(DISTINCT group_id) FROM product_similarity_groups")
        total_groups = count_result.result_rows[0][0]
        
        # Get groups with most products
        groups_query = clickhouse_client.query(f"""
        SELECT 
            group_id,
            count() as product_count
        FROM product_similarity_groups
        GROUP BY group_id
        ORDER BY product_count DESC
        LIMIT {limit}
        """)
        
        top_groups = []
        for row in groups_query.result_rows:
            top_groups.append((row[0], row[1]))
        
        print(f"\n=== Found {total_groups} total product groups ===\n")
        print(f"Top {limit} largest groups:")
        
        # For each top group, get the products
        for group_id, count in top_groups:
            print(f"\nGroup {group_id} ({count} products):")
            
            products_query = clickhouse_client.query(f"""
            SELECT 
                product_id,
                shop_domain,
                name,
                url,
                image_url,
                similarity
            FROM product_similarity_groups
            WHERE group_id = {group_id}
            ORDER BY similarity DESC
            """)
            
            # Display products in this group
            shop_products = defaultdict(list)
            
            for row in products_query.result_rows:
                product_id = row[0]
                shop = row[1]
                name = row[2]
                url = row[3]
                image_url = row[4]
                similarity = row[5]
                
                shop_products[shop].append({
                    'id': product_id,
                    'name': name,
                    'url': url,
                    'image_url': image_url,
                    'similarity': similarity
                })
            
            # Print one product from each shop
            for shop, products in shop_products.items():
                product = products[0]  # Take the first product
                print(f"  - [{shop}] {product['name'][:60]}...")
                print(f"    ID: {product['id']}, Similarity: {product['similarity']:.4f}")
                print(f"    URL: {product['url']}")
                
            print(f"  Total: {count} products from {len(shop_products)} different shops")
        
        return total_groups
    
    except Exception as e:
        print(f"Error displaying product groups: {e}")
        traceback.print_exc()
        return 0

def get_products_from_clickhouse(client):
    """Retrieve products from ClickHouse database"""
    print("Retrieving products from ClickHouse...")
    try:
        # Execute the query
        result = client.query('SELECT * FROM product_metadata')
        
        # Get column names
        columns = result.column_names
        print(f"Retrieved column names: {columns}")
        
        # Try different methods to access data based on API version
        raw_data = None
        
        try:
            # Try direct access to result data via rows attribute (for newer versions)
            raw_data = result.result_rows
            print("Successfully accessed data using result_rows attribute")
        except AttributeError:
            try:
                # Try using named_results (for some versions)
                raw_data = result.named_results()
                print("Successfully accessed data using named_results() method")
            except AttributeError:
                try:
                    # Fall back to using the raw rows
                    raw_data = result.rows
                    print("Successfully accessed data using rows attribute")
                except AttributeError:
                    # Try the older API style
                    raw_data = list(result)
                    print("Successfully accessed data by iterating the result object")
        
        if not raw_data:
            print("ERROR: Could not access data from ClickHouse query result")
            sys.exit(1)
            
        print(f"Retrieved {len(raw_data)} rows of data")
        
        # Convert to list format for processing
        products = []
        for row in raw_data:
            # If row is a dict, convert to tuple
            if isinstance(row, dict):
                products.append((row.get('sku', ''), row.get('name', ''), 
                                row.get('url', ''), row.get('image_url', '')))
            else:
                # Assume it's already a tuple or list
                products.append(row)
        
        if not products:
            print("ERROR: No products found in the database")
            sys.exit(1)
            
        print(f"Data retrieval completed successfully: {len(products)} products")
        return products
        
    except Exception as e:
        print(f"ERROR: Unable to retrieve data from ClickHouse database: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print("=== Image-Based Cross-Shop Product Matcher ===")
    print("Starting process...")
    
    # Set TensorFlow to use CPU or GPU
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        print(f"Found {len(gpus)} GPU(s), using GPU acceleration")
        # Allow TensorFlow to use memory as needed
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
    else:
        print("No GPUs found, using CPU for processing")
    
    # Connect to ClickHouse using environment variables
    try:
        print("\n=== Phase 1: Connecting to ClickHouse ===")
        clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
        client = clickhouse_connect.get_client(host=clickhouse_host)
        print(f"Connected to ClickHouse successfully at {clickhouse_host}")
        
        # Initialize tables
        init_clickhouse_tables(client)
        
        # Get products
        print("\n=== Phase 2: Retrieving Products ===")
        products = get_products_from_clickhouse(client)
        
        # Process product images and extract features
        print("\n=== Phase 3: Processing Images ===")
        process_product_images(products, client)
        
        # Create product groups based on image similarity
        print("\n=== Phase 4: Creating Product Groups ===")
        num_groups = create_product_groups(client)
        
        # Display product groups
        print("\n=== Phase 5: Results ===")
        total_groups = display_product_groups(client)
        
        print(f"\nSuccessfully identified {total_groups} groups of similar products across different shops")
        print("Results are stored in the 'product_similarity_groups' table in ClickHouse")
        
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)