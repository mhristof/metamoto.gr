from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import clickhouse_connect

app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)

# Connect to ClickHouse (host is localhost, no username or password required)
client = clickhouse_connect.get_client(host='localhost')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/products', methods=['GET'])
def get_products():
    query = request.args.get('query', '')
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 20))

    sql = """
        SELECT 
            p.sku,
            argMax(m.name, p.timestamp) AS name,
            argMax(m.category, p.timestamp) AS category,
            argMax(m.url, p.timestamp) AS url,
            argMax(m.image_url, p.timestamp) AS image_url,
            argMax(p.price, p.timestamp) AS price,
            max(p.timestamp) AS timestamp
        FROM default.products p
        JOIN default.product_metadata m ON p.sku = m.sku
        WHERE m.name ILIKE %(query)s OR m.category ILIKE %(query)s
        GROUP BY p.sku
        ORDER BY name ASC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    
    params = {
        'query': f"%{query}%",
        'limit': limit,
        'offset': offset
    }
    
    results = client.query(sql, params).result_rows

    products = [
        {
            "sku": row[0],
            "name": row[1],
            "category": row[2],
            "url": row[3],
            "image_url": row[4],
            "price": float(row[5]),  # Make sure to adjust if needed
            "timestamp": row[6]
        }
        for row in results
    ]

    return jsonify(products)

# New route for fetching price history
@app.route('/price-history', methods=['GET'])
def price_history():
    sku = request.args.get('sku', '')

    if not sku:
        return jsonify({"error": "Missing SKU"}), 400

    sql = """
        SELECT timestamp, price
        FROM default.products
        WHERE sku = %(sku)s
        ORDER BY timestamp ASC
    """
    
    results = client.query(sql, {'sku': sku}).result_rows

    history = {
        "dates": [row[0].strftime('%Y-%m-%d') for row in results],
        "prices": [float(row[1]) / 10 for row in results]  # Adjust factor if needed
    }

    return jsonify(history)

if __name__ == '__main__':
    app.run(debug=True)
