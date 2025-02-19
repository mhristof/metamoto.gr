import os
import time
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import clickhouse_connect

app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)

# Connect to ClickHouse (host is localhost, no username or password required)

for i in range(5):
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost")
        )

        break
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
        time.sleep(5)

        continue

# check show tables

if client.query("SHOW TABLES").result_rows:
    print("Connected to ClickHouse")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/products", methods=["GET"])
def get_products():
    query = request.args.get("query", "")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 20))

    sql = """
        SELECT
            p.sku,
            argMax(m.name, p.timestamp) AS name,
            argMax(m.url, p.timestamp) AS url,
            argMax(m.image_url, p.timestamp) AS image_url,
            argMax(p.price, p.timestamp) AS price,
            max(p.timestamp) AS timestamp
        FROM default.products p
        JOIN default.product_metadata m ON p.sku = m.sku
        WHERE m.name ILIKE %(query)s OR m.sku ILIKE %(query)s
        GROUP BY p.sku
        ORDER BY name ASC
        LIMIT %(limit)s OFFSET %(offset)s
    """

    params = {"query": f"%{query}%", "limit": limit, "offset": offset}

    results = client.query(sql, params).result_rows

    products = [
        {
            "sku": row[0],
            "name": row[1],
            "url": row[2],
            "image_url": row[3],
            "price": float(row[4]),
            "timestamp": row[5],
        }
        for row in results
    ]

    return jsonify(products)


# New route for fetching price history
@app.route("/price-history", methods=["GET"])
def price_history():
    sku = request.args.get("sku", "")

    if not sku:
        return jsonify({"error": "Missing SKU"}), 400

    sql = """
        SELECT timestamp, price
        FROM default.products
        WHERE sku = %(sku)s
        ORDER BY timestamp ASC
    """

    results = client.query(sql, {"sku": sku}).result_rows

    history = {
        "dates": [row[0].strftime("%Y-%m-%d") for row in results],
        "prices": [float(row[1]) / 10 for row in results],  # Adjust factor if needed
    }

    return jsonify(history)


@app.route("/stats", methods=["GET"])
def stats():
    # distinct products
    total_distinct_products = client.query(
        "SELECT COUNT(DISTINCT sku) FROM default.product_metadata"
    ).result_rows[0][0]

    total_products = client.query(
        "SELECT COUNT(sku) FROM default.product_metadata"
    ).result_rows[0][0]

    entries_per_day = {
        row[0].strftime("%Y-%m-%d"): row[1]
        for row in client.query(
            "SELECT timestamp, COUNT(*) AS entry_count FROM default.products GROUP BY timestamp ORDER BY timestamp ASC"
        ).result_rows
    }

    return jsonify(
        {
            "total_products": total_products,
            "total_distinct_products": total_distinct_products,
            "entries_per_day": entries_per_day,
        }
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
