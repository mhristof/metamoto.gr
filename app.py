import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import clickhouse_connect

app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)

clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
client = clickhouse_connect.get_client(host=clickhouse_host)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/products", methods=["GET"])
def get_products():
    query = request.args.get("query", "")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 20))

    # Split the search query into individual terms
    terms = query.strip().split() if query.strip() else []
    params = {"limit": limit, "offset": offset}

    # Build the WHERE clause dynamically based on search terms.
    # Each term must be present in either m.name or m.sku.

    if terms:
        conditions = []

        for i, term in enumerate(terms):
            param_name = f"term{i}"
            conditions.append(
                f"(m.name ILIKE %({param_name})s OR m.sku ILIKE %({param_name})s)"
            )
            params[param_name] = f"%{term}%"
        where_clause = " AND ".join(conditions)
    else:
        where_clause = "1"  # No filtering if no query provided

    sql = f"""
        SELECT
            p.sku,
            argMax(m.name, p.timestamp) AS name,
            argMax(m.url, p.timestamp) AS url,
            argMax(m.image_url, p.timestamp) AS image_url,
            argMax(p.price, p.timestamp) AS price,
            max(p.timestamp) AS timestamp
        FROM default.products p
        JOIN default.product_metadata m ON p.sku = m.sku
        WHERE {where_clause}
        GROUP BY p.sku
        ORDER BY name ASC
        LIMIT %(limit)s OFFSET %(offset)s
    """

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

    duplicate_skus = client.query(
        "SELECT sku, COUNT(*) AS count FROM default.product_metadata GROUP BY sku HAVING count > 1"
    ).result_rows

    return jsonify(
        {
            "total_products": total_products,
            "total_distinct_products": total_distinct_products,
            "entries_per_day": entries_per_day,
            "duplicate_skus": duplicate_skus,
            "items_per_host": client.query(
                "SELECT replaceRegexpOne(url, '^https?://([^/]+).*', '\\1') AS hostname, count(*) AS count FROM default.product_metadata GROUP BY hostname ORDER BY count DESC"
            ).result_rows,
        }
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
