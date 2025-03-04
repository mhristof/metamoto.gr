import os
import logging
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import clickhouse_connect

app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)


# Define a filter to hide logs that include "/health"
class HealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()


# Apply the filter to the Werkzeug logger
werkzeug_logger = logging.getLogger("werkzeug")
werkzeug_logger.addFilter(HealthFilter())

clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "localhost")


def get_ch_client():
    """Create and return a new ClickHouse client instance."""

    return clickhouse_connect.get_client(host=clickhouse_host)


@app.route("/")
def index():
    git_version = os.environ.get("GIT_VERSION", "unknown")

    return render_template("index.html", git_version=git_version)


@app.route("/products", methods=["GET"])
def get_products():
    client = get_ch_client()
    query = request.args.get("query", "")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 20))

    # Build dynamic filtering
    terms = query.strip().split() if query.strip() else []
    params = {"limit": limit, "offset": offset}

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
        where_clause = "1"  # No filtering

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


@app.route("/price-history", methods=["GET"])
def price_history():
    client = get_ch_client()
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
        "prices": [float(row[1]) / 10 for row in results],
    }
    return jsonify(history)


@app.route("/similar-products", methods=["GET"])
def similar_products():
    client = get_ch_client()
    sku = request.args.get("sku", "")
    if not sku:
        return jsonify({"error": "Missing SKU"}), 400

    group_query = """
        SELECT group_id
        FROM default.product_similarity_groups
        WHERE product_id = %(sku)s
        LIMIT 1
    """
    try:
        group_result = client.query(group_query, {"sku": sku}).result_rows
        if not group_result:
            return jsonify({"similar_products": []})
        group_id = group_result[0][0]

        similar_query = """
            SELECT
                p.product_id,
                p.name,
                p.url,
                p.image_url,
                p.shop_domain,
                p.similarity,
                COALESCE(latest_price.price, 0) as price
            FROM default.product_similarity_groups p
            LEFT JOIN (
                SELECT sku, argMax(price, timestamp) as price
                FROM default.products
                GROUP BY sku
            ) as latest_price ON p.product_id = latest_price.sku
            WHERE p.group_id = %(group_id)s
            AND p.product_id != %(sku)s
            ORDER BY p.similarity DESC
        """
        similar_result = client.query(
            similar_query, {"group_id": group_id, "sku": sku}
        ).result_rows

        similar_products = [
            {
                "sku": row[0],
                "name": row[1],
                "url": row[2],
                "image_url": row[3],
                "shop": row[4],
                "similarity": float(row[5]),
                "price": float(row[6]),
            }
            for row in similar_result
        ]
        return jsonify({"group_id": group_id, "similar_products": similar_products})
    except Exception as e:
        app.logger.error(f"Error fetching similar products: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/stats", methods=["GET"])
def stats():
    client = get_ch_client()
    total_distinct_products = client.query(
        "SELECT COUNT(DISTINCT sku) FROM default.product_metadata"
    ).result_rows[0][0]

    total_products = client.query(
        "SELECT COUNT(sku) FROM default.product_metadata"
    ).result_rows[0][0]

    entries_query = client.query(
        "SELECT timestamp, COUNT(*) AS entry_count FROM default.products GROUP BY timestamp ORDER BY timestamp ASC"
    )
    entries_per_day = {
        row[0].strftime("%Y-%m-%d"): row[1] for row in entries_query.result_rows
    }

    duplicate_skus = client.query(
        "SELECT sku, COUNT(*) AS count FROM default.product_metadata GROUP BY sku HAVING count > 1"
    ).result_rows

    items_per_host = client.query(
        "SELECT replaceRegexpOne(url, '^https?://([^/]+).*', '\\1') AS hostname, count(*) AS count FROM default.product_metadata GROUP BY hostname ORDER BY count DESC"
    ).result_rows

    return jsonify(
        {
            "total_products": total_products,
            "total_distinct_products": total_distinct_products,
            "entries_per_day": entries_per_day,
            "duplicate_skus": duplicate_skus,
            "items_per_host": items_per_host,
        }
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
