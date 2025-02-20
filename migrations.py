#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#

import os
import clickhouse_connect

# Table names
CLICKHOUSE_TABLE_PRODUCTS = "products"
CLICKHOUSE_TABLE_METADATA = "product_metadata"
MIGRATIONS_TABLE = "schema_migrations"

# Get ClickHouse client
client = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "localhost"))


def create_migrations_table():
    """
    Create a migrations table if it doesn't exist.
    This table is used to record which migrations have been applied.
    """
    client.command(
        f"""
    CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
        id String,
        applied_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree()
    ORDER BY id
    SETTINGS index_granularity = 8192;
    """
    )


def migration_applied(migration_id):
    """
    Check if a migration with the given ID has already been applied.
    """
    result = client.query(
        f"SELECT count() FROM {MIGRATIONS_TABLE} WHERE id = '{migration_id}'"
    ).result_rows
    return result[0][0] > 0


def record_migration(migration_id):
    """
    Record the given migration as applied.
    """
    client.command(f"INSERT INTO {MIGRATIONS_TABLE} (id) VALUES ('{migration_id}')")


def run_migration(migration_id, sql):
    """
    Run the provided migration SQL and record its execution.
    """
    print(f"Applying migration {migration_id}...")
    client.command(sql)
    record_migration(migration_id)
    print(f"Migration {migration_id} applied.")


def main():
    # Ensure the migrations table exists
    create_migrations_table()

    # Define your migrations in order. You can add more migrations to this list.
    migrations = [
        {"id": "001_create_database", "sql": "CREATE DATABASE IF NOT EXISTS default"},
        {
            "id": "002_create_products_table",
            "sql": f"""
CREATE TABLE {CLICKHOUSE_TABLE_PRODUCTS}
(
    sku String,
    price Decimal(10, 2) DEFAULT 0.,
    timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (sku, timestamp)
SETTINGS index_granularity = 8192;
""",
        },
        {
            "id": "003_create_product_metadata_table",
            "sql": f"""
CREATE TABLE {CLICKHOUSE_TABLE_METADATA}
(
    sku String,
    name LowCardinality(String),
    url LowCardinality(String),
    image_url LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY sku
SETTINGS index_granularity = 8192;
""",
        },
    ]

    # Iterate through migrations and apply any that haven't been run yet
    for migration in migrations:
        if not migration_applied(migration["id"]):
            run_migration(migration["id"], migration["sql"])
        else:
            print(f"Skipping migration {migration['id']}: already applied.")


if __name__ == "__main__":
    main()
