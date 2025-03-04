#!/usr/bin/env python3
import argparse
import clickhouse_connect
import sys
import time
from datetime import datetime

def log(message):
    """Print a timestamped log message."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def get_tables(client):
    """Get all tables from the database."""
    result = client.query("SHOW TABLES")
    return [row[0] for row in result.result_rows]

def get_table_schema(client, table_name):
    """Get the CREATE TABLE statement for a table."""
    result = client.query(f"SHOW CREATE TABLE {table_name}")
    return result.result_rows[0][0]

def check_if_table_exists(client, table_name):
    """Check if a table exists in the database."""
    try:
        client.query(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False

def get_row_count(client, table_name):
    """Get the number of rows in a table."""
    result = client.query(f"SELECT count() FROM {table_name}")
    return result.result_rows[0][0]

def sync_table(remote_client, local_client, table_name, batch_size=10000, wipe_tables=False):
    """Sync a table from remote to local ClickHouse."""
    # Check if table exists locally and handle accordingly
    if check_if_table_exists(local_client, table_name):
        if wipe_tables:
            log(f"Dropping existing table {table_name} locally...")
            try:
                local_client.command(f"DROP TABLE {table_name}")
                log(f"Table {table_name} dropped.")
            except Exception as e:
                log(f"Error dropping table {table_name}: {e}")
                return
    
    # Create table if it doesn't exist
    if not check_if_table_exists(local_client, table_name):
        log(f"Creating table {table_name} locally...")
        schema = get_table_schema(remote_client, table_name)
        local_client.command(schema)
        log(f"Table {table_name} created.")
    else:
        log(f"Table {table_name} already exists locally.")
    
    # Get total row count to track progress
    row_count = get_row_count(remote_client, table_name)
    log(f"Table {table_name} has {row_count} rows to sync.")
    
    if row_count == 0:
        log(f"Table {table_name} is empty, nothing to sync.")
        return
    
    # Get column names
    columns_query = remote_client.query(f"DESCRIBE TABLE {table_name}")
    columns = [row[0] for row in columns_query.result_rows]
    column_str = ", ".join(columns)
    
    # Clear local table data if not wiped already
    if not wipe_tables:
        log(f"Truncating local table {table_name}...")
        try:
            local_client.command(f"TRUNCATE TABLE {table_name}")
        except Exception as e:
            log(f"Warning: Unable to truncate table: {e}")
            log("Proceeding with insertion anyway...")
    
    # Get primary key column(s) for ordering (if any)
    try:
        pk_query = remote_client.query(f"SHOW CREATE TABLE {table_name}")
        create_stmt = pk_query.result_rows[0][0]
        
        # Try to extract ORDER BY clause
        order_by = None
        if "ORDER BY" in create_stmt:
            order_parts = create_stmt.split("ORDER BY")
            if len(order_parts) > 1:
                order_clause = order_parts[1].strip()
                # Extract the column names from the ORDER BY clause
                # This handles different formats like "ORDER BY col" or "ORDER BY (col1, col2)"
                if order_clause.startswith("("):
                    end_paren = order_clause.find(")")
                    if end_paren > 0:
                        order_by = order_clause[1:end_paren].strip()
                else:
                    # Find the end of the ORDER BY clause (usually ends with a space followed by a keyword)
                    keywords = ["SETTINGS", "PRIMARY KEY", "PARTITION BY", ";"]
                    end_pos = len(order_clause)
                    for keyword in keywords:
                        pos = order_clause.find(keyword)
                        if pos > 0 and pos < end_pos:
                            end_pos = pos
                    order_by = order_clause[:end_pos].strip()
    except Exception:
        order_by = None
    
    # Fetch and insert data in batches
    offset = 0
    total_rows_synced = 0
    start_time = time.time()
    
    while offset < row_count:
        # Use ORDER BY with primary key if available, otherwise just use LIMIT/OFFSET
        if order_by:
            query = f"SELECT {column_str} FROM {table_name} ORDER BY {order_by} LIMIT {batch_size} OFFSET {offset}"
        else:
            query = f"SELECT {column_str} FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        
        try:
            batch_data = remote_client.query(query)
            
            if not batch_data.result_rows:
                break
            
            # Insert data into local table
            local_client.insert(table_name, batch_data.result_rows, column_names=columns)
            batch_row_count = len(batch_data.result_rows)
            total_rows_synced += batch_row_count
            
            # Calculate progress and estimated time remaining
            progress = min(100, total_rows_synced * 100 / row_count)
            elapsed_time = time.time() - start_time
            rows_per_second = total_rows_synced / elapsed_time if elapsed_time > 0 else 0
            estimated_remaining = (row_count - total_rows_synced) / rows_per_second if rows_per_second > 0 else 0
            
            log(f"Synced {total_rows_synced}/{row_count} rows ({progress:.1f}%) of {table_name}. "
                f"Rate: {rows_per_second:.1f} rows/sec. "
                f"Est. remaining: {estimated_remaining:.1f} sec.")
            
        except Exception as e:
            log(f"Error at offset {offset}: {e}")
            log("Trying alternative query without ordering...")
            
            try:
                # Fallback query without ORDER BY
                fallback_query = f"SELECT {column_str} FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                batch_data = remote_client.query(fallback_query)
                
                if not batch_data.result_rows:
                    break
                
                # Insert data into local table
                local_client.insert(table_name, batch_data.result_rows, column_names=columns)
                batch_row_count = len(batch_data.result_rows)
                total_rows_synced += batch_row_count
                
                log(f"Synced {total_rows_synced}/{row_count} rows ({progress:.1f}%) of {table_name} with fallback query.")
            except Exception as inner_e:
                log(f"Error with fallback query at offset {offset}: {inner_e}")
                log("Continuing with next batch...")
        
        offset += batch_size
    
    log(f"Completed sync of table {table_name}. {total_rows_synced} rows transferred.")

def main():
    parser = argparse.ArgumentParser(description="Sync data from remote ClickHouse to local instance")
    parser.add_argument("--remote-host", required=True, help="Remote ClickHouse host")
    parser.add_argument("--remote-port", type=int, default=8123, help="Remote ClickHouse HTTP port")
    parser.add_argument("--remote-user", default="default", help="Remote ClickHouse username")
    parser.add_argument("--remote-password", default="", help="Remote ClickHouse password")
    parser.add_argument("--remote-database", default="default", help="Remote ClickHouse database")
    parser.add_argument("--local-host", default="localhost", help="Local ClickHouse host")
    parser.add_argument("--local-port", type=int, default=8123, help="Local ClickHouse HTTP port")
    parser.add_argument("--local-user", default="default", help="Local ClickHouse username")
    parser.add_argument("--local-password", default="", help="Local ClickHouse password")
    parser.add_argument("--local-database", default="default", help="Local ClickHouse database")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for data transfer")
    parser.add_argument("--tables", help="Comma-separated list of tables to sync (default: all tables)")
    parser.add_argument("--wipe-tables", action="store_true", help="Drop existing local tables before recreating them")
    
    args = parser.parse_args()
    
    # Connect to remote ClickHouse
    log(f"Connecting to remote ClickHouse at {args.remote_host}:{args.remote_port}...")
    try:
        remote_client = clickhouse_connect.get_client(
            host=args.remote_host,
            port=args.remote_port,
            username=args.remote_user,
            password=args.remote_password,
            database=args.remote_database
        )
        log("Connected to remote ClickHouse.")
    except Exception as e:
        log(f"Error connecting to remote ClickHouse: {e}")
        sys.exit(1)
    
    # Connect to local ClickHouse
    log(f"Connecting to local ClickHouse at {args.local_host}:{args.local_port}...")
    try:
        local_client = clickhouse_connect.get_client(
            host=args.local_host,
            port=args.local_port,
            username=args.local_user,
            password=args.local_password,
            database=args.local_database
        )
        log("Connected to local ClickHouse.")
    except Exception as e:
        log(f"Error connecting to local ClickHouse: {e}")
        sys.exit(1)
    
    # Create local database if it doesn't exist
    try:
        local_client.command(f"CREATE DATABASE IF NOT EXISTS {args.local_database}")
        log(f"Ensured database {args.local_database} exists locally.")
    except Exception as e:
        log(f"Error creating local database: {e}")
        sys.exit(1)
    
    # Get tables to sync
    if args.tables:
        tables_to_sync = [t.strip() for t in args.tables.split(',')]
        log(f"Will sync specified tables: {', '.join(tables_to_sync)}")
    else:
        tables_to_sync = get_tables(remote_client)
        log(f"Found {len(tables_to_sync)} tables to sync: {', '.join(tables_to_sync)}")
    
    # Sync each table
    for table in tables_to_sync:
        log(f"Starting sync for table: {table}")
        try:
            sync_table(remote_client, local_client, table, args.batch_size, args.wipe_tables)
        except Exception as e:
            log(f"Error syncing table {table}: {e}")
    
    log("Sync process completed.")

if __name__ == "__main__":
    main()