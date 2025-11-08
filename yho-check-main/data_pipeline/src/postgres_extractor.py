import os
import json
import psycopg2
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timezone
from psycopg2.extras import DictCursor

load_dotenv()

def get_postgres_connection():
    """Establishes a connection to Postgres."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        cursor_factory=DictCursor  # Use DictCursor to get column names
    )

def get_snowflake_connection():
    """Establishes a connection to Snowflake."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

def get_last_run_timestamp(cursor, table_name, timestamp_column='updated_at'):
    """Gets the latest timestamp from the specified staging table."""
    query = f"""
    SELECT MAX(data:"{timestamp_column}"::timestamp_tz)
    FROM {table_name}
    """
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return result if result else datetime(1970, 1, 1, tzinfo=timezone.utc)

def default_json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def process_table(pg_conn, sf_cursor, table_name, primary_key, timestamp_column='updated_at'):
    """
    Extracts, loads, and merges data for a single Postgres table.
    """
    staging_table_name = f"staging_{table_name}"

    # 1. Create staging table if it doesn't exist
    sf_cursor.execute(f"CREATE TABLE IF NOT EXISTS {staging_table_name} (data VARIANT);")

    # 2. Get the watermark from the last run
    last_run_timestamp = get_last_run_timestamp(sf_cursor, staging_table_name, timestamp_column)
    print(f"Fetching records from '{table_name}' updated after: {last_run_timestamp}")

    # 3. Extract new/updated data from Postgres
    with pg_conn.cursor() as pg_cursor:
        pg_cursor.execute(
            f"SELECT * FROM {table_name} WHERE {timestamp_column} > %s;",
            (last_run_timestamp,)
        )
        rows = pg_cursor.fetchall()

    if not rows:
        print(f"No new or updated records to load from Postgres table: {table_name}")
        return

    print(f"Found {len(rows)} new/updated records in '{table_name}'.")

    # 4. Load data into a temporary Snowflake table
    temp_table_name = f"{staging_table_name}_temp"
    sf_cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} (data VARIANT);")

    for row in rows:
        # Convert row to dict and serialize to JSON
        json_data = json.dumps(dict(row), default=default_json_serializer)
        sf_cursor.execute(
            f"INSERT INTO {temp_table_name} (data) SELECT PARSE_JSON(%s);",
            (json_data,)
        )

    # 5. Merge from temp table into the main staging table
    merge_sql = f"""
    MERGE INTO {staging_table_name} AS target
    USING {temp_table_name} AS source
    ON target.data:"{primary_key}"::string = source.data:"{primary_key}"::string
    WHEN MATCHED THEN
        UPDATE SET target.data = source.data
    WHEN NOT MATCHED THEN
        INSERT (data) VALUES (source.data);
    """
    sf_cursor.execute(merge_sql)
    print(f"Successfully merged {sf_cursor.rowcount} records into '{staging_table_name}'.")

def extract_and_load_postgres_data():
    """
    Orchestrates the EL process for all Postgres tables.
    """
    pg_conn = get_postgres_connection()
    sf_conn = get_snowflake_connection()

    tables_to_process = [
        {"name": "savings_transaction", "pk": "txn_id"},
        {"name": "savings_plan", "pk": "plan_id"}
    ]

    try:
        with sf_conn.cursor() as sf_cursor:
            for table_info in tables_to_process:
                process_table(
                    pg_conn=pg_conn,
                    sf_cursor=sf_cursor,
                    table_name=table_info["name"],
                    primary_key=table_info["pk"]
                )
    finally:
        pg_conn.close()
        sf_conn.close()

if __name__ == "__main__":
    extract_and_load_postgres_data()
