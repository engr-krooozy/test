import os
import json
import pymongo
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def get_mongo_collection():
    """Establishes a connection to MongoDB and returns the users collection."""
    mongo_client = pymongo.MongoClient(
        host=os.getenv("MONGO_HOST"),
        port=int(os.getenv("MONGO_PORT"))
    )
    # A default db name for local dev
    db_name = os.getenv("MONGO_DB", "users_db")
    db = mongo_client[db_name]
    return db.users

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

def ensure_last_updated_at(collection):
    """Ensures that all documents in the collection have a last_updated_at field."""
    collection.update_many(
        {"last_updated_at": {"$exists": False}},
        {"$set": {"last_updated_at": datetime.now(timezone.utc)}}
    )

def get_last_run_timestamp(cursor):
    """Gets the latest last_updated_at timestamp from the staging_users table."""
    cursor.execute("SELECT MAX(data:\"last_updated_at\"::timestamp_tz) FROM staging_users;")
    result = cursor.fetchone()[0]
    # Return a very old timestamp if the table is empty
    return result if result else datetime(1970, 1, 1, tzinfo=timezone.utc)


def extract_and_load_mongo_data():
    """
    Extracts new and updated data from MongoDB based on the last run timestamp
    and merges it into the Snowflake staging table.
    """
    snowflake_conn = get_snowflake_connection()
    cursor = snowflake_conn.cursor()

    try:
        # 1. Create the staging table if it doesn't exist
        cursor.execute("CREATE TABLE IF NOT EXISTS staging_users (data VARIANT);")

        # 2. Get the watermark (latest timestamp) from the last successful run
        last_run_timestamp = get_last_run_timestamp(cursor)
        print(f"Fetching MongoDB records updated after: {last_run_timestamp}")

        # 3. Extract data from MongoDB that is new or updated
        mongo_collection = get_mongo_collection()
        ensure_last_updated_at(mongo_collection)

        # PyMongo expects naive datetimes to be UTC
        query_timestamp = last_run_timestamp.replace(tzinfo=None)
        users_data = list(mongo_collection.find({"last_updated_at": {"$gt": query_timestamp}}))

        if not users_data:
            print("No new or updated records to load from MongoDB.")
            return

        print(f"Found {len(users_data)} new/updated records in MongoDB.")

        # 4. Load data into a temporary table in Snowflake
        temp_table_name = "staging_users_temp"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} (data VARIANT);")

        for user in users_data:
            # Prepare data for JSON serialization
            user['_id'] = str(user['_id'])
            if isinstance(user['last_updated_at'], datetime):
                 user['last_updated_at'] = user['last_updated_at'].isoformat()

            # Insert into temp table
            cursor.execute(
                f"INSERT INTO {temp_table_name} (data) SELECT PARSE_JSON(%s);",
                (json.dumps(user),)
            )

        # 5. Merge data from the temporary table into the main staging table
        merge_sql = f"""
        MERGE INTO staging_users AS target
        USING {temp_table_name} AS source
        ON target.data:"_id"::string = source.data:"_id"::string
        WHEN MATCHED THEN
            UPDATE SET target.data = source.data
        WHEN NOT MATCHED THEN
            INSERT (data) VALUES (source.data);
        """
        cursor.execute(merge_sql)

        print(f"Successfully merged {cursor.rowcount} records from MongoDB into Snowflake.")

    finally:
        cursor.close()
        snowflake_conn.close()

if __name__ == "__main__":
    extract_and_load_mongo_data()
