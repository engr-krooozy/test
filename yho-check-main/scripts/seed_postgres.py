import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def get_postgres_connection():
    """Establishes a connection to Postgres."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB")
    )

def create_tables(cursor):
    """Creates the savings_plan and savings_transaction tables."""
    # Drop tables if they exist for a clean seed
    cursor.execute("DROP TABLE IF EXISTS savings_transaction;")
    cursor.execute("DROP TABLE IF EXISTS savings_plan;")

    create_plan_table = """
    CREATE TABLE savings_plan (
        plan_id UUID PRIMARY KEY,
        product_type TEXT,
        customer_uid TEXT,
        amount NUMERIC,
        frequency TEXT,
        start_date DATE,
        end_date DATE,
        status TEXT,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        deleted_at TIMESTAMP WITH TIME ZONE
    );
    """
    create_transaction_table = """
    CREATE TABLE savings_transaction (
        txn_id UUID PRIMARY KEY,
        plan_id UUID REFERENCES savings_plan(plan_id),
        amount NUMERIC,
        currency TEXT,
        side TEXT,
        rate NUMERIC,
        txn_timestamp TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        deleted_at TIMESTAMP WITH TIME ZONE
    );
    """
    cursor.execute(create_plan_table)
    cursor.execute(create_transaction_table)
    print("Successfully created savings_plan and savings_transaction tables.")

def seed_postgres_data():
    """Seeds the Postgres database with sample data."""
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            create_tables(cursor)

            # Insert sample data
            now = datetime.now(timezone.utc)
            plans = [
                ('a1b2c3d4-e5f6-7890-1234-567890abcdef', 'SAVINGS', 'user1', 1000, 'monthly', now, now, 'active', now, now, None),
                ('b2c3d4e5-f6a7-8901-2345-67890abcdef0', 'INVESTMENT', 'user2', 500, 'weekly', now, now, 'active', now, now, None)
            ]
            transactions = [
                ('c3d4e5f6-a7b8-9012-3456-7890abcdef01', 'a1b2c3d4-e5f6-7890-1234-567890abcdef', 50, 'USD', 'buy', 1.0, now, now, None),
                ('d4e5f6a7-b8c9-0123-4567-890abcdef012', 'b2c3d4e5-f6a7-8901-2345-67890abcdef0', 25, 'USD', 'buy', 1.0, now, now, None)
            ]

            insert_plan_query = "INSERT INTO savings_plan VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_plan_query, plans)

            insert_txn_query = "INSERT INTO savings_transaction VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(insert_txn_query, transactions)

            conn.commit()
            print(f"Successfully seeded {len(plans)} plans and {len(transactions)} transactions.")

    finally:
        conn.close()

if __name__ == "__main__":
    seed_postgres_data()
