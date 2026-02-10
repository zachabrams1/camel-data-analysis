#!/usr/bin/env python3
"""
Create the analytics_graphs table in the Railway PostgreSQL database.
This table stores PNG images of analytics visualizations.

Usage:
    python create_analytics_graphs_table.py
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def connect_to_db():
    """Connect to the Railway PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=os.getenv('PGPORT'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )


def create_analytics_graphs_table():
    """Create the analytics_graphs table."""
    print("Connecting to Railway database...")
    conn = connect_to_db()
    cursor = conn.cursor()
    print("✅ Connected successfully!")

    # Create the table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS analytics_graphs (
        id SERIAL PRIMARY KEY,
        graph_name VARCHAR(100) UNIQUE NOT NULL,
        image_data BYTEA NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """

    print("\nCreating analytics_graphs table...")
    cursor.execute(create_table_sql)
    conn.commit()
    print("✅ Table created successfully!")

    # Verify the table exists
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'analytics_graphs';
    """)

    result = cursor.fetchone()
    if result:
        print(f"✅ Verified: '{result[0]}' table exists in database")
    else:
        print("❌ Warning: Table creation may have failed")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    try:
        create_analytics_graphs_table()
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
