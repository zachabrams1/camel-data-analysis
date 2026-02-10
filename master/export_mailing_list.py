#!/usr/bin/env python3
"""
Export mailing list from allmailing SQL table to CSV
Creates a CSV with first_name, last_name, and email columns
"""

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

def export_mailing_list():
    """Export mailing list from allmailing table to CSV"""

    # Load environment variables
    load_dotenv()

    # Get database credentials from environment
    db_config = {
        'dbname': os.getenv('PGDATABASE'),
        'user': os.getenv('PGUSER'),
        'password': os.getenv('PGPASSWORD'),
        'host': os.getenv('PGHOST'),
        'port': os.getenv('PGPORT')
    }

    # Connect to database
    print("Connecting to database...")
    conn = psycopg2.connect(**db_config)

    try:
        # Query allmailing table
        query = """
        SELECT
            first_name,
            last_name,
            contact_value as email
        FROM allmailing
        ORDER BY last_name, first_name
        """

        print("Querying allmailing table...")
        df = pd.read_sql_query(query, conn)

        # Output file path
        output_path = os.path.join(
            os.path.dirname(__file__),
            'mailing_list.csv'
        )

        # Export to CSV
        df.to_csv(output_path, index=False)

        print(f"\nExported {len(df)} records to: {output_path}")
        print(f"\nColumns: {', '.join(df.columns)}")
        print(f"First few rows:")
        print(df.head(10).to_string(index=False))

    finally:
        # Close connection
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    export_mailing_list()
