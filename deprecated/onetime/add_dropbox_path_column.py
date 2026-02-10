#!/usr/bin/env python3
"""
Add dropbox_path column to Events table

This script adds a 'dropbox_path' TEXT column to the Events table
in the railway database.
"""

import psycopg2
from dotenv import load_dotenv
import os
import sys

# Load environment variables from parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(parent_dir, '.env')
load_dotenv(env_path)

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD'),
    'connect_timeout': 10
}

def main():
    """Add dropbox_path column to Events table."""
    print("Connecting to railway database...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to database successfully\n")

        with conn.cursor() as cur:
            # Check if column already exists
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'events'
                AND column_name = 'dropbox_path'
            """)

            if cur.fetchone():
                print("⚠️  Column 'dropbox_path' already exists in Events table")
                print("No changes made.")
                conn.close()
                return

            # Add the column
            print("Adding 'dropbox_path' column to Events table...")
            cur.execute("""
                ALTER TABLE Events
                ADD COLUMN dropbox_path TEXT
            """)

            conn.commit()
            print("✓ Successfully added 'dropbox_path' column to Events table\n")

            # Verify the column was added
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'events'
                ORDER BY ordinal_position
            """)

            columns = cur.fetchall()
            print("Current Events table structure:")
            print(f"{'Column Name':<30} {'Data Type':<20} {'Max Length':<15}")
            print("=" * 65)
            for col in columns:
                col_name, data_type, max_length = col
                max_len_str = str(max_length) if max_length else 'N/A'
                print(f"{col_name:<30} {data_type:<20} {max_len_str:<15}")

        conn.close()
        print("\n✓ Migration complete!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
