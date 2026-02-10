#!/usr/bin/env python3
"""
Migration script to add first_name and last_name columns to Subscribers table on Railway.

This script will:
1. Connect to the Railway PostgreSQL database
2. Add first_name and last_name columns to the Subscribers table
3. Handle the case where columns might already exist

Usage:
    python migrate_add_subscriber_names.py --dbname railway --user postgres --password your_password --host yamabiko.proxy.rlwy.net --port 58300

Or set environment variables:
    export PGDATABASE=railway
    export PGUSER=postgres
    export PGPASSWORD=your_password
    export PGHOST=yamabiko.proxy.rlwy.net
    export PGPORT=58300
    python migrate_add_subscriber_names.py
"""

import os
import psycopg2
from psycopg2 import sql
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection(dbname, user, password, host, port):
    """Create a connection to the Railway database."""
    return psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=password
    )

def migrate_add_subscriber_names(dbname, user, password, host, port):
    """Add first_name and last_name columns to Subscribers table."""

    print("Connecting to Railway database...")
    conn = get_db_connection(dbname, user, password, host, port)
    cursor = conn.cursor()

    try:
        # Add first_name column if it doesn't exist
        print("Adding first_name column to Subscribers table...")
        cursor.execute("""
            ALTER TABLE Subscribers
            ADD COLUMN IF NOT EXISTS first_name VARCHAR(50);
        """)

        # Add last_name column if it doesn't exist
        print("Adding last_name column to Subscribers table...")
        cursor.execute("""
            ALTER TABLE Subscribers
            ADD COLUMN IF NOT EXISTS last_name VARCHAR(50);
        """)

        # Commit the changes
        conn.commit()
        print("✓ Migration completed successfully!")

        # Show the updated table structure
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'subscribers'
            ORDER BY ordinal_position;
        """)

        print("\nUpdated Subscribers table structure:")
        print("-" * 60)
        for row in cursor.fetchall():
            col_name, data_type, max_length, nullable = row
            length_str = f"({max_length})" if max_length else ""
            print(f"  {col_name:<20} {data_type}{length_str:<15} NULL: {nullable}")

    except psycopg2.Error as e:
        print(f"✗ Error during migration: {e}")
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Migrate Subscribers table to add first_name and last_name columns')
    parser.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'railway'),
                        help='Database name (default: railway or $PGDATABASE)')
    parser.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'),
                        help='Database user (default: postgres or $PGUSER)')
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD', ''),
                        help='Database password (default: $PGPASSWORD)')
    parser.add_argument('--host', default=os.environ.get('PGHOST', 'yamabiko.proxy.rlwy.net'),
                        help='Database host (default: yamabiko.proxy.rlwy.net or $PGHOST)')
    parser.add_argument('--port', default=os.environ.get('PGPORT', '58300'),
                        help='Database port (default: 58300 or $PGPORT)')

    args = parser.parse_args()

    print("=" * 60)
    print("Migration: Add first_name and last_name to Subscribers")
    print("=" * 60)
    print(f"Connecting to {args.host}:{args.port}/{args.dbname} as {args.user}")
    print()

    try:
        migrate_add_subscriber_names(args.dbname, args.user, args.password, args.host, args.port)
    except psycopg2.Error as e:
        print(f"\n✗ Migration failed: {e}")
        exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        exit(1)
