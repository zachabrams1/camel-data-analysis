#!/usr/bin/env python3
"""
Add the 'value' column to InviteTokens table if it doesn't exist.

Usage:
    python add_value_column.py --dbname railway --user postgres --password your_password --host yamabiko.proxy.rlwy.net --port 58300

Or set environment variables:
    export PGDATABASE=railway
    export PGUSER=postgres
    export PGPASSWORD=your_password
    export PGHOST=yamabiko.proxy.rlwy.net
    export PGPORT=58300
    python add_value_column.py
"""
import psycopg2
import os
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def add_value_column(dbname, user, password, host, port):
    """Add the 'value' column to InviteTokens table."""
    print(f"Connecting to {host}:{port}/{dbname} as {user}...")

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    print("✓ Connected to PostgreSQL")

    # Check if the column already exists
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='invitetokens' AND column_name='value';
    """)

    exists = cursor.fetchone()

    if exists:
        print("✓ Column 'value' already exists in InviteTokens table")
    else:
        print("Adding 'value' column to InviteTokens table...")
        cursor.execute("""
            ALTER TABLE InviteTokens
            ADD COLUMN value VARCHAR(100);
        """)
        conn.commit()
        print("✓ Column 'value' added successfully!")

    # Show current table structure
    cursor.execute("""
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name='invitetokens'
        ORDER BY ordinal_position;
    """)

    print("\nCurrent InviteTokens table structure:")
    print("-" * 70)
    for row in cursor.fetchall():
        col_name, data_type, max_length, nullable = row
        length_info = f"({max_length})" if max_length else ""
        print(f"  {col_name:20s} {data_type}{length_info:20s} {'NULL' if nullable == 'YES' else 'NOT NULL'}")
    print("-" * 70)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Add value column to InviteTokens table')
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

    print("=" * 70)
    print("Migration: Add 'value' column to InviteTokens table")
    print("=" * 70)
    print()

    try:
        add_value_column(args.dbname, args.user, args.password, args.host, args.port)
    except psycopg2.Error as e:
        print(f"\n✗ Database error: {e}")
        exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        exit(1)
