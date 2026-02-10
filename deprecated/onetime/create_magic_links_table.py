#!/usr/bin/env python3
"""
Create verification_codes table in Railway PostgreSQL database.

Usage:
    python create_magic_links_table.py

Set environment variables for Railway connection:
    export PGHOST=yamabiko.proxy.rlwy.net
    export PGPORT=58300
    export PGDATABASE=railway
    export PGUSER=postgres
    export PGPASSWORD=MURyJhuWJvkGJbbhVLInwSimZeanilKF

Or pass them as arguments:
    python create_magic_links_table.py --host yamabiko.proxy.rlwy.net --port 58300
"""

import psycopg2
import os
import argparse


def create_verification_codes_table(cursor):
    """Create the verification_codes table."""

    print("Creating verification_codes table...")

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS verification_codes (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            code VARCHAR(6) NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("✓ Table created successfully")


def main():
    parser = argparse.ArgumentParser(description='Create verification_codes table in PostgreSQL')
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

    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL database '{args.dbname}' on {args.host}:{args.port}...")
        conn = psycopg2.connect(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port
        )
        conn.autocommit = False
        cursor = conn.cursor()

        print("✓ Connected successfully")

        # Drop magic_links table if it exists
        print("\nDropping magic_links table if it exists...")
        cursor.execute("DROP TABLE IF EXISTS magic_links CASCADE;")
        print("✓ magic_links table dropped")

        # Create new verification_codes table
        create_verification_codes_table(cursor)
        conn.commit()
        print("✓ Changes committed")

        # Verify table exists
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'verification_codes';
        """)
        result = cursor.fetchone()

        if result:
            print("\n" + "="*60)
            print("✓ verification_codes table created successfully!")
            print("="*60)

            # Show table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'verification_codes'
                ORDER BY ordinal_position;
            """)

            print("\nTable structure:")
            for row in cursor.fetchall():
                print(f"  {row[0]:15} {row[1]:20} NULL={row[2]:3} DEFAULT={row[3]}")

            # Show indexes
            cursor.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'verification_codes';
            """)

            print("\nIndexes:")
            for row in cursor.fetchall():
                print(f"  {row[0]}")
        else:
            print("\n⚠ Table creation verification failed")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"\n✗ Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
