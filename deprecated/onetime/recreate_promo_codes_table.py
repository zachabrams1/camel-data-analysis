#!/usr/bin/env python3
"""
Drop and recreate promo_codes table in Railway PostgreSQL database.
This script handles the migration from event_id to promo_id.
"""

import psycopg2
import os

# Get credentials from environment or use defaults
PGHOST = os.environ.get('PGHOST', 'yamabiko.proxy.rlwy.net')
PGPORT = os.environ.get('PGPORT', '58300')
PGDATABASE = os.environ.get('PGDATABASE', 'railway')
PGUSER = os.environ.get('PGUSER', 'postgres')
PGPASSWORD = os.environ.get('PGPASSWORD', 'MURyJhuWJvkGJbbhVLInwSimZeanilKF')

try:
    print(f"Connecting to PostgreSQL database '{PGDATABASE}' on {PGHOST}:{PGPORT}...")
    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    conn.autocommit = False
    cursor = conn.cursor()

    print("✓ Connected successfully\n")

    # Drop existing promo_codes table if it exists
    print("Dropping existing promo_codes table (if exists)...")
    cursor.execute("""
        DROP TABLE IF EXISTS promo_codes CASCADE;
    """)
    print("✓ Table dropped")

    # Create promo_codes table with correct schema
    print("Creating promo_codes table with promo_id...")
    cursor.execute("""
        CREATE TABLE promo_codes (
          id SERIAL PRIMARY KEY,
          promo_id INTEGER NOT NULL,
          person_id INTEGER NOT NULL,
          code VARCHAR(100) UNIQUE NOT NULL,
          created_at TIMESTAMP DEFAULT NOW(),
          CONSTRAINT fk_promo_codes_promo FOREIGN KEY (promo_id) REFERENCES promos(id) ON DELETE CASCADE,
          CONSTRAINT fk_promo_codes_person FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE,
          CONSTRAINT unique_person_promo UNIQUE (person_id, promo_id)
        );
    """)
    print("✓ Table created")

    # Create indexes for query performance
    print("Creating index on promo_id column...")
    cursor.execute("""
        CREATE INDEX idx_promo_codes_promo_id ON promo_codes(promo_id);
    """)
    print("✓ Index created on promo_id")

    print("Creating index on person_id column...")
    cursor.execute("""
        CREATE INDEX idx_promo_codes_person_id ON promo_codes(person_id);
    """)
    print("✓ Index created on person_id")

    print("Creating index on code column...")
    cursor.execute("""
        CREATE INDEX idx_promo_codes_code ON promo_codes(code);
    """)
    print("✓ Index created on code")

    conn.commit()
    print("✓ Changes committed\n")

    # Verify and show results
    print("="*80)
    print("✓ promo_codes table recreated successfully!")
    print("="*80)

    # Show table structure
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'promo_codes'
        ORDER BY ordinal_position;
    """)

    print("\nTable structure:")
    for row in cursor.fetchall():
        print(f"  {row[0]:20} {row[1]:30} NULL={row[2]:3} DEFAULT={row[3]}")

    # Show constraints
    cursor.execute("""
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = 'promo_codes'
        ORDER BY constraint_type, constraint_name;
    """)

    print("\nConstraints:")
    for row in cursor.fetchall():
        print(f"  {row[0]:40} {row[1]}")

    # Show indexes
    cursor.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'promo_codes'
        ORDER BY indexname;
    """)

    print("\nIndexes:")
    for row in cursor.fetchall():
        print(f"  {row[0]}")

    cursor.close()
    conn.close()

except psycopg2.Error as e:
    print(f"\n✗ Database error: {e}")
    if 'conn' in locals():
        conn.rollback()
    exit(1)
except Exception as e:
    print(f"\n✗ Error: {e}")
    if 'conn' in locals():
        conn.rollback()
    exit(1)
