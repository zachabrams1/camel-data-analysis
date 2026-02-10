#!/usr/bin/env python3
"""
Create partner_sessions table in Railway PostgreSQL database.
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

    # Create partner_sessions table
    print("Creating partner_sessions table...")
    cursor.execute("""
        CREATE TABLE partner_sessions (
          id SERIAL PRIMARY KEY,
          email VARCHAR(255) UNIQUE NOT NULL,
          partner_code VARCHAR(50) NOT NULL,
          partner_name VARCHAR(255),
          organization VARCHAR(255),
          last_login TIMESTAMP DEFAULT NOW(),
          created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("✓ Table created")

    # Create index
    print("Creating index on email column...")
    cursor.execute("""
        CREATE INDEX idx_partner_sessions_email ON partner_sessions(email);
    """)
    print("✓ Index created")

    conn.commit()
    print("✓ Changes committed\n")

    # Verify and show results
    print("="*80)
    print("✓ partner_sessions table created successfully!")
    print("="*80)

    # Show table structure
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'partner_sessions'
        ORDER BY ordinal_position;
    """)

    print("\nTable structure:")
    for row in cursor.fetchall():
        print(f"  {row[0]:20} {row[1]:30} NULL={row[2]:3} DEFAULT={row[3]}")

    # Show indexes
    cursor.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'partner_sessions';
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
