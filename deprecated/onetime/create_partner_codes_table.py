#!/usr/bin/env python3
"""
Create partner_codes table in Railway PostgreSQL database.
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

    # Create partner_codes table
    print("Creating partner_codes table...")
    cursor.execute("""
        CREATE TABLE partner_codes (
          id SERIAL PRIMARY KEY,
          code VARCHAR(50) UNIQUE NOT NULL,
          partner_name VARCHAR(255),
          organization VARCHAR(255),
          email VARCHAR(255),
          is_active BOOLEAN DEFAULT true,
          used_count INTEGER DEFAULT 0,
          max_uses INTEGER DEFAULT 1,
          created_at TIMESTAMP DEFAULT NOW(),
          last_used_at TIMESTAMP
        );
    """)
    print("✓ Table created")

    # Create index
    print("Creating index on code column...")
    cursor.execute("""
        CREATE INDEX idx_partner_codes_code ON partner_codes(code);
    """)
    print("✓ Index created")

    # Insert example data
    print("Inserting example partner codes...")
    cursor.execute("""
        INSERT INTO partner_codes (code, partner_name, organization, email, max_uses) VALUES
        ('PARTNER2024', 'Partner Access', 'General Partners', NULL, NULL),
        ('DEMO123', 'Demo Partner', 'Demo Corp', 'demo@example.com', 1);
    """)
    print("✓ Example codes inserted")

    conn.commit()
    print("✓ Changes committed\n")

    # Verify and show results
    cursor.execute("SELECT * FROM partner_codes ORDER BY id;")
    rows = cursor.fetchall()

    print("="*80)
    print("✓ partner_codes table created successfully!")
    print("="*80)

    # Show table structure
    cursor.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'partner_codes'
        ORDER BY ordinal_position;
    """)

    print("\nTable structure:")
    for row in cursor.fetchall():
        print(f"  {row[0]:20} {row[1]:30} NULL={row[2]:3} DEFAULT={row[3]}")

    # Show inserted data
    cursor.execute("SELECT code, partner_name, organization, email, max_uses, is_active FROM partner_codes;")
    print("\nInserted partner codes:")
    print(f"  {'Code':15} {'Partner':20} {'Organization':20} {'Email':25} {'Max Uses':10} {'Active':7}")
    print("  " + "-"*100)
    for row in cursor.fetchall():
        max_uses = "Unlimited" if row[4] is None else str(row[4])
        email = row[3] if row[3] else "N/A"
        print(f"  {row[0]:15} {row[1]:20} {row[2]:20} {email:25} {max_uses:10} {row[5]}")

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
