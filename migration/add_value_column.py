#!/usr/bin/env python3
"""
Add the 'value' column to InviteTokens table if it doesn't exist.
"""
import psycopg2

# Railway connection string
DATABASE_URL = "postgresql://postgres:OzafqpSfkSRdIkTpMNdgtbFvLPvmLYPw@yamabiko.proxy.rlwy.net:58300/railway"

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Connected to Railway PostgreSQL")

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

except Exception as e:
    print(f"✗ Error: {e}")
    if 'conn' in locals():
        conn.rollback()
        conn.close()
