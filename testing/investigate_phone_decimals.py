#!/usr/bin/env python3
"""Investigate phone number decimal issue"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD')
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=== INVESTIGATING PHONE NUMBER STORAGE ===\n")

    # Check the Contacts table schema
    print("1. Contacts table schema:")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'contacts'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        for col in columns:
            print(f"   {col['column_name']}: {col['data_type']}", end='')
            if col['character_maximum_length']:
                print(f"({col['character_maximum_length']})")
            else:
                print()

    # Check phone numbers with decimals
    print("\n2. Phone numbers containing '.0':")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT person_id, contact_type, contact_value,
                   pg_typeof(contact_value) as value_type
            FROM Contacts
            WHERE contact_type = 'phone'
              AND contact_value LIKE '%.0%'
            LIMIT 10
        """)
        phones_with_decimals = cur.fetchall()

        if phones_with_decimals:
            for p in phones_with_decimals:
                print(f"   Person {p['person_id']}: '{p['contact_value']}' (type: {p['value_type']})")
        else:
            print("   None found")

    # Check phone numbers without + prefix
    print("\n3. Phone numbers WITHOUT '+' prefix:")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT person_id, contact_type, contact_value
            FROM Contacts
            WHERE contact_type = 'phone'
              AND contact_value NOT LIKE '+%'
            LIMIT 10
        """)
        phones_no_plus = cur.fetchall()

        if phones_no_plus:
            for p in phones_no_plus:
                print(f"   Person {p['person_id']}: '{p['contact_value']}'")
        else:
            print("   None found")

    # Check Gideon's contacts specifically
    print("\n4. Gideon Bialkin's phone numbers:")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT c.contact_type, c.contact_value, c.id
            FROM Contacts c
            JOIN People p ON c.person_id = p.id
            WHERE LOWER(p.first_name) = 'gideon'
              AND LOWER(p.last_name) = 'bialkin'
              AND c.contact_type = 'phone'
        """)
        gideon_phones = cur.fetchall()

        for p in gideon_phones:
            print(f"   Contact ID {p['id']}: {p['contact_type']} = '{p['contact_value']}'")

    # Count total phones with issues
    print("\n5. Summary:")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'phone'")
        total_phones = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'phone' AND contact_value LIKE '%.0%'")
        phones_with_decimal = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'phone' AND contact_value NOT LIKE '+%'")
        phones_without_plus = cur.fetchone()[0]

    print(f"   Total phone contacts: {total_phones}")
    print(f"   Phones with '.0': {phones_with_decimal} ({phones_with_decimal/total_phones*100:.1f}%)")
    print(f"   Phones without '+': {phones_without_plus} ({phones_without_plus/total_phones*100:.1f}%)")

    conn.close()

if __name__ == "__main__":
    main()
