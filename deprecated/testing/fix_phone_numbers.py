#!/usr/bin/env python3
"""Fix corrupted phone numbers in database"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import re

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD')
}

def fix_phone_number(phone):
    """Fix phone number: remove .0 suffix and add + prefix"""
    if not phone:
        return phone

    # Remove .0 suffix
    if phone.endswith('.0'):
        phone = phone[:-2]

    # Add + prefix if missing
    if not phone.startswith('+'):
        phone = '+' + phone

    return phone

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=== FIXING CORRUPTED PHONE NUMBERS ===\n")

    # Get all corrupted phone numbers
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, person_id, contact_value
            FROM Contacts
            WHERE contact_type = 'phone'
              AND (contact_value LIKE '%.0%' OR contact_value NOT LIKE '+%')
        """)
        corrupted = cur.fetchall()

    print(f"Found {len(corrupted)} corrupted phone numbers")

    if len(corrupted) == 0:
        print("✓ No corrupted phone numbers to fix!")
        conn.close()
        return

    # Ask for confirmation
    print("\nExamples of fixes:")
    for i in range(min(5, len(corrupted))):
        old = corrupted[i]['contact_value']
        new = fix_phone_number(old)
        print(f"  '{old}' → '{new}'")

    response = input(f"\nFix {len(corrupted)} phone numbers? (yes/no): ")

    if response.lower() != 'yes':
        print("Cancelled.")
        conn.close()
        return

    # Fix the phone numbers
    fixed_count = 0
    with conn.cursor() as cur:
        for contact in corrupted:
            old_value = contact['contact_value']
            new_value = fix_phone_number(old_value)

            # Update the contact
            cur.execute("""
                UPDATE Contacts
                SET contact_value = %s
                WHERE id = %s
            """, (new_value, contact['id']))

            fixed_count += 1

            if fixed_count % 100 == 0:
                print(f"  Fixed {fixed_count}/{len(corrupted)}...")
                conn.commit()  # Commit in batches

    conn.commit()
    print(f"\n✓ Fixed {fixed_count} phone numbers!")

    # Check for duplicates created by the fix
    print("\n=== CHECKING FOR DUPLICATES ===")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT person_id, contact_value, COUNT(*) as count
            FROM Contacts
            WHERE contact_type = 'phone'
            GROUP BY person_id, contact_value
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()

    if duplicates:
        print(f"⚠️  Found {len(duplicates)} duplicate phone entries")
        print("\nRemoving duplicates (keeping one per person/phone)...")

        with conn.cursor() as cur:
            for dup in duplicates:
                # Keep the lowest ID, delete the rest
                cur.execute("""
                    DELETE FROM Contacts
                    WHERE id NOT IN (
                        SELECT MIN(id)
                        FROM Contacts
                        WHERE person_id = %s
                          AND contact_value = %s
                          AND contact_type = 'phone'
                    )
                    AND person_id = %s
                    AND contact_value = %s
                    AND contact_type = 'phone'
                """, (dup['person_id'], dup['contact_value'], dup['person_id'], dup['contact_value']))

        conn.commit()
        print(f"✓ Removed {len(duplicates)} duplicate entries")
    else:
        print("✓ No duplicates found")

    conn.close()
    print("\n✓ All done!")

if __name__ == "__main__":
    main()
