#!/usr/bin/env python3
"""
Check School Fields for Harvard/MIT Emails

This script queries the allmailing table for Harvard and MIT emails,
then checks if the person's school field in the people table matches
their email domain. It only prints cases where there's a mismatch
(i.e., Harvard/MIT email but school field doesn't contain "harvard" or "mit").
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 5432),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD')
}

def check_school_fields():
    """
    Query allmailing for Harvard/MIT emails and check their school fields.
    Only print entries where school field doesn't match the email domain.
    """
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query allmailing for Harvard and MIT emails
        query = """
            SELECT DISTINCT contact_value, first_name, last_name
            FROM allmailing
            WHERE LOWER(contact_value) LIKE '%@harvard.edu'
               OR LOWER(contact_value) LIKE '%@mit.edu'
            ORDER BY contact_value;
        """

        cursor.execute(query)
        mailing_list_entries = cursor.fetchall()

        print(f"Found {len(mailing_list_entries)} Harvard/MIT emails in allmailing table\n")
        print("=" * 80)
        print("Checking for mismatches (email domain doesn't match school field):")
        print("=" * 80)
        print()

        mismatch_count = 0
        not_found_count = 0

        for email, first_name, last_name in mailing_list_entries:
            # Look up person in people table via contacts table
            lookup_query = """
                SELECT p.id, p.first_name, p.last_name, p.school
                FROM people p
                JOIN contacts c ON c.person_id = p.id
                WHERE LOWER(c.contact_value) = LOWER(%s);
            """

            cursor.execute(lookup_query, (email,))
            person = cursor.fetchone()

            if person:
                person_id, db_first_name, db_last_name, school = person

                # Only print if school field doesn't contain "harvard" or "mit"
                school_lower = (school or "").lower()
                if "harvard" not in school_lower and "mit" not in school_lower:
                    print(f"{email} | {db_first_name} {db_last_name} | {school if school else 'NULL'}")
                    mismatch_count += 1
            else:
                # Person not found in people table
                print(f"WARNING: no person found for: {email} (listed as {first_name} {last_name} in allmailing)")
                not_found_count += 1

        print()
        print("=" * 80)
        print("Summary:")
        print(f"  Total Harvard/MIT emails checked: {len(mailing_list_entries)}")
        print(f"  Mismatches found (school field doesn't match email): {mismatch_count}")
        print(f"  Emails not found in people table: {not_found_count}")
        print(f"  Correct matches (not shown): {len(mailing_list_entries) - mismatch_count - not_found_count}")
        print("=" * 80)

        cursor.close()

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    check_school_fields()
