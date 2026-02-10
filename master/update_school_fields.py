#!/usr/bin/env python3
"""
Update School Fields for Harvard/MIT Emails

This script updates the school field in the people table based on email domains:
- @mit.edu emails → school = "mit"
- @college.harvard.edu emails → school = "harvard"

The script:
1. Queries emails from allmailing table first
2. Looks up people via exact email match to prevent duplicate processing
3. Detects and fixes cross-contamination (e.g., MIT email with "harvard" school)
4. Updates records where school field is incorrect or missing

Note: School values must be lowercase to comply with the people_school_check constraint.
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

def update_school_fields():
    """
    Update school fields for people with Harvard/MIT emails.
    - @mit.edu → "mit"
    - @college.harvard.edu → "harvard"

    Catches cross-contamination cases where emails and school fields don't match.
    """
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("=" * 80)
        print("Updating School Fields for Harvard/MIT Emails")
        print("=" * 80)
        print()

        # ===== Process MIT emails =====
        # Query allmailing for MIT emails (same as check_school_fields.py)
        mit_email_query = """
            SELECT DISTINCT contact_value, first_name, last_name
            FROM allmailing
            WHERE LOWER(contact_value) LIKE '%@mit.edu'
            ORDER BY contact_value;
        """

        cursor.execute(mit_email_query)
        mit_emails = cursor.fetchall()

        print(f"Found {len(mit_emails)} @mit.edu emails in allmailing table")
        print()

        mit_updated_count = 0
        mit_cross_contamination_count = 0
        mit_skipped_count = 0
        mit_not_found_count = 0

        for email, first_name, last_name in mit_emails:
            # Look up person in people table via contacts table (exact match)
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
                school_lower = (school or "").lower()

                # Check if update is needed
                is_cross_contamination = "harvard" in school_lower
                needs_update = "mit" not in school_lower

                if needs_update:
                    # Update the school field
                    update_query = """
                        UPDATE people
                        SET school = %s
                        WHERE id = %s;
                    """
                    cursor.execute(update_query, ("mit", person_id))

                    if is_cross_contamination:
                        print(f"  ⚠ CROSS-CONTAMINATION FIX: {db_first_name} {db_last_name} (ID: {person_id})")
                        mit_cross_contamination_count += 1
                    else:
                        print(f"  ✓ Updated: {db_first_name} {db_last_name} (ID: {person_id})")

                    print(f"    Email: {email}")
                    print(f"    Old school: {school if school else 'NULL'} → New school: mit")
                    print()
                    mit_updated_count += 1
                else:
                    mit_skipped_count += 1
            else:
                print(f"  WARNING: No person found for: {email} (listed as {first_name} {last_name} in allmailing)")
                mit_not_found_count += 1

        # ===== Process Harvard emails =====
        # Query allmailing for Harvard emails
        harvard_email_query = """
            SELECT DISTINCT contact_value, first_name, last_name
            FROM allmailing
            WHERE LOWER(contact_value) LIKE '%@college.harvard.edu'
            ORDER BY contact_value;
        """

        cursor.execute(harvard_email_query)
        harvard_emails = cursor.fetchall()

        print(f"Found {len(harvard_emails)} @college.harvard.edu emails in allmailing table")
        print()

        harvard_updated_count = 0
        harvard_cross_contamination_count = 0
        harvard_skipped_count = 0
        harvard_not_found_count = 0

        for email, first_name, last_name in harvard_emails:
            # Look up person in people table via contacts table (exact match)
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
                school_lower = (school or "").lower()

                # Check if update is needed
                is_cross_contamination = "mit" in school_lower
                needs_update = "harvard" not in school_lower

                if needs_update:
                    # Update the school field
                    update_query = """
                        UPDATE people
                        SET school = %s
                        WHERE id = %s;
                    """
                    cursor.execute(update_query, ("harvard", person_id))

                    if is_cross_contamination:
                        print(f"  ⚠ CROSS-CONTAMINATION FIX: {db_first_name} {db_last_name} (ID: {person_id})")
                        harvard_cross_contamination_count += 1
                    else:
                        print(f"  ✓ Updated: {db_first_name} {db_last_name} (ID: {person_id})")

                    print(f"    Email: {email}")
                    print(f"    Old school: {school if school else 'NULL'} → New school: harvard")
                    print()
                    harvard_updated_count += 1
                else:
                    harvard_skipped_count += 1
            else:
                print(f"  WARNING: No person found for: {email} (listed as {first_name} {last_name} in allmailing)")
                harvard_not_found_count += 1

        # Commit all changes
        conn.commit()

        print("=" * 80)
        print("Summary:")
        print()
        print(f"  MIT emails (@mit.edu):")
        print(f"    Emails found in allmailing: {len(mit_emails)}")
        print(f"    Total updates made: {mit_updated_count}")
        print(f"      - Cross-contamination fixes: {mit_cross_contamination_count}")
        print(f"      - Normal updates: {mit_updated_count - mit_cross_contamination_count}")
        print(f"    Already correct (skipped): {mit_skipped_count}")
        print(f"    Not found in people table: {mit_not_found_count}")
        print()
        print(f"  Harvard emails (@college.harvard.edu):")
        print(f"    Emails found in allmailing: {len(harvard_emails)}")
        print(f"    Total updates made: {harvard_updated_count}")
        print(f"      - Cross-contamination fixes: {harvard_cross_contamination_count}")
        print(f"      - Normal updates: {harvard_updated_count - harvard_cross_contamination_count}")
        print(f"    Already correct (skipped): {harvard_skipped_count}")
        print(f"    Not found in people table: {harvard_not_found_count}")
        print()
        print(f"  GRAND TOTAL UPDATES: {mit_updated_count + harvard_updated_count}")
        print(f"  TOTAL CROSS-CONTAMINATION FIXES: {mit_cross_contamination_count + harvard_cross_contamination_count}")
        print("=" * 80)

        cursor.close()

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    update_school_fields()
