#!/usr/bin/env python3
"""
Migrate Contacts table data into People table.

This script reads all rows from the Contacts table and updates the corresponding
person record in the People table with their contact information:
- contact_type='school email' → people.school_email
- contact_type='personal email' → people.personal_email
- contact_type='phone' → people.phone_number

Features:
- Only updates fields when values differ (no-op if already matching)
- Cleans phone numbers by removing decimals (e.g., "1234567890.0" → "1234567890")
- Shows old and new values when updating existing data

Run this FIRST (after SQL migration) because it uses person_id foreign key,
which is the most reliable way to match records.

Usage:
    python3 02_migrate_contacts_to_people.py [--dry-run]
"""

import psycopg2
import argparse
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def clean_phone_number(phone):
    """
    Clean phone number by removing decimals and converting to string.
    Some phone numbers are stored as floats (e.g., 1234567890.0).
    """
    if phone is None:
        return None
    # Convert to string and remove decimal point and trailing zeros
    phone_str = str(phone)
    if '.' in phone_str:
        phone_str = phone_str.split('.')[0]
    return phone_str.strip()


def migrate_contacts_to_people(conn, dry_run=False):
    """
    Migrate all contacts into people table.
    Returns stats dict with counts.
    """
    cursor = conn.cursor()

    stats = {
        'total_contacts': 0,
        'school_emails_added': 0,
        'personal_emails_added': 0,
        'phones_added': 0,
        'skipped_no_change': 0,
        'errors': 0
    }

    # Get all contacts
    cursor.execute("""
        SELECT
            c.id,
            c.person_id,
            c.contact_type,
            c.contact_value,
            p.first_name,
            p.last_name,
            p.school_email,
            p.personal_email,
            p.phone_number
        FROM Contacts c
        JOIN People p ON c.person_id = p.id
        ORDER BY c.person_id, c.contact_type
    """)

    contacts = cursor.fetchall()
    stats['total_contacts'] = len(contacts)

    print(f"\nFound {len(contacts)} contacts to migrate\n")
    print("=" * 80)

    for contact in contacts:
        contact_id, person_id, contact_type, contact_value, first_name, last_name, \
            existing_school_email, existing_personal_email, existing_phone = contact

        person_display = f"{first_name} {last_name} (person_id={person_id})"

        try:
            if contact_type == 'school email':
                new_value = contact_value.lower()
                if existing_school_email and existing_school_email.lower() == new_value:
                    # No change needed - values already match
                    print(f"  ⊘ {person_display}: school email already matches")
                    stats['skipped_no_change'] += 1
                else:
                    # Update only if different (including if existing is NULL)
                    cursor.execute("""
                        UPDATE People
                        SET school_email = %s
                        WHERE id = %s
                    """, (new_value, person_id))
                    if existing_school_email:
                        print(f"  ✓ {person_display}: updated school email")
                        print(f"     Was: {existing_school_email}")
                        print(f"     Now: {new_value}")
                    else:
                        print(f"  ✓ {person_display}: added school email = {new_value}")
                    stats['school_emails_added'] += 1

            elif contact_type == 'personal email':
                new_value = contact_value.lower()
                if existing_personal_email and existing_personal_email.lower() == new_value:
                    # No change needed - values already match
                    print(f"  ⊘ {person_display}: personal email already matches")
                    stats['skipped_no_change'] += 1
                else:
                    # Update only if different (including if existing is NULL)
                    cursor.execute("""
                        UPDATE People
                        SET personal_email = %s
                        WHERE id = %s
                    """, (new_value, person_id))
                    if existing_personal_email:
                        print(f"  ✓ {person_display}: updated personal email")
                        print(f"     Was: {existing_personal_email}")
                        print(f"     Now: {new_value}")
                    else:
                        print(f"  ✓ {person_display}: added personal email = {new_value}")
                    stats['personal_emails_added'] += 1

            elif contact_type == 'phone':
                # Clean phone number to remove decimals
                new_value = clean_phone_number(contact_value)
                existing_clean = clean_phone_number(existing_phone)

                if existing_clean and existing_clean == new_value:
                    # No change needed - values already match
                    print(f"  ⊘ {person_display}: phone already matches")
                    stats['skipped_no_change'] += 1
                else:
                    # Update only if different (including if existing is NULL)
                    cursor.execute("""
                        UPDATE People
                        SET phone_number = %s
                        WHERE id = %s
                    """, (new_value, person_id))
                    if existing_phone:
                        print(f"  ✓ {person_display}: updated phone")
                        print(f"     Was: {existing_phone}")
                        print(f"     Now: {new_value}")
                    else:
                        print(f"  ✓ {person_display}: added phone = {new_value}")
                    stats['phones_added'] += 1

            else:
                print(f"  ⚠ {person_display}: unknown contact_type = {contact_type}")
                stats['errors'] += 1

        except Exception as e:
            print(f"  ✗ {person_display}: ERROR - {e}")
            stats['errors'] += 1

    # Update preferred_email (school email takes priority) - only when value differs
    print("\n" + "=" * 80)
    print("Updating preferred_email for all people...")
    cursor.execute("""
        UPDATE People
        SET preferred_email = COALESCE(school_email, personal_email)
        WHERE (school_email IS NOT NULL OR personal_email IS NOT NULL)
          AND preferred_email IS DISTINCT FROM COALESCE(school_email, personal_email)
    """)
    rows_updated = cursor.rowcount
    print(f"✓ Updated preferred_email for {rows_updated} people (skipped records where it already matched)")

    return stats


def print_summary(stats):
    """Print migration summary."""
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    print(f"Total contacts processed:    {stats['total_contacts']}")
    print(f"School emails added/updated: {stats['school_emails_added']}")
    print(f"Personal emails added/updated: {stats['personal_emails_added']}")
    print(f"Phone numbers added/updated: {stats['phones_added']}")
    print(f"Skipped (no change):         {stats['skipped_no_change']}")
    print(f"Errors:                      {stats['errors']}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Migrate contacts table data into people table'
    )
    parser.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'railway'),
                        help='Database name')
    parser.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'),
                        help='Database user')
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD', ''),
                        help='Database password')
    parser.add_argument('--host', default=os.environ.get('PGHOST', 'localhost'),
                        help='Database host')
    parser.add_argument('--port', default=os.environ.get('PGPORT', '5432'),
                        help='Database port')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without committing')

    args = parser.parse_args()

    try:
        print(f"Connecting to PostgreSQL database '{args.dbname}' on {args.host}:{args.port}...")
        conn = psycopg2.connect(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port
        )
        conn.autocommit = False
        print("✓ Connected successfully\n")

        # Run migration
        stats = migrate_contacts_to_people(conn, args.dry_run)

        # Print summary
        print_summary(stats)

        # Commit or rollback
        if args.dry_run:
            print("\n⚠ DRY RUN: Rolling back changes (not committed)")
            conn.rollback()
        else:
            print("\n✓ Committing changes to database...")
            conn.commit()
            print("✓ Migration completed successfully!")

        conn.close()
        return 0

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


if __name__ == '__main__':
    exit(main())
