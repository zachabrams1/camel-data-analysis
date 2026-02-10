#!/usr/bin/env python3
"""
Sync Subscribers to AllMailing table.

Reads from Subscribers table and adds any new emails to AllMailing with:
- first_name: empty string (table doesn't allow NULL)
- last_name: empty string (table doesn't allow NULL)
- school: auto-detected from email domain (Harvard/MIT)
- contact_value: the email address
- event_count: 0

After processing, deletes all emails from Subscribers table (both newly added and duplicates).

Usage:
    python3 sync_subscribers_to_mailing.py
"""

import psycopg2
import argparse
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def detect_school_from_email(email):
    """
    Detect school from email domain.
    Returns 'Harvard', 'MIT', or None.
    """
    email_lower = email.lower()

    # Check for Harvard domains
    if '@harvard.edu' in email_lower or '@college.harvard.edu' in email_lower:
        return 'Harvard'

    # Check for MIT domains
    if '@mit.edu' in email_lower:
        return 'MIT'

    return None


def sync_subscribers_to_mailing(conn):
    """
    Sync subscribers to AllMailing table and delete them from Subscribers.
    Returns (added_count, skipped_count, deleted_count).
    """
    cursor = conn.cursor()

    # Get all subscriber emails
    cursor.execute("SELECT id, email FROM Subscribers ORDER BY id")
    subscribers = cursor.fetchall()

    print(f"Found {len(subscribers)} subscribers")

    added_count = 0
    skipped_count = 0
    deleted_count = 0

    for sub_id, email in subscribers:
        # Check if email already exists in AllMailing (case-insensitive)
        cursor.execute("""
            SELECT id FROM AllMailing
            WHERE LOWER(contact_value) = LOWER(%s)
        """, (email,))

        existing = cursor.fetchone()

        if existing:
            print(f"  ⊘ Skipping {email} (already exists in AllMailing)")
            skipped_count += 1
            # Still delete from Subscribers since it's already in AllMailing
            cursor.execute("DELETE FROM Subscribers WHERE id = %s", (sub_id,))
            deleted_count += 1
        else:
            # Detect school from email
            school = detect_school_from_email(email)

            # Insert into AllMailing
            # Note: first_name and last_name are NOT NULL in schema, so using empty strings
            cursor.execute("""
                INSERT INTO AllMailing (first_name, last_name, school, contact_value, event_count)
                VALUES (%s, %s, %s, %s, %s)
            """, ('', '', school, email.lower(), 0))

            school_str = f" [{school}]" if school else ""
            print(f"  ✓ Added {email}{school_str}")
            added_count += 1

            # Delete from Subscribers after successful addition
            cursor.execute("DELETE FROM Subscribers WHERE id = %s", (sub_id,))
            deleted_count += 1

    return added_count, skipped_count, deleted_count


def main():
    parser = argparse.ArgumentParser(description='Sync Subscribers to AllMailing table')
    parser.add_argument('--dbname', default=os.environ.get('DB_NAME', 'railway'),
                        help='Database name (default: railway or $DB_NAME)')
    parser.add_argument('--user', default=os.environ.get('DB_USER', 'postgres'),
                        help='Database user (default: postgres or $DB_USER)')
    parser.add_argument('--password', default=os.environ.get('DB_PASSWORD', ''),
                        help='Database password (default: $DB_PASSWORD)')
    parser.add_argument('--host', default=os.environ.get('DB_HOST', 'localhost'),
                        help='Database host (default: localhost or $DB_HOST)')
    parser.add_argument('--port', default=os.environ.get('DB_PORT', '5432'),
                        help='Database port (default: 5432 or $DB_PORT)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without committing to database')

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

        print("✓ Connected successfully\n")

        # Sync subscribers to mailing list
        added_count, skipped_count, deleted_count = sync_subscribers_to_mailing(conn)

        # Summary
        print("\n" + "="*60)
        print(f"✓ Sync complete!")
        print(f"  Added to AllMailing: {added_count}")
        print(f"  Skipped (already exist): {skipped_count}")
        print(f"  Deleted from Subscribers: {deleted_count}")
        print("="*60)

        if args.dry_run:
            print("\n⚠ DRY RUN: Rolling back changes (not committed)")
            conn.rollback()
        else:
            print("\n✓ Committing changes to database...")
            conn.commit()
            print("✓ Changes committed successfully")

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
