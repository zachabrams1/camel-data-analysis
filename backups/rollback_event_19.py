#!/usr/bin/env python3
"""
Rollback Event Import for Event ID 19

This script will:
1. Delete all attendance records for event 19
2. Delete all invite tokens for event 19
3. Delete the event record itself
4. Optionally delete newly created people (if you provide their IDs)
5. Restore mailing list tables from backup

Usage:
    # Simple rollback (keep new people)
    python3 rollback_event_19.py

    # Full rollback (delete new people too)
    python3 rollback_event_19.py --delete-people 1234,1235,1236

    # Just restore mailing lists
    python3 rollback_event_19.py --restore-only
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import csv
import argparse
import sys

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD')
}

EVENT_ID = 19

def get_db_connection():
    """Establish database connection."""
    return psycopg2.connect(**DB_CONFIG)

def rollback_event(delete_people_ids=None):
    """Rollback event import."""
    conn = get_db_connection()

    try:
        print(f"=== Rolling Back Event {EVENT_ID} ===\n")

        # 1. Get stats before deletion
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as count FROM Attendance WHERE event_id = %s", (EVENT_ID,))
            attendance_count = cur.fetchone()['count']

            cur.execute("SELECT COUNT(*) as count FROM InviteTokens WHERE event_id = %s", (EVENT_ID,))
            token_count = cur.fetchone()['count']

            cur.execute("SELECT event_name FROM Events WHERE id = %s", (EVENT_ID,))
            event = cur.fetchone()
            event_name = event['event_name'] if event else "UNKNOWN"

        print(f"Event: {event_name} (ID: {EVENT_ID})")
        print(f"Found {attendance_count} attendance records")
        print(f"Found {token_count} invite tokens\n")

        # Confirm before deletion
        confirm = input("Are you sure you want to delete this event? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Rollback cancelled.")
            return

        # 2. Delete attendance records
        with conn.cursor() as cur:
            cur.execute("DELETE FROM Attendance WHERE event_id = %s", (EVENT_ID,))
            deleted = cur.rowcount
            print(f"✓ Deleted {deleted} attendance records")

        # 3. Delete newly created people (if specified)
        if delete_people_ids:
            with conn.cursor() as cur:
                # Delete contacts first (foreign key)
                cur.execute("DELETE FROM Contacts WHERE person_id = ANY(%s)", (delete_people_ids,))
                contacts_deleted = cur.rowcount

                # Delete people
                cur.execute("DELETE FROM People WHERE id = ANY(%s)", (delete_people_ids,))
                people_deleted = cur.rowcount

                print(f"✓ Deleted {contacts_deleted} contacts for new people")
                print(f"✓ Deleted {people_deleted} people records")

        # 4. Delete invite tokens
        with conn.cursor() as cur:
            cur.execute("DELETE FROM InviteTokens WHERE event_id = %s", (EVENT_ID,))
            deleted = cur.rowcount
            print(f"✓ Deleted {deleted} invite tokens")

        # 5. Delete event
        with conn.cursor() as cur:
            cur.execute("DELETE FROM Events WHERE id = %s", (EVENT_ID,))
            deleted = cur.rowcount
            print(f"✓ Deleted event record (rows: {deleted})")

        conn.commit()
        print("\n✓ Event rollback complete!")

    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error during rollback: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

def restore_mailing_lists(backup_dir="backups"):
    """Restore mailing list tables from most recent backup."""
    import glob

    print("\n=== Restoring Mailing List Tables ===\n")

    # Find most recent backups
    mailing_list_backups = sorted(glob.glob(f"{backup_dir}/MailingList_backup_*.csv"), reverse=True)
    all_mailing_backups = sorted(glob.glob(f"{backup_dir}/AllMailing_backup_*.csv"), reverse=True)

    if not mailing_list_backups or not all_mailing_backups:
        print("✗ No backup files found in backups/ directory")
        return

    mailing_list_file = mailing_list_backups[0]
    all_mailing_file = all_mailing_backups[0]

    print(f"Using backups:")
    print(f"  MailingList: {mailing_list_file}")
    print(f"  AllMailing: {all_mailing_file}\n")

    conn = get_db_connection()

    try:
        # Restore MailingList
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE MailingList RESTART IDENTITY CASCADE")

            with open(mailing_list_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                for row in rows:
                    cur.execute("""
                        INSERT INTO MailingList (
                            first_name, last_name, gender, class_year, is_jewish, school,
                            event_attendance_count, event_rsvp_count,
                            school_email, personal_email, preferred_email, phone_number
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['first_name'], row['last_name'],
                        row['gender'] if row['gender'] and row['gender'].strip() else None,
                        row['class_year'] if row['class_year'] else None,
                        row['is_jewish'] if row['is_jewish'] else None,
                        row['school'],
                        int(row['event_attendance_count']) if row['event_attendance_count'] else 0,
                        int(row['event_rsvp_count']) if row['event_rsvp_count'] else 0,
                        row['school_email'] if row['school_email'] else None,
                        row['personal_email'] if row['personal_email'] else None,
                        row['preferred_email'] if row['preferred_email'] else None,
                        row['phone_number'] if row['phone_number'] else None
                    ))

            print(f"✓ Restored {len(rows)} rows to MailingList")

        # Restore AllMailing
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE AllMailing RESTART IDENTITY CASCADE")

            with open(all_mailing_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                for row in rows:
                    cur.execute("""
                        INSERT INTO AllMailing (first_name, last_name, school, contact_value, event_count)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        row['first_name'], row['last_name'], row['school'],
                        row['contact_value'],
                        float(row['event_count']) if row['event_count'] else 0
                    ))

            print(f"✓ Restored {len(rows)} rows to AllMailing")

        conn.commit()
        print("\n✓ Mailing list tables restored successfully!")

    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error restoring mailing lists: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Rollback event import for Event ID 19')
    parser.add_argument('--delete-people', type=str, help='Comma-separated list of person IDs to delete (e.g., 1234,1235)')
    parser.add_argument('--restore-only', action='store_true', help='Only restore mailing lists from backup')
    args = parser.parse_args()

    try:
        if not args.restore_only:
            # Parse people IDs if provided
            delete_people_ids = None
            if args.delete_people:
                delete_people_ids = [int(id.strip()) for id in args.delete_people.split(',')]
                print(f"Will also delete people: {delete_people_ids}\n")

            # Rollback event
            rollback_event(delete_people_ids)

        # Restore mailing lists
        restore_mailing_lists()

        print("\n✓ All done!")

    except Exception as e:
        print(f"\n✗ Rollback failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
