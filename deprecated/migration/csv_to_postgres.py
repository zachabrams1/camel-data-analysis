#!/usr/bin/env python3
"""
Transfer CSV files to PostgreSQL database using psycopg2.

This script:
1. Creates the database schema with fixed circular dependency
2. Imports data from CSV files in the correct order
3. Handles data type conversions and NULL values appropriately

Usage:
    python csv_to_postgres.py --dbname your_database --user your_user --password your_password

Or set environment variables:
    export PGDATABASE=your_database
    export PGUSER=your_user
    export PGPASSWORD=your_password
    python csv_to_postgres.py
"""

import psycopg2
import csv
import os
import argparse


def parse_boolean(value):
    """Convert various boolean representations to proper boolean."""
    if value == '' or value is None:
        return None
    if isinstance(value, bool):
        return value
    value_str = str(value).strip().lower()
    if value_str in ('true', '1', 't', 'yes', 'y'):
        return True
    elif value_str in ('false', '0', 'f', 'no', 'n'):
        return False
    return None


def parse_integer(value):
    """Convert to integer or None."""
    if value == '' or value is None or str(value).strip().lower() in ('na', 'nan', 'none'):
        return None
    try:
        return int(float(value))  # Handle "2028.0" format
    except (ValueError, TypeError):
        return None


def parse_timestamp(value):
    """Convert timestamp string to proper format or None."""
    if value == '' or value is None or str(value).strip().lower() in ('na', 'nan', 'none'):
        return None
    try:
        # Try parsing various timestamp formats
        return str(value).strip()
    except:
        return None


def parse_gender(value):
    """Normalize gender value."""
    if value == '' or value is None or str(value).strip().lower() in ('na', 'nan', 'none'):
        return None
    value_str = str(value).strip().upper()
    if value_str in ('M', 'F', 'O'):
        return value_str
    return None


def parse_school(value):
    """Normalize school value."""
    if value == '' or value is None or str(value).strip().lower() in ('na', 'nan', 'none'):
        return None
    value_str = str(value).strip().lower()
    if value_str in ('harvard', 'mit', 'other'):
        return value_str
    # Try to map variations
    if 'harvard' in value_str:
        return 'harvard'
    elif 'mit' in value_str:
        return 'mit'
    else:
        return 'other'


def parse_is_jewish(value):
    """Normalize is_jewish value to J/N."""
    if value == '' or value is None or str(value).strip().lower() in ('na', 'nan', 'none'):
        return None
    value_str = str(value).strip().upper()
    if value_str in ('J', 'N'):
        return value_str
    return None


def create_database_schema(cursor):
    """Create all database tables in the correct order."""
    print("Creating database schema...")

    with open('dbdesign_fixed.sql', 'r') as f:
        schema_sql = f.read()

    cursor.execute(schema_sql)
    print("✓ Schema created successfully")


def import_people(cursor, csv_path='final/people.csv'):
    """Import people from CSV."""
    print(f"\nImporting people from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM People")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            cursor.execute("""
                INSERT INTO People (id, first_name, last_name, preferred_name, gender,
                                    class_year, is_jewish, school, additional_info)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                parse_integer(row['id']),
                row['first_name'],
                row['last_name'],
                row.get('preferred_name') if row.get('preferred_name') else None,
                parse_gender(row.get('gender')),
                parse_integer(row.get('class_year')),
                parse_boolean(row.get('is_jewish')),
                parse_school(row.get('school')),
                None  # additional_info JSON field
            ))
            count += 1

    print(f"✓ Imported {count} people")
    return count


def import_contacts(cursor, csv_path='final/contacts.csv'):
    """Import contacts from CSV."""
    print(f"\nImporting contacts from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM Contacts")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            cursor.execute("""
                INSERT INTO Contacts (id, person_id, contact_type, contact_value, is_verified)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                parse_integer(row['id']),
                parse_integer(row['person_id']),
                row['contact_type'],
                row['contact_value'].lower(),  # Store emails in lowercase
                parse_boolean(row.get('is_verified', 'False'))
            ))
            count += 1

    print(f"✓ Imported {count} contacts")
    return count


def import_events(cursor, csv_path='final/events.csv'):
    """Import events from CSV."""
    print(f"\nImporting events from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM Events")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            cursor.execute("""
                INSERT INTO Events (id, event_name, category, location, start_datetime, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                parse_integer(row['id']),
                row['event_name'],
                row.get('category') if row.get('category') else None,
                row['location'],
                parse_timestamp(row['start_datetime']),
                row.get('description') if row.get('description') else None
            ))
            count += 1

    print(f"✓ Imported {count} events")
    return count


def import_invite_tokens(cursor, csv_path='final/invite_tokens.csv'):
    """Import invite tokens from CSV."""
    print(f"\nImporting invite tokens from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM InviteTokens")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            cursor.execute("""
                INSERT INTO InviteTokens (id, event_id, value, category, description)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                parse_integer(row['id']),
                parse_integer(row['event_id']),
                row.get('value') if row.get('value') else None,
                row['category'].strip(),  # Strip whitespace from category
                row.get('description') if row.get('description') else None
            ))
            count += 1

    print(f"✓ Imported {count} invite tokens")
    return count


def import_attendance(cursor, csv_path='final/attendance.csv'):
    """Import attendance from CSV."""
    print(f"\nImporting attendance from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM Attendance")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            cursor.execute("""
                INSERT INTO Attendance (id, person_id, event_id, rsvp, approved,
                                        checked_in, rsvp_datetime, is_first_event, invite_token_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                parse_integer(row['id']),
                parse_integer(row['person_id']),
                parse_integer(row['event_id']),
                parse_boolean(row['rsvp']),
                parse_boolean(row['approved']),
                parse_boolean(row['checked_in']),
                parse_timestamp(row.get('rsvp_datetime')),
                parse_boolean(row['is_first_event']),
                parse_integer(row['invite_token_id'])
            ))
            count += 1

    print(f"✓ Imported {count} attendance records")
    return count


def import_mailing_list(cursor, csv_path='final/mailing_list.csv'):
    """Import mailing list from CSV."""
    print(f"\nImporting mailing list from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM MailingList")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            # Parse phone number - remove decimal point if present
            phone = row.get('phone_number', '')
            if phone:
                phone = phone.replace('.0', '').strip()
                if not phone or phone.lower() in ('na', 'nan', 'none'):
                    phone = None
            else:
                phone = None

            cursor.execute("""
                INSERT INTO MailingList (first_name, last_name, gender, class_year,
                                         is_jewish, school, event_attendance_count,
                                         event_rsvp_count, school_email, personal_email,
                                         preferred_email, phone_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['first_name'],
                row['last_name'],
                parse_gender(row.get('gender')),
                parse_integer(row.get('class_year')),
                parse_is_jewish(row.get('is_jewish')),
                parse_school(row.get('school')),
                parse_integer(row.get('event_attendance_count', 0)),
                parse_integer(row.get('event_rsvp_count', 0)),
                row.get('school_email').lower() if row.get('school_email') and row.get('school_email').strip() else None,
                row.get('personal_email').lower() if row.get('personal_email') and row.get('personal_email').strip() else None,
                row.get('preferred_email').lower() if row.get('preferred_email') and row.get('preferred_email').strip() else None,
                phone
            ))
            count += 1

    print(f"✓ Imported {count} mailing list entries")
    return count


def import_all_mailing(cursor, csv_path='all_mailing.csv'):
    """Import simplified all_mailing list from CSV."""
    print(f"\nImporting all_mailing from {csv_path}...")

    # Check if table already has data
    cursor.execute("SELECT COUNT(*) FROM AllMailing")
    existing_count = cursor.fetchone()[0]
    if existing_count > 0:
        print(f"⚠ Table already contains {existing_count} records. Skipping import.")
        return 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            # Parse event_count - handle decimal format
            event_count = row.get('event_count', '0')
            if event_count:
                event_count = event_count.strip()
                if not event_count or event_count.lower() in ('na', 'nan', 'none'):
                    event_count = 0
                else:
                    try:
                        event_count = float(event_count)
                    except:
                        event_count = 0
            else:
                event_count = 0

            cursor.execute("""
                INSERT INTO AllMailing (first_name, last_name, school, contact_value, event_count)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['first_name'],
                row['last_name'],
                row.get('school') if row.get('school') and row.get('school').strip() else None,
                row['contact_value'].lower(),  # Store emails in lowercase
                event_count
            ))
            count += 1

    print(f"✓ Imported {count} all_mailing entries")
    return count


def update_sequences(cursor):
    """Update PostgreSQL sequences to match the max ID from imported data."""
    print("\nUpdating sequences...")

    tables = ['People', 'Contacts', 'Events', 'InviteTokens', 'Attendance', 'MailingList', 'AllMailing']

    for table in tables:
        cursor.execute(f"SELECT MAX(id) FROM {table}")
        max_id = cursor.fetchone()[0]
        if max_id is not None:
            cursor.execute(f"SELECT setval('{table.lower()}_id_seq', {max_id})")
            print(f"✓ Set {table} sequence to {max_id}")


def main():
    parser = argparse.ArgumentParser(description='Import CSV data into PostgreSQL')
    parser.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'event_analytics'),
                        help='Database name (default: event_analytics or $PGDATABASE)')
    parser.add_argument('--user', default=os.environ.get('PGUSER', 'postgres'),
                        help='Database user (default: postgres or $PGUSER)')
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD', ''),
                        help='Database password (default: $PGPASSWORD)')
    parser.add_argument('--host', default=os.environ.get('PGHOST', 'localhost'),
                        help='Database host (default: localhost or $PGHOST)')
    parser.add_argument('--port', default=os.environ.get('PGPORT', '5432'),
                        help='Database port (default: 5432 or $PGPORT)')
    parser.add_argument('--drop-existing', action='store_true',
                        help='Drop existing tables before creating (WARNING: destroys data)')

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
        cursor = conn.cursor()

        print("✓ Connected successfully")

        # Drop existing tables if requested
        if args.drop_existing:
            print("\nDropping existing tables...")
            cursor.execute("""
                DROP TABLE IF EXISTS AllMailing CASCADE;
                DROP TABLE IF EXISTS MailingList CASCADE;
                DROP TABLE IF EXISTS Expenses CASCADE;
                DROP TABLE IF EXISTS Attendance CASCADE;
                DROP TABLE IF EXISTS InviteTokens CASCADE;
                DROP TABLE IF EXISTS Events CASCADE;
                DROP TABLE IF EXISTS Contacts CASCADE;
                DROP TABLE IF EXISTS People CASCADE;
            """)
            print("✓ Existing tables dropped")

        # Create schema
        create_database_schema(cursor)
        conn.commit()
        print("✓ Schema creation committed")

        # Import data in correct order (respecting foreign key constraints)
        # Order: People -> Contacts, Events -> InviteTokens -> Attendance
        # Commit after each CSV to allow recovery from failures

        import_people(cursor)
        conn.commit()
        print("✓ People import committed")

        import_contacts(cursor)
        conn.commit()
        print("✓ Contacts import committed")

        import_events(cursor)
        conn.commit()
        print("✓ Events import committed")

        import_invite_tokens(cursor)
        conn.commit()
        print("✓ Invite tokens import committed")

        import_attendance(cursor)
        conn.commit()
        print("✓ Attendance import committed")

        import_mailing_list(cursor)
        conn.commit()
        print("✓ Mailing list import committed")

        import_all_mailing(cursor)
        conn.commit()
        print("✓ All mailing import committed")

        # Update sequences
        update_sequences(cursor)
        conn.commit()
        print("✓ Sequences updated and committed")

        print("\n" + "="*60)
        print("✓ All data imported successfully!")
        print("="*60)

        # Print summary
        cursor.execute("SELECT COUNT(*) FROM People")
        print(f"Total People: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Contacts")
        print(f"Total Contacts: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Events")
        print(f"Total Events: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM InviteTokens")
        print(f"Total Invite Tokens: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Attendance")
        print(f"Total Attendance Records: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM MailingList")
        print(f"Total Mailing List Entries: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM AllMailing")
        print(f"Total All Mailing Entries: {cursor.fetchone()[0]}")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"\n✗ Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 1
    except FileNotFoundError as e:
        print(f"\n✗ File not found: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 1


if __name__ == '__main__':
    exit(main())
