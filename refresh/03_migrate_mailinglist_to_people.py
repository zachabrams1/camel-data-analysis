#!/usr/bin/env python3
"""
Migrate MailingList table data into People table.

This script reads all rows from the MailingList table and:
1. Matches to existing people by email (school_email or personal_email) or name
2. Fills in any missing demographic fields (gender, class_year, is_jewish, school)
3. Updates event attendance/RSVP counts
4. Sets preferred_email if not already set
5. **INTERACTIVE**: Prompts user to resolve email conflicts when values differ

When an email field (school_email, personal_email, preferred_email) has a conflict,
the script will pause and ask you to choose:
  1. Keep current value
  2. Replace with new value
  3. Skip this person entirely

Run this SECOND after migrate_contacts_to_people.py

Usage:
    python3 03_migrate_mailinglist_to_people.py [--dry-run]
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


def prompt_user_for_email_conflict(person_display, field_name, current_value, new_value):
    """
    Prompt user to decide what to do when email values conflict.
    Returns: 'keep_current', 'use_new', or 'skip'
    """
    print("\n" + "!" * 80)
    print(f"⚠ EMAIL CONFLICT DETECTED")
    print("!" * 80)
    print(f"Person: {person_display}")
    print(f"Field: {field_name}")
    print(f"Current value: {current_value}")
    print(f"New value from source: {new_value}")
    print()
    print("What would you like to do?")
    print("  1. Keep current value")
    print("  2. Replace with new value")
    print("  3. Skip this person entirely")
    print()

    while True:
        choice = input("Your choice [1/2/3]: ").strip()
        if choice == '1':
            return 'keep_current'
        elif choice == '2':
            return 'use_new'
        elif choice == '3':
            return 'skip'
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")


def normalize_name(name):
    """Normalize name for comparison."""
    if not name:
        return ""
    return name.strip().lower()


def find_person_match(cursor, mailing_record):
    """
    Try to find matching person in People table.
    Returns person_id or None.
    Matching priority:
    1. school_email match
    2. personal_email match
    3. first_name + last_name match
    """
    ml_school_email = mailing_record['school_email']
    ml_personal_email = mailing_record['personal_email']
    ml_first_name = mailing_record['first_name']
    ml_last_name = mailing_record['last_name']

    # Try school email match
    if ml_school_email:
        cursor.execute("""
            SELECT id FROM People
            WHERE LOWER(school_email) = LOWER(%s)
        """, (ml_school_email,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # Try personal email match
    if ml_personal_email:
        cursor.execute("""
            SELECT id FROM People
            WHERE LOWER(personal_email) = LOWER(%s)
        """, (ml_personal_email,))
        result = cursor.fetchone()
        if result:
            return result[0]

    # Try name match
    if ml_first_name and ml_last_name:
        cursor.execute("""
            SELECT id FROM People
            WHERE LOWER(first_name) = LOWER(%s)
              AND LOWER(last_name) = LOWER(%s)
        """, (ml_first_name, ml_last_name))
        result = cursor.fetchone()
        if result:
            return result[0]

    return None


def convert_is_jewish(value):
    """Convert mailinglist is_jewish (J/N) to boolean."""
    if not value:
        return None
    if value.upper() == 'J':
        return True
    elif value.upper() == 'N':
        return False
    return None


def migrate_mailinglist_to_people(conn, dry_run=False):
    """
    Migrate all mailinglist records into people table.
    Returns stats dict with counts.
    """
    cursor = conn.cursor()

    stats = {
        'total_records': 0,
        'matched_existing': 0,
        'created_new': 0,
        'fields_updated': 0,
        'event_counts_updated': 0,
        'emails_added': 0,
        'emails_replaced_by_user': 0,
        'emails_kept_by_user': 0,
        'phones_added': 0,
        'phones_kept_conflict': 0,
        'skipped_no_change': 0,
        'skipped_by_user': 0,
        'errors': 0
    }

    # Get all mailinglist records
    cursor.execute("""
        SELECT
            id,
            first_name,
            last_name,
            gender,
            class_year,
            is_jewish,
            school,
            event_attendance_count,
            event_rsvp_count,
            school_email,
            personal_email,
            preferred_email,
            phone_number
        FROM MailingList
        ORDER BY id
    """)

    mailing_records = cursor.fetchall()
    stats['total_records'] = len(mailing_records)

    print(f"\nFound {len(mailing_records)} mailinglist records to migrate\n")
    print("=" * 80)

    for record in mailing_records:
        ml = {
            'id': record[0],
            'first_name': record[1],
            'last_name': record[2],
            'gender': record[3],
            'class_year': record[4],
            'is_jewish': record[5],
            'school': record[6],
            'event_attendance_count': record[7] or 0,
            'event_rsvp_count': record[8] or 0,
            'school_email': record[9],
            'personal_email': record[10],
            'preferred_email': record[11],
            'phone_number': record[12]
        }

        record_display = f"{ml['first_name']} {ml['last_name']} (ml_id={ml['id']})"

        try:
            # Try to find existing person
            person_id = find_person_match(cursor, ml)

            if person_id:
                # Person exists - update missing fields
                print(f"  ✓ {record_display}: matched to person_id={person_id}")
                stats['matched_existing'] += 1

                # Get existing person data
                cursor.execute("""
                    SELECT
                        gender, class_year, is_jewish, school,
                        school_email, personal_email, phone_number,
                        event_attendance_count, event_rsvp_count, preferred_email
                    FROM People
                    WHERE id = %s
                """, (person_id,))
                existing = cursor.fetchone()

                updates = []
                update_values = []

                # Check each field and update if missing
                if not existing[0] and ml['gender']:  # gender
                    updates.append("gender = %s")
                    update_values.append(ml['gender'])
                    print(f"    → Adding gender: {ml['gender']}")
                    stats['fields_updated'] += 1

                if not existing[1] and ml['class_year']:  # class_year
                    updates.append("class_year = %s")
                    update_values.append(ml['class_year'])
                    print(f"    → Adding class_year: {ml['class_year']}")
                    stats['fields_updated'] += 1

                if existing[2] is None and ml['is_jewish']:  # is_jewish
                    is_jewish_bool = convert_is_jewish(ml['is_jewish'])
                    if is_jewish_bool is not None:
                        updates.append("is_jewish = %s")
                        update_values.append(is_jewish_bool)
                        print(f"    → Adding is_jewish: {is_jewish_bool}")
                        stats['fields_updated'] += 1

                if not existing[3] and ml['school']:  # school
                    updates.append("school = %s")
                    update_values.append(ml['school'].lower())
                    print(f"    → Adding school: {ml['school']}")
                    stats['fields_updated'] += 1

                # School email - prompt user if conflict
                skip_person = False
                if ml['school_email']:
                    new_value = ml['school_email'].lower()
                    if existing[4] and existing[4].lower() == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing[4]:
                        # Differs - prompt user
                        decision = prompt_user_for_email_conflict(
                            record_display, 'school_email', existing[4], new_value
                        )
                        if decision == 'use_new':
                            updates.append("school_email = %s")
                            update_values.append(new_value)
                            print(f"    ✓ User chose: Replace school_email with {new_value}")
                            stats['emails_replaced_by_user'] += 1
                        elif decision == 'keep_current':
                            print(f"    ✓ User chose: Keep current school_email {existing[4]}")
                            stats['emails_kept_by_user'] += 1
                        else:  # skip
                            print(f"    ✓ User chose: Skip this person")
                            stats['skipped_by_user'] += 1
                            skip_person = True
                    else:
                        # NULL - add it
                        updates.append("school_email = %s")
                        update_values.append(new_value)
                        print(f"    → Adding school_email: {new_value}")
                        stats['emails_added'] += 1

                if skip_person:
                    continue

                # Personal email - prompt user if conflict
                if ml['personal_email']:
                    new_value = ml['personal_email'].lower()
                    if existing[5] and existing[5].lower() == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing[5]:
                        # Differs - prompt user
                        decision = prompt_user_for_email_conflict(
                            record_display, 'personal_email', existing[5], new_value
                        )
                        if decision == 'use_new':
                            updates.append("personal_email = %s")
                            update_values.append(new_value)
                            print(f"    ✓ User chose: Replace personal_email with {new_value}")
                            stats['emails_replaced_by_user'] += 1
                        elif decision == 'keep_current':
                            print(f"    ✓ User chose: Keep current personal_email {existing[5]}")
                            stats['emails_kept_by_user'] += 1
                        else:  # skip
                            print(f"    ✓ User chose: Skip this person")
                            stats['skipped_by_user'] += 1
                            skip_person = True
                    else:
                        # NULL - add it
                        updates.append("personal_email = %s")
                        update_values.append(new_value)
                        print(f"    → Adding personal_email: {new_value}")
                        stats['emails_added'] += 1

                if skip_person:
                    continue

                # Phone number - clean and only add if NULL, keep existing if differs
                if ml['phone_number']:
                    new_value = clean_phone_number(ml['phone_number'])
                    existing_clean = clean_phone_number(existing[6])
                    if existing_clean and existing_clean == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing_clean:
                        # Differs - keep existing (no prompt for phones)
                        print(f"    ⊘ Phone conflict: existing={existing[6]} / from_source={new_value} → keeping existing")
                        stats['phones_kept_conflict'] += 1
                    else:
                        # NULL - add it
                        updates.append("phone_number = %s")
                        update_values.append(new_value)
                        print(f"    → Adding phone: {new_value}")
                        stats['phones_added'] += 1

                # Update event counts if different
                if existing[7] != ml['event_attendance_count'] or existing[8] != ml['event_rsvp_count']:
                    updates.append("event_attendance_count = %s")
                    update_values.append(ml['event_attendance_count'])
                    updates.append("event_rsvp_count = %s")
                    update_values.append(ml['event_rsvp_count'])
                    print(f"    → Updating event counts:")
                    print(f"       Was: {existing[7]} attendance, {existing[8]} rsvp")
                    print(f"       Now: {ml['event_attendance_count']} attendance, {ml['event_rsvp_count']} rsvp")
                    stats['event_counts_updated'] += 1

                # Preferred email - prompt user if conflict
                if ml['preferred_email']:
                    new_value = ml['preferred_email'].lower()
                    if existing[9] and existing[9].lower() == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing[9]:
                        # Differs - prompt user
                        decision = prompt_user_for_email_conflict(
                            record_display, 'preferred_email', existing[9], new_value
                        )
                        if decision == 'use_new':
                            updates.append("preferred_email = %s")
                            update_values.append(new_value)
                            print(f"    ✓ User chose: Replace preferred_email with {new_value}")
                            stats['emails_replaced_by_user'] += 1
                        elif decision == 'keep_current':
                            print(f"    ✓ User chose: Keep current preferred_email {existing[9]}")
                            stats['emails_kept_by_user'] += 1
                        else:  # skip
                            print(f"    ✓ User chose: Skip this person")
                            stats['skipped_by_user'] += 1
                            skip_person = True
                    else:
                        # NULL - add it
                        updates.append("preferred_email = %s")
                        update_values.append(new_value)
                        print(f"    → Setting preferred_email: {new_value}")

                if skip_person:
                    continue

                # Execute updates if any
                if updates:
                    update_values.append(person_id)
                    sql = f"UPDATE People SET {', '.join(updates)} WHERE id = %s"
                    cursor.execute(sql, update_values)

            else:
                # Person doesn't exist - create new record
                print(f"  ✓ {record_display}: creating new person")
                stats['created_new'] += 1

                # Convert is_jewish to boolean
                is_jewish_bool = convert_is_jewish(ml['is_jewish'])

                cursor.execute("""
                    INSERT INTO People (
                        first_name, last_name, gender, class_year, is_jewish, school,
                        school_email, personal_email, preferred_email, phone_number,
                        event_attendance_count, event_rsvp_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    ml['first_name'], ml['last_name'], ml['gender'], ml['class_year'],
                    is_jewish_bool, ml['school'].lower() if ml['school'] else None,
                    ml['school_email'].lower() if ml['school_email'] else None,
                    ml['personal_email'].lower() if ml['personal_email'] else None,
                    ml['preferred_email'].lower() if ml['preferred_email'] else None,
                    ml['phone_number'],
                    ml['event_attendance_count'], ml['event_rsvp_count']
                ))

                new_person_id = cursor.fetchone()[0]
                print(f"    → Created person_id={new_person_id}")

        except Exception as e:
            print(f"  ✗ {record_display}: ERROR - {e}")
            stats['errors'] += 1

    return stats


def print_summary(stats):
    """Print migration summary."""
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    print(f"Total mailinglist records:     {stats['total_records']}")
    print(f"Matched to existing people:    {stats['matched_existing']}")
    print(f"Created new people:            {stats['created_new']}")
    print(f"Demographic fields updated:    {stats['fields_updated']}")
    print(f"Event counts updated:          {stats['event_counts_updated']}")
    print(f"Emails added:                  {stats['emails_added']}")
    print(f"Emails replaced (user choice): {stats['emails_replaced_by_user']}")
    print(f"Emails kept (user choice):     {stats['emails_kept_by_user']}")
    print(f"Phone numbers added:           {stats['phones_added']}")
    print(f"Phone conflicts (kept existing): {stats['phones_kept_conflict']}")
    print(f"Skipped (no change):           {stats['skipped_no_change']}")
    print(f"Skipped (user choice):         {stats['skipped_by_user']}")
    print(f"Errors:                        {stats['errors']}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Migrate mailinglist table data into people table'
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
        stats = migrate_mailinglist_to_people(conn, args.dry_run)

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
