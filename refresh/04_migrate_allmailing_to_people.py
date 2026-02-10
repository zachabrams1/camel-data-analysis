#!/usr/bin/env python3
"""
Migrate AllMailing table data into People table.

This script reads all rows from the AllMailing table and:
1. Matches to existing people by email (contact_value)
2. If person exists but missing name: updates with allmailing name
3. If person exists and has name but missing email: adds the email
4. If no match: creates new person with email and auto-detected school
5. **INTERACTIVE**: Prompts user to resolve email conflicts when values differ

When an email field (school_email, personal_email) has a conflict,
the script will pause and ask you to choose:
  1. Keep current value
  2. Replace with new value
  3. Skip this person entirely

Run this THIRD (last) after migrate_contacts_to_people.py and migrate_mailinglist_to_people.py

Note: AllMailing is denormalized (one row per email), so a person with 2 emails
has 2 rows. This script handles that by matching emails to existing people.

Usage:
    python3 04_migrate_allmailing_to_people.py [--dry-run]
"""

import psycopg2
import argparse
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


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


def detect_school_from_email(email):
    """
    Detect school from email domain.
    Returns 'harvard', 'mit', or 'other'.
    """
    if not email:
        return None

    email_lower = email.lower()

    # Harvard domains
    if '@harvard.edu' in email_lower or '@college.harvard.edu' in email_lower:
        # Check if it's a graduate school
        harvard_grad_schools = [
            '@hbs.edu', '@hms.harvard.edu', '@hsph.harvard.edu',
            '@gse.harvard.edu', '@hls.harvard.edu', '@gsd.harvard.edu',
            '@hks.harvard.edu', '@divinity.harvard.edu'
        ]
        for grad_school in harvard_grad_schools:
            if grad_school in email_lower:
                return 'other'
        return 'harvard'

    # MIT (same for undergrad and grad)
    if '@mit.edu' in email_lower:
        return 'mit'

    # Other Boston schools
    boston_schools = {
        '@bu.edu': 'other',
        '@northeastern.edu': 'other',
        '@tufts.edu': 'other',
        '@wellesley.edu': 'other',
        '@brandeis.edu': 'other',
        '@emerson.edu': 'other',
        '@suffolk.edu': 'other',
        '@berklee.edu': 'other',
        '@simmons.edu': 'other'
    }

    for domain, school in boston_schools.items():
        if domain in email_lower:
            return school

    return None


def is_school_email(email):
    """Check if email is a school email (vs personal)."""
    if not email:
        return False

    email_lower = email.lower()

    # Known school domains
    school_domains = [
        '@harvard.edu', '@college.harvard.edu', '@mit.edu',
        '@bu.edu', '@northeastern.edu', '@tufts.edu', '@wellesley.edu',
        '@brandeis.edu', '@emerson.edu', '@suffolk.edu', '@berklee.edu',
        '@simmons.edu', '@hbs.edu', '@hms.harvard.edu', '@hsph.harvard.edu',
        '@gse.harvard.edu', '@hls.harvard.edu', '@gsd.harvard.edu',
        '@hks.harvard.edu', '@divinity.harvard.edu'
    ]

    return any(domain in email_lower for domain in school_domains)


def find_person_by_email(cursor, email):
    """
    Try to find person by email in school_email or personal_email.
    Returns person_id or None.
    """
    cursor.execute("""
        SELECT id FROM People
        WHERE LOWER(school_email) = LOWER(%s)
           OR LOWER(personal_email) = LOWER(%s)
    """, (email, email))
    result = cursor.fetchone()
    return result[0] if result else None


def find_person_by_name(cursor, first_name, last_name):
    """
    Try to find person by name.
    Returns person_id or None.
    """
    if not first_name or not last_name:
        return None

    cursor.execute("""
        SELECT id FROM People
        WHERE LOWER(first_name) = LOWER(%s)
          AND LOWER(last_name) = LOWER(%s)
    """, (first_name, last_name))
    result = cursor.fetchone()
    return result[0] if result else None


def migrate_allmailing_to_people(conn, dry_run=False):
    """
    Migrate all allmailing records into people table.
    Returns stats dict with counts.
    """
    cursor = conn.cursor()

    stats = {
        'total_records': 0,
        'matched_by_email': 0,
        'matched_by_name': 0,
        'created_new': 0,
        'names_updated': 0,
        'emails_added': 0,
        'emails_replaced_by_user': 0,
        'emails_kept_by_user': 0,
        'schools_added': 0,
        'skipped_no_change': 0,
        'skipped_by_user': 0,
        'errors': 0
    }

    # Get all allmailing records
    cursor.execute("""
        SELECT
            id,
            first_name,
            last_name,
            school,
            contact_value,
            event_count
        FROM AllMailing
        ORDER BY id
    """)

    mailing_records = cursor.fetchall()
    stats['total_records'] = len(mailing_records)

    print(f"\nFound {len(mailing_records)} allmailing records to migrate\n")
    print("=" * 80)

    for record in mailing_records:
        am_id, first_name, last_name, school, contact_value, event_count = record

        record_display = f"{first_name or '(no name)'} {last_name or ''} <{contact_value}> (am_id={am_id})"

        try:
            # Try to find person by email first
            person_id = find_person_by_email(cursor, contact_value)

            if person_id:
                print(f"  ✓ {record_display}: matched by email to person_id={person_id}")
                stats['matched_by_email'] += 1

                # Get existing person data
                cursor.execute("""
                    SELECT
                        first_name, last_name, school,
                        school_email, personal_email
                    FROM People
                    WHERE id = %s
                """, (person_id,))
                existing = cursor.fetchone()

                updates = []
                update_values = []

                # If person has no name but allmailing has name, update it
                if (not existing[0] or not existing[1]) and first_name and last_name:
                    updates.append("first_name = %s")
                    updates.append("last_name = %s")
                    update_values.extend([first_name, last_name])
                    print(f"    → Adding name: {first_name} {last_name}")
                    stats['names_updated'] += 1

                # If person has no school but allmailing has school, update it
                if not existing[2] and school:
                    updates.append("school = %s")
                    update_values.append(school.lower())
                    print(f"    → Adding school: {school}")
                    stats['schools_added'] += 1

                # Check if email should be in school_email or personal_email field
                is_school = is_school_email(contact_value)
                new_value = contact_value.lower()
                skip_person = False

                if is_school:
                    # Should be in school_email field
                    if existing[3] and existing[3].lower() == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing[3]:
                        # Differs - prompt user
                        decision = prompt_user_for_email_conflict(
                            record_display, 'school_email', existing[3], new_value
                        )
                        if decision == 'use_new':
                            updates.append("school_email = %s")
                            update_values.append(new_value)
                            print(f"    ✓ User chose: Replace school_email with {new_value}")
                            stats['emails_replaced_by_user'] += 1
                        elif decision == 'keep_current':
                            print(f"    ✓ User chose: Keep current school_email {existing[3]}")
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
                else:
                    # Should be in personal_email field
                    if existing[4] and existing[4].lower() == new_value:
                        # Already matches - skip
                        stats['skipped_no_change'] += 1
                    elif existing[4]:
                        # Differs - prompt user
                        decision = prompt_user_for_email_conflict(
                            record_display, 'personal_email', existing[4], new_value
                        )
                        if decision == 'use_new':
                            updates.append("personal_email = %s")
                            update_values.append(new_value)
                            print(f"    ✓ User chose: Replace personal_email with {new_value}")
                            stats['emails_replaced_by_user'] += 1
                        elif decision == 'keep_current':
                            print(f"    ✓ User chose: Keep current personal_email {existing[4]}")
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

                # Execute updates if any
                if updates:
                    update_values.append(person_id)
                    sql = f"UPDATE People SET {', '.join(updates)} WHERE id = %s"
                    cursor.execute(sql, update_values)

            else:
                # Try to find by name
                person_id = find_person_by_name(cursor, first_name, last_name)

                if person_id:
                    print(f"  ✓ {record_display}: matched by name to person_id={person_id}")
                    stats['matched_by_name'] += 1

                    # Get existing person data
                    cursor.execute("""
                        SELECT school_email, personal_email, school
                        FROM People
                        WHERE id = %s
                    """, (person_id,))
                    existing = cursor.fetchone()

                    updates = []
                    update_values = []

                    # Add email to appropriate field - prompt user if conflict
                    is_school = is_school_email(contact_value)
                    new_value = contact_value.lower()
                    skip_person = False

                    if is_school:
                        # Should be in school_email field
                        if existing[0] and existing[0].lower() == new_value:
                            # Already matches - skip
                            stats['skipped_no_change'] += 1
                        elif existing[0]:
                            # Differs - prompt user
                            decision = prompt_user_for_email_conflict(
                                record_display, 'school_email', existing[0], new_value
                            )
                            if decision == 'use_new':
                                updates.append("school_email = %s")
                                update_values.append(new_value)
                                print(f"    ✓ User chose: Replace school_email with {new_value}")
                                stats['emails_replaced_by_user'] += 1
                            elif decision == 'keep_current':
                                print(f"    ✓ User chose: Keep current school_email {existing[0]}")
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
                    else:
                        # Should be in personal_email field
                        if existing[1] and existing[1].lower() == new_value:
                            # Already matches - skip
                            stats['skipped_no_change'] += 1
                        elif existing[1]:
                            # Differs - prompt user
                            decision = prompt_user_for_email_conflict(
                                record_display, 'personal_email', existing[1], new_value
                            )
                            if decision == 'use_new':
                                updates.append("personal_email = %s")
                                update_values.append(new_value)
                                print(f"    ✓ User chose: Replace personal_email with {new_value}")
                                stats['emails_replaced_by_user'] += 1
                            elif decision == 'keep_current':
                                print(f"    ✓ User chose: Keep current personal_email {existing[1]}")
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

                    # Add school if missing
                    if not existing[2]:
                        detected_school = school or detect_school_from_email(contact_value)
                        if detected_school:
                            updates.append("school = %s")
                            update_values.append(detected_school.lower())
                            print(f"    → Adding school: {detected_school}")
                            stats['schools_added'] += 1

                    # Execute updates if any
                    if updates:
                        update_values.append(person_id)
                        sql = f"UPDATE People SET {', '.join(updates)} WHERE id = %s"
                        cursor.execute(sql, update_values)

                else:
                    # No match found - create new person
                    print(f"  ✓ {record_display}: creating new person")
                    stats['created_new'] += 1

                    # Auto-detect school from email
                    detected_school = school or detect_school_from_email(contact_value)
                    is_school = is_school_email(contact_value)

                    # Determine which email field to use
                    school_email = contact_value.lower() if is_school else None
                    personal_email = contact_value.lower() if not is_school else None

                    cursor.execute("""
                        INSERT INTO People (
                            first_name, last_name, school,
                            school_email, personal_email, preferred_email
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        first_name or 'Unknown',
                        last_name or '',
                        detected_school.lower() if detected_school else None,
                        school_email,
                        personal_email,
                        contact_value.lower()  # preferred_email
                    ))

                    new_person_id = cursor.fetchone()[0]
                    school_str = f" [{detected_school}]" if detected_school else ""
                    print(f"    → Created person_id={new_person_id}{school_str}")

        except Exception as e:
            print(f"  ✗ {record_display}: ERROR - {e}")
            stats['errors'] += 1

    # Update preferred_email for all people who don't have it set
    print("\n" + "=" * 80)
    print("Updating preferred_email for people without it...")
    cursor.execute("""
        UPDATE People
        SET preferred_email = COALESCE(school_email, personal_email)
        WHERE preferred_email IS NULL
          AND (school_email IS NOT NULL OR personal_email IS NOT NULL)
    """)
    rows_updated = cursor.rowcount
    print(f"✓ Set preferred_email for {rows_updated} people")

    return stats


def print_summary(stats):
    """Print migration summary."""
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    print(f"Total allmailing records:      {stats['total_records']}")
    print(f"Matched by email:              {stats['matched_by_email']}")
    print(f"Matched by name:               {stats['matched_by_name']}")
    print(f"Created new people:            {stats['created_new']}")
    print(f"Names updated:                 {stats['names_updated']}")
    print(f"Emails added:                  {stats['emails_added']}")
    print(f"Emails replaced (user choice): {stats['emails_replaced_by_user']}")
    print(f"Emails kept (user choice):     {stats['emails_kept_by_user']}")
    print(f"Schools added:                 {stats['schools_added']}")
    print(f"Skipped (no change):           {stats['skipped_no_change']}")
    print(f"Skipped (user choice):         {stats['skipped_by_user']}")
    print(f"Errors:                        {stats['errors']}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Migrate allmailing table data into people table'
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
        stats = migrate_allmailing_to_people(conn, args.dry_run)

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
