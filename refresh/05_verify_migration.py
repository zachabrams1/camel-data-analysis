#!/usr/bin/env python3
"""
Verify Migration Completeness

This script checks that all data from contacts, mailinglist, and allmailing
has been successfully migrated to the people table. It identifies any missing
or orphaned data.

Run this AFTER all three migration scripts to verify success.

Usage:
    python3 05_verify_migration.py
"""

import psycopg2
import argparse
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def verify_contacts_migration(cursor):
    """Verify all contacts data is in people table."""
    print("\n" + "=" * 80)
    print("CHECKING: Contacts → People Migration")
    print("=" * 80)

    # Count contacts
    cursor.execute("SELECT COUNT(*) FROM Contacts")
    total_contacts = cursor.fetchone()[0]
    print(f"Total contacts in Contacts table: {total_contacts}")

    # Count school emails
    cursor.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'school email'")
    school_email_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM People WHERE school_email IS NOT NULL")
    people_school_email_count = cursor.fetchone()[0]
    print(f"School emails: {school_email_count} in Contacts, {people_school_email_count} in People")

    # Count personal emails
    cursor.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'personal email'")
    personal_email_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM People WHERE personal_email IS NOT NULL")
    people_personal_email_count = cursor.fetchone()[0]
    print(f"Personal emails: {personal_email_count} in Contacts, {people_personal_email_count} in People")

    # Count phones
    cursor.execute("SELECT COUNT(*) FROM Contacts WHERE contact_type = 'phone'")
    phone_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM People WHERE phone_number IS NOT NULL")
    people_phone_count = cursor.fetchone()[0]
    print(f"Phone numbers: {phone_count} in Contacts, {people_phone_count} in People")

    # Find orphan contacts (emails in Contacts but not in People)
    cursor.execute("""
        SELECT c.contact_type, c.contact_value, p.first_name, p.last_name
        FROM Contacts c
        JOIN People p ON c.person_id = p.id
        WHERE c.contact_type = 'school email'
          AND (p.school_email IS NULL OR LOWER(p.school_email) != LOWER(c.contact_value))
        LIMIT 5
    """)
    orphan_school = cursor.fetchall()
    if orphan_school:
        print(f"\n⚠ WARNING: Found {len(orphan_school)} school emails in Contacts not in People.school_email:")
        for contact in orphan_school[:5]:
            print(f"  - {contact[2]} {contact[3]}: {contact[1]}")

    cursor.execute("""
        SELECT c.contact_type, c.contact_value, p.first_name, p.last_name
        FROM Contacts c
        JOIN People p ON c.person_id = p.id
        WHERE c.contact_type = 'personal email'
          AND (p.personal_email IS NULL OR LOWER(p.personal_email) != LOWER(c.contact_value))
        LIMIT 5
    """)
    orphan_personal = cursor.fetchall()
    if orphan_personal:
        print(f"\n⚠ WARNING: Found {len(orphan_personal)} personal emails in Contacts not in People.personal_email:")
        for contact in orphan_personal[:5]:
            print(f"  - {contact[2]} {contact[3]}: {contact[1]}")

    if not orphan_school and not orphan_personal:
        print("\n✓ All contacts successfully migrated to People table!")

    return {
        'total_contacts': total_contacts,
        'orphans': len(orphan_school) + len(orphan_personal)
    }


def verify_mailinglist_migration(cursor):
    """Verify all mailinglist data is in people table."""
    print("\n" + "=" * 80)
    print("CHECKING: MailingList → People Migration")
    print("=" * 80)

    # Count mailinglist records
    cursor.execute("SELECT COUNT(*) FROM MailingList")
    total_mailinglist = cursor.fetchone()[0]
    print(f"Total records in MailingList: {total_mailinglist}")

    cursor.execute("SELECT COUNT(*) FROM People")
    total_people = cursor.fetchone()[0]
    print(f"Total records in People: {total_people}")

    # Find emails in mailinglist not in people
    cursor.execute("""
        SELECT ml.first_name, ml.last_name, ml.school_email, ml.personal_email
        FROM MailingList ml
        WHERE ml.school_email IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM People p
              WHERE LOWER(p.school_email) = LOWER(ml.school_email)
                 OR LOWER(p.personal_email) = LOWER(ml.school_email)
          )
        LIMIT 5
    """)
    orphan_emails = cursor.fetchall()

    if orphan_emails:
        print(f"\n⚠ WARNING: Found emails in MailingList not in People:")
        for record in orphan_emails[:5]:
            print(f"  - {record[0]} {record[1]}: {record[2]}")
    else:
        print("\n✓ All mailinglist emails found in People table!")

    # Check event counts
    cursor.execute("""
        SELECT COUNT(*)
        FROM MailingList ml
        JOIN People p ON (
            LOWER(p.school_email) = LOWER(ml.school_email)
            OR LOWER(p.personal_email) = LOWER(ml.personal_email)
            OR (LOWER(p.first_name) = LOWER(ml.first_name) AND LOWER(p.last_name) = LOWER(ml.last_name))
        )
        WHERE p.event_attendance_count = ml.event_attendance_count
          AND p.event_rsvp_count = ml.event_rsvp_count
    """)
    matching_counts = cursor.fetchone()[0]
    print(f"\nEvent counts match: {matching_counts}/{total_mailinglist} records")

    return {
        'total_mailinglist': total_mailinglist,
        'orphan_emails': len(orphan_emails)
    }


def verify_allmailing_migration(cursor):
    """Verify all allmailing data is in people table."""
    print("\n" + "=" * 80)
    print("CHECKING: AllMailing → People Migration")
    print("=" * 80)

    # Count allmailing records
    cursor.execute("SELECT COUNT(*) FROM AllMailing")
    total_allmailing = cursor.fetchone()[0]
    print(f"Total records in AllMailing: {total_allmailing}")

    # Count unique emails in allmailing
    cursor.execute("SELECT COUNT(DISTINCT LOWER(contact_value)) FROM AllMailing")
    unique_emails = cursor.fetchone()[0]
    print(f"Unique emails in AllMailing: {unique_emails}")

    # Count emails in people
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT school_email FROM People WHERE school_email IS NOT NULL
            UNION
            SELECT personal_email FROM People WHERE personal_email IS NOT NULL
        ) AS all_emails
    """)
    people_emails = cursor.fetchone()[0]
    print(f"Total emails in People: {people_emails}")

    # Find emails in allmailing not in people
    cursor.execute("""
        SELECT am.first_name, am.last_name, am.contact_value, am.school
        FROM AllMailing am
        WHERE NOT EXISTS (
            SELECT 1 FROM People p
            WHERE LOWER(p.school_email) = LOWER(am.contact_value)
               OR LOWER(p.personal_email) = LOWER(am.contact_value)
        )
        LIMIT 10
    """)
    orphan_emails = cursor.fetchall()

    if orphan_emails:
        print(f"\n⚠ WARNING: Found {len(orphan_emails)} emails in AllMailing not in People:")
        for record in orphan_emails[:10]:
            print(f"  - {record[0]} {record[1]} <{record[2]}> [{record[3] or 'no school'}]")
        print("\n  These should be investigated - may indicate migration issue!")
    else:
        print("\n✓ All allmailing emails found in People table!")

    return {
        'total_allmailing': total_allmailing,
        'unique_emails': unique_emails,
        'orphan_emails': len(orphan_emails)
    }


def verify_preferred_email(cursor):
    """Verify preferred_email is set correctly."""
    print("\n" + "=" * 80)
    print("CHECKING: Preferred Email Logic")
    print("=" * 80)

    # Count people with emails
    cursor.execute("""
        SELECT COUNT(*)
        FROM People
        WHERE school_email IS NOT NULL OR personal_email IS NOT NULL
    """)
    people_with_emails = cursor.fetchone()[0]

    # Count people with preferred_email set
    cursor.execute("""
        SELECT COUNT(*)
        FROM People
        WHERE preferred_email IS NOT NULL
    """)
    people_with_preferred = cursor.fetchone()[0]

    print(f"People with emails: {people_with_emails}")
    print(f"People with preferred_email set: {people_with_preferred}")

    # Check if preferred_email logic is correct
    cursor.execute("""
        SELECT COUNT(*)
        FROM People
        WHERE (school_email IS NOT NULL OR personal_email IS NOT NULL)
          AND preferred_email = COALESCE(school_email, personal_email)
    """)
    correct_preferred = cursor.fetchone()[0]

    print(f"Preferred_email correctly set: {correct_preferred}/{people_with_emails}")

    if correct_preferred == people_with_emails and people_with_emails == people_with_preferred:
        print("\n✓ Preferred email logic is correct!")
    else:
        print("\n⚠ WARNING: Some preferred_email values may be incorrect")

    return {
        'people_with_emails': people_with_emails,
        'correctly_set': correct_preferred
    }


def print_final_summary(results):
    """Print final summary of verification."""
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)

    total_issues = (
        results['contacts']['orphans'] +
        results['mailinglist']['orphan_emails'] +
        results['allmailing']['orphan_emails']
    )

    if total_issues == 0:
        print("✓ ALL CHECKS PASSED!")
        print("✓ All data successfully migrated to People table")
        print("✓ No orphaned data found")
        print("\nYou can proceed with the next steps:")
        print("  1. Create SQL views to replace mailinglist/allmailing")
        print("  2. Update application scripts to use People table")
        print("  3. Drop redundant tables (after thorough testing)")
    else:
        print(f"⚠ WARNING: Found {total_issues} potential issues")
        print("\nReview the warnings above and:")
        print("  1. Investigate orphaned data")
        print("  2. Re-run migration scripts if needed")
        print("  3. Manually resolve any conflicts")
        print("\nDO NOT proceed with dropping tables until issues are resolved!")

    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Verify migration completeness'
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
        conn.autocommit = True
        print("✓ Connected successfully")

        cursor = conn.cursor()

        # Run all verification checks
        results = {
            'contacts': verify_contacts_migration(cursor),
            'mailinglist': verify_mailinglist_migration(cursor),
            'allmailing': verify_allmailing_migration(cursor),
            'preferred_email': verify_preferred_email(cursor)
        }

        # Print final summary
        print_final_summary(results)

        conn.close()
        return 0

    except psycopg2.Error as e:
        print(f"\n✗ Database error: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
