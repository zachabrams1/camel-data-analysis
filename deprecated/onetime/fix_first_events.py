#!/usr/bin/env python3
"""
Fix is_first_event flags in the attendance table.

For each person who doesn't have any attendance record marked as their first event,
this script will mark their earliest event (by rsvp_datetime) as their first event.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def fix_first_events(dry_run=True):
    """
    Fix is_first_event flags for all people.

    Args:
        dry_run: If True, only show what would be changed without making changes
    """
    # Connect to the database
    conn = psycopg2.connect(
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        host=os.getenv('PGHOST'),
        port=os.getenv('PGPORT')
    )

    cur = conn.cursor()

    # Find all person_ids that don't have any is_first_event = True
    print("Finding people without a first event marked...")
    cur.execute("""
        SELECT DISTINCT a.person_id
        FROM attendance a
        WHERE a.person_id NOT IN (
            SELECT person_id
            FROM attendance
            WHERE is_first_event = TRUE
        )
        ORDER BY a.person_id;
    """)

    people_without_first = cur.fetchall()
    print(f"Found {len(people_without_first)} people without a first event marked.\n")

    if len(people_without_first) == 0:
        print("No fixes needed!")
        cur.close()
        conn.close()
        return

    # For each person, find their earliest event
    updates = []
    for (person_id,) in people_without_first:
        cur.execute("""
            SELECT id, event_id, rsvp_datetime
            FROM attendance
            WHERE person_id = %s
            ORDER BY rsvp_datetime ASC
            LIMIT 1;
        """, (person_id,))

        result = cur.fetchone()
        if result:
            attendance_id, event_id, rsvp_datetime = result
            updates.append({
                'attendance_id': attendance_id,
                'person_id': person_id,
                'event_id': event_id,
                'rsvp_datetime': rsvp_datetime
            })

    # Display the changes
    print("=" * 80)
    print("Changes to be made:")
    print("=" * 80)
    for update in updates:
        print(f"Person ID: {update['person_id']:4d} | "
              f"Attendance ID: {update['attendance_id']:4d} | "
              f"Event ID: {update['event_id']:2d} | "
              f"Date: {update['rsvp_datetime']}")

    print(f"\nTotal updates: {len(updates)}")

    if dry_run:
        print("\n*** DRY RUN MODE - No changes made ***")
        print("Run with --commit flag to apply changes")
    else:
        # Apply the updates
        print("\nApplying updates...")
        for update in updates:
            cur.execute("""
                UPDATE attendance
                SET is_first_event = TRUE
                WHERE id = %s;
            """, (update['attendance_id'],))

        conn.commit()
        print(f"✓ Successfully updated {len(updates)} records!")

    cur.close()
    conn.close()

if __name__ == "__main__":
    import sys

    # Check for --commit flag
    commit = '--commit' in sys.argv

    if commit:
        print("COMMIT MODE - Changes will be applied to the database")
        response = input("Are you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)
    else:
        print("DRY RUN MODE - No changes will be made")
        print("Run with --commit flag to apply changes\n")

    fix_first_events(dry_run=not commit)
