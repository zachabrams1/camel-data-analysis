#!/usr/bin/env python3
"""Check for duplicate people"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD')
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    print("=== CHECKING FOR DUPLICATE PEOPLE ===\n")

    # Check for duplicate names
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT LOWER(first_name) as first_lower, LOWER(last_name) as last_lower, COUNT(*) as count
            FROM People
            WHERE LOWER(first_name) IN ('rohan', 'nahida', 'sami', 'gideon', 'garrick', 'james')
              AND LOWER(last_name) IN ('deb', 'shehab', 'syed', 'bialkin', 'schwartz', 'byrd')
            GROUP BY LOWER(first_name), LOWER(last_name)
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()

    if duplicates:
        print("⚠️  DUPLICATES FOUND:\n")
        for dup in duplicates:
            print(f"{dup['first_lower'].title()} {dup['last_lower'].title()} - {dup['count']} records")

            # Get details of each duplicate
            with conn.cursor(cursor_factory=RealDictCursor) as cur2:
                cur2.execute("""
                    SELECT p.id, p.first_name, p.last_name, p.school,
                           STRING_AGG(c.contact_value, ', ') as contacts
                    FROM People p
                    LEFT JOIN Contacts c ON p.id = c.person_id
                    WHERE LOWER(p.first_name) = %s
                      AND LOWER(p.last_name) = %s
                    GROUP BY p.id, p.first_name, p.last_name, p.school
                    ORDER BY p.id
                """, (dup['first_lower'], dup['last_lower']))
                records = cur2.fetchall()

                for rec in records:
                    print(f"  - ID {rec['id']}: {rec['first_name']} {rec['last_name']} | {rec['school']} | {rec['contacts']}")
            print()
    else:
        print("✓ No duplicates found among these names")

    # Check the latest event
    print("\n=== LATEST EVENT ===")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, event_name, start_datetime FROM Events ORDER BY id DESC LIMIT 1")
        latest_event = cur.fetchone()
        if latest_event:
            print(f"Event ID: {latest_event['id']}")
            print(f"Name: {latest_event['event_name']}")
            print(f"Date: {latest_event['start_datetime']}")

            # Check attendance for this event
            cur.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN checked_in THEN 1 ELSE 0 END) as checked_in,
                       SUM(CASE WHEN rsvp THEN 1 ELSE 0 END) as rsvp
                FROM Attendance
                WHERE event_id = %s
            """, (latest_event['id'],))
            stats = cur.fetchone()
            print(f"\nAttendance records: {stats['total']}")
            print(f"Checked in: {stats['checked_in']}")
            print(f"RSVP'd: {stats['rsvp']}")

    conn.close()

if __name__ == "__main__":
    main()
