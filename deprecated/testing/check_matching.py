#!/usr/bin/env python3
"""Check database matching logic"""

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

    # Check for the people mentioned in error messages
    test_cases = [
        ('rohan', 'deb', 'rohandeb@mit.edu', '+19083461606'),
        ('nahida', 'shehab', 'nahidash@mit.edu', '+12023453997'),
        ('sami', 'syed', 'sami8999@mit.edu', '+16175401694'),
        ('gideon', 'bialkin', 'gideonbialkin@college.harvard.edu', '+19175978692'),
        ('garrick', 'schwartz', 'garrickschwartz@gmail.com', '+16466961175'),
        ('james', 'byrd', 'jebyrd@mit.edu', '+15096076552'),
    ]

    print("=== CHECKING DATABASE FOR MATCHING CANDIDATES ===\n")

    for first, last, email, phone in test_cases:
        print(f"\n--- Checking: {first} {last} ---")
        print(f"    Email: {email}")
        print(f"    Phone: {phone}")

        # Check exact name match
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM People WHERE LOWER(first_name) = %s AND LOWER(last_name) = %s",
                (first.lower(), last.lower())
            )
            name_matches = cur.fetchall()

        if name_matches:
            print(f"    ✓ Found {len(name_matches)} exact name match(es)")
            for match in name_matches:
                print(f"      - ID: {match['id']}, Name: {match['first_name']} {match['last_name']}")
        else:
            print(f"    ✗ No exact name matches")

        # Check email match
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT person_id, contact_value FROM Contacts WHERE LOWER(contact_value) = %s",
                (email.lower(),)
            )
            email_matches = cur.fetchall()

        if email_matches:
            print(f"    ✓ Email found in database")
            for match in email_matches:
                print(f"      - Person ID: {match['person_id']}, Email: {match['contact_value']}")
        else:
            print(f"    ✗ Email NOT in database")

        # Check phone match
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT person_id, contact_value FROM Contacts WHERE contact_value = %s",
                (phone,)
            )
            phone_matches = cur.fetchall()

        if phone_matches:
            print(f"    ✓ Phone found in database")
            for match in phone_matches:
                print(f"      - Person ID: {match['person_id']}, Phone: {match['contact_value']}")
        else:
            print(f"    ✗ Phone NOT in database")

    # Check fuzzy matching candidates
    print("\n\n=== CHECKING FOR SIMILAR NAMES (FUZZY MATCHES) ===\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, first_name, last_name FROM People WHERE LOWER(first_name) LIKE 'rohan%' OR LOWER(first_name) LIKE 'gideon%' OR LOWER(first_name) LIKE 'garrick%'")
        similar = cur.fetchall()

    if similar:
        print("Found people with similar names:")
        for person in similar:
            print(f"  - ID: {person['id']}, Name: {person['first_name']} {person['last_name']}")
    else:
        print("No similar names found")

    # Show total counts
    print("\n\n=== DATABASE SUMMARY ===")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM People")
        people_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM Contacts")
        contacts_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM Events")
        events_count = cur.fetchone()[0]

    print(f"Total people: {people_count}")
    print(f"Total contacts: {contacts_count}")
    print(f"Total events: {events_count}")

    conn.close()

if __name__ == "__main__":
    main()
