#!/usr/bin/env python3
"""Check recently added people"""

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

    # Get the last 20 people added (highest IDs)
    print("=== MOST RECENTLY ADDED PEOPLE (Last 20) ===\n")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT p.id, p.first_name, p.last_name, p.school,
                   STRING_AGG(c.contact_value, ', ') as contacts
            FROM People p
            LEFT JOIN Contacts c ON p.id = c.person_id
            GROUP BY p.id, p.first_name, p.last_name, p.school
            ORDER BY p.id DESC
            LIMIT 20
        """)
        recent_people = cur.fetchall()

    for person in recent_people:
        print(f"ID: {person['id']:4d} | {person['first_name']} {person['last_name']} | {person['school']} | {person['contacts']}")

    # Check for the specific people from the error messages
    print("\n\n=== CHECKING FOR PEOPLE FROM ERROR MESSAGES ===\n")

    names_to_check = [
        ('Rohan', 'Deb'),
        ('Nahida', 'Shehab'),
        ('Sami', 'Syed'),
        ('Gideon', 'Bialkin'),
        ('Garrick', 'Schwartz'),
        ('James', 'Byrd')
    ]

    for first, last in names_to_check:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT p.id, p.first_name, p.last_name,
                       COUNT(c.id) as contact_count,
                       STRING_AGG(c.contact_value, ', ') as contacts
                FROM People p
                LEFT JOIN Contacts c ON p.id = c.person_id
                WHERE LOWER(p.first_name) = LOWER(%s)
                  AND LOWER(p.last_name) = LOWER(%s)
                GROUP BY p.id, p.first_name, p.last_name
            """, (first, last))
            results = cur.fetchall()

            if results:
                for r in results:
                    print(f"{r['first_name']} {r['last_name']} - ID: {r['id']} ({r['contact_count']} contacts)")
                    print(f"  Contacts: {r['contacts']}")
            else:
                print(f"{first} {last} - NOT FOUND")

    conn.close()

if __name__ == "__main__":
    main()
