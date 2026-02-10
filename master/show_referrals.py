#!/usr/bin/env python3
"""
Display all people who have made referrals (referral_count > 0).

This script connects to the Railway database and prints a list of people
who have successfully referred others, along with their referral counts.
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD'),
}


def get_db_connection():
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def fetch_people_with_referrals(conn):
    """
    Fetch all people who have referral_count > 0.

    Returns:
        list: List of dictionaries with first_name, last_name, referral_count
    """
    query = """
    SELECT
        first_name,
        last_name,
        referral_count
    FROM People
    WHERE referral_count > 0
    ORDER BY referral_count DESC, last_name, first_name
    """

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            people = cur.fetchall()
            return people
    except psycopg2.Error as e:
        print(f"Error fetching people with referrals: {e}")
        sys.exit(1)


def print_referrals(people):
    """
    Print the list of people with referrals in a formatted table.

    Args:
        people (list): List of people dictionaries
    """
    if not people:
        print("\nNo people with referrals found in the database.")
        return

    print("\n" + "="*80)
    print("PEOPLE WITH REFERRALS")
    print("="*80)

    # Calculate column widths
    max_first_name = max(len(p['first_name'] or '') for p in people)
    max_last_name = max(len(p['last_name'] or '') for p in people)

    # Ensure minimum column widths
    first_name_width = max(max_first_name, len("First Name"))
    last_name_width = max(max_last_name, len("Last Name"))

    # Print header
    header = f"{'First Name':<{first_name_width}}  {'Last Name':<{last_name_width}}  Referral Count"
    print(f"\n{header}")
    print("-" * len(header))

    # Print each person
    for person in people:
        first_name = person['first_name'] or ''
        last_name = person['last_name'] or ''
        referral_count = person['referral_count']

        print(f"{first_name:<{first_name_width}}  {last_name:<{last_name_width}}  {referral_count}")

    # Print summary
    total_people = len(people)
    total_referrals = sum(p['referral_count'] for p in people)

    print("\n" + "-" * len(header))
    print(f"Total: {total_people} people with {total_referrals} referrals")
    print("="*80 + "\n")


def main():
    """Main program execution."""
    # Connect to database
    conn = get_db_connection()

    try:
        # Fetch people with referrals
        people = fetch_people_with_referrals(conn)

        # Print results
        print_referrals(people)

    finally:
        # Close database connection
        conn.close()


if __name__ == "__main__":
    main()
