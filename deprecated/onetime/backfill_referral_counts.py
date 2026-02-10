#!/usr/bin/env python3
"""
Backfill Referral Counts Script

This script updates the referral_count column in the People table based on
existing attendance records in the database. It counts how many people each
person has successfully referred (i.e., people who checked in to events).

Usage:
    python backfill_referral_counts.py
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
from difflib import SequenceMatcher
import pandas as pd

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD')
}

def fuzzy_ratio(str_a, str_b):
    """Calculate fuzzy string match ratio."""
    return SequenceMatcher(None, str_a, str_b).ratio()

def match_tracking_link_to_person(conn, link_value, fuzzy_threshold=0.8):
    """
    Match a tracking link value to a person in the database using fuzzy matching.

    Args:
        conn: Database connection
        link_value: The tracking link string (e.g., "doron", "[name]", "admlzr")
        fuzzy_threshold: Fuzzy matching threshold (default 0.8)

    Returns:
        person_id if match found, else None
    """
    if not link_value or pd.isna(link_value):
        return None

    # Clean the link value
    link_value = str(link_value).strip().lower()

    # Skip generic tracking codes that don't represent personal referrals
    generic_codes = {
        'default', 'emailreferral', 'email_first_button',
        'email_second_button', 'email', 'txt', 'insta',
        'maillist', 'lastname', '[name]'
    }
    if link_value in generic_codes:
        return None

    # Try to extract a name from the link value
    # Remove common prefixes/suffixes
    clean_name = link_value.replace('_', ' ').replace('-', ' ').strip()

    # Get all people from database
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, first_name, last_name FROM People")
        all_people = cur.fetchall()

    # Try exact match on first name or last name
    for person in all_people:
        first = person['first_name'].lower() if person['first_name'] else ''
        last = person['last_name'].lower() if person['last_name'] else ''

        if clean_name == first or clean_name == last:
            return person['id']

    # Try fuzzy matching on first name or last name
    best_match = None
    best_ratio = 0

    for person in all_people:
        first = person['first_name'].lower() if person['first_name'] else ''
        last = person['last_name'].lower() if person['last_name'] else ''

        # Check fuzzy match against first name
        if first:
            ratio = fuzzy_ratio(clean_name, first)
            if ratio >= fuzzy_threshold and ratio > best_ratio:
                best_ratio = ratio
                best_match = person['id']

        # Check fuzzy match against last name
        if last:
            ratio = fuzzy_ratio(clean_name, last)
            if ratio >= fuzzy_threshold and ratio > best_ratio:
                best_ratio = ratio
                best_match = person['id']

    return best_match

def backfill_referral_counts():
    """Backfill referral counts for all people based on existing attendance records."""

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        print("Starting referral count backfill...")
        print("=" * 60)

        # Reset all referral counts to 0
        with conn.cursor() as cur:
            cur.execute("UPDATE People SET referral_count = 0")
            conn.commit()
        print("✓ Reset all referral counts to 0")

        # Get all attendance records where people checked in
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    a.id as attendance_id,
                    a.person_id,
                    a.event_id,
                    it.value as tracking_link,
                    p.first_name || ' ' || p.last_name as attendee_name
                FROM Attendance a
                JOIN InviteTokens it ON a.invite_token_id = it.id
                JOIN People p ON a.person_id = p.id
                WHERE a.checked_in = TRUE
                ORDER BY a.event_id, a.id
            """)
            attendance_records = cur.fetchall()

        print(f"Found {len(attendance_records)} checked-in attendance records")
        print("=" * 60)

        # Track referral counts per person
        referral_counts = {}
        total_matched = 0
        total_skipped = 0

        for idx, record in enumerate(attendance_records, 1):
            tracking_link = record['tracking_link']
            person_id = record['person_id']
            attendee_name = record['attendee_name']

            # Try to match tracking link to a referrer
            referrer_id = match_tracking_link_to_person(conn, tracking_link)

            if referrer_id and referrer_id != person_id:  # Don't count self-referrals
                if referrer_id not in referral_counts:
                    referral_counts[referrer_id] = 0
                referral_counts[referrer_id] += 1
                total_matched += 1

                if idx % 100 == 0:
                    print(f"Processed {idx}/{len(attendance_records)} records... ({total_matched} matched)")
            else:
                total_skipped += 1

        print(f"\n{'=' * 60}")
        print(f"Processing complete:")
        print(f"  Total records: {len(attendance_records)}")
        print(f"  Matched to referrers: {total_matched}")
        print(f"  Skipped (generic/self-referral): {total_skipped}")
        print(f"  Unique referrers: {len(referral_counts)}")
        print(f"{'=' * 60}\n")

        # Update referral counts in database
        print("Updating referral counts in database...")
        with conn.cursor() as cur:
            for referrer_id, count in referral_counts.items():
                cur.execute("""
                    UPDATE People
                    SET referral_count = %s
                    WHERE id = %s
                """, (count, referrer_id))
        conn.commit()
        print(f"✓ Updated {len(referral_counts)} people with referral counts")

        # Show top referrers
        print(f"\n{'=' * 60}")
        print("Top 20 Referrers:")
        print(f"{'=' * 60}")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    id,
                    first_name,
                    last_name,
                    referral_count
                FROM People
                WHERE referral_count > 0
                ORDER BY referral_count DESC
                LIMIT 20
            """)
            top_referrers = cur.fetchall()

            for i, person in enumerate(top_referrers, 1):
                print(f"{i:2d}. {person['first_name']} {person['last_name']}: {person['referral_count']} referrals")

        print(f"\n{'=' * 60}")
        print("✓ Backfill complete!")
        print(f"{'=' * 60}")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    backfill_referral_counts()
