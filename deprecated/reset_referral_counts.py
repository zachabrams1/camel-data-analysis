#!/usr/bin/env python3
"""
Reset Referral Counts to Zero

This script resets the referral_count field to 0 for all people in the database.

Usage:
    python reset_referral_counts.py
"""

import psycopg2
from dotenv import load_dotenv
import os

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

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_CONFIG)

def reset_referral_counts():
    """Reset all referral_count values to 0 in the People table."""
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # First, get count of people with non-zero referral counts
            cur.execute("SELECT COUNT(*) FROM People WHERE referral_count > 0")
            count_before = cur.fetchone()[0]

            print(f"Found {count_before} people with non-zero referral counts")

            if count_before == 0:
                print("All referral counts are already 0. Nothing to reset.")
                return

            # Confirm with user
            confirm = input(f"\nReset referral_count to 0 for all {count_before} people? (yes/no): ")

            if confirm.lower() not in ['yes', 'y']:
                print("Operation cancelled.")
                return

            # Reset all referral counts to 0
            cur.execute("UPDATE People SET referral_count = 0 WHERE referral_count > 0")
            rows_updated = cur.rowcount
            conn.commit()

            print(f"✓ Successfully reset referral_count to 0 for {rows_updated} people")

            # Verify
            cur.execute("SELECT COUNT(*) FROM People WHERE referral_count > 0")
            count_after = cur.fetchone()[0]

            if count_after == 0:
                print("✓ Verification: All referral counts are now 0")
            else:
                print(f"⚠️  Warning: {count_after} people still have non-zero referral counts")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

def main():
    print("=== Reset Referral Counts ===\n")

    # Test connection
    try:
        conn = get_db_connection()
        print("✓ Connected to database successfully\n")
        conn.close()

        # Reset counts
        reset_referral_counts()

        print("\n✓ Done!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
