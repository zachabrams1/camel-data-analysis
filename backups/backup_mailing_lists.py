#!/usr/bin/env python3
"""
Backup MailingList and AllMailing tables before import

Usage:
    python backup_mailing_lists.py
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import csv
from datetime import datetime

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD')
}

def get_db_connection():
    """Establish database connection."""
    return psycopg2.connect(**DB_CONFIG)

def backup_table(table_name, output_file):
    """Backup a table to CSV file."""
    conn = get_db_connection()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()

            if rows:
                # Get column names from first row
                fieldnames = list(rows[0].keys())

                # Write to CSV
                with open(output_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                print(f"✓ Backed up {len(rows)} rows from {table_name} to {output_file}")
            else:
                print(f"⚠ {table_name} is empty, no backup created")

    except Exception as e:
        print(f"Error backing up {table_name}: {e}")
        raise
    finally:
        conn.close()

def main():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("=== Backing Up Mailing List Tables ===\n")

    # Backup MailingList
    mailing_list_file = f"backups/MailingList_backup_{timestamp}.csv"
    os.makedirs("backups", exist_ok=True)
    backup_table("MailingList", mailing_list_file)

    # Backup AllMailing
    all_mailing_file = f"backups/AllMailing_backup_{timestamp}.csv"
    backup_table("AllMailing", all_mailing_file)

    print(f"\n✓ Backups complete!")
    print(f"  MailingList: {mailing_list_file}")
    print(f"  AllMailing: {all_mailing_file}")

if __name__ == "__main__":
    main()
