#!/usr/bin/env python3
"""Check for duplicate IDs in all CSV files."""

import csv

def check_csv_id_duplicates(filename, id_column='id'):
    """Check for duplicate IDs in a CSV file."""
    print(f"\nChecking {filename}...")

    seen = {}
    duplicates = []

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            if id_column not in reader.fieldnames:
                print(f"  ⚠️  Column '{id_column}' not found. Available: {reader.fieldnames}")
                return 0

            for i, row in enumerate(reader, start=2):
                row_id = row[id_column]

                if row_id in seen:
                    duplicates.append(row_id)
                    print(f"  ⚠️  Duplicate {id_column}: {row_id} (row {i}, first seen at row {seen[row_id]})")
                else:
                    seen[row_id] = i

        if duplicates:
            print(f"  ✗ Found {len(duplicates)} duplicate {id_column}s")
        else:
            print(f"  ✓ No duplicate {id_column}s found ({len(seen)} unique entries)")

        return len(duplicates)
    except FileNotFoundError:
        print(f"  ⚠️  File not found")
        return 0

if __name__ == '__main__':
    print("=" * 80)
    print("CHECKING ALL CSV FILES FOR DUPLICATE IDs")
    print("=" * 80)

    total = 0
    total += check_csv_id_duplicates('final/people.csv', 'id')
    total += check_csv_id_duplicates('final/contacts.csv', 'id')
    total += check_csv_id_duplicates('final/events.csv', 'id')
    total += check_csv_id_duplicates('final/attendance.csv', 'id')
    total += check_csv_id_duplicates('final/invite_tokens.csv', 'id')

    print("\n" + "=" * 80)
    print(f"TOTAL DUPLICATE IDs FOUND: {total}")
    print("=" * 80)
