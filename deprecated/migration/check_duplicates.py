#!/usr/bin/env python3
"""Check for and remove duplicate entries in CSV files."""

import csv
from collections import defaultdict

def check_contacts_duplicates():
    """Check for duplicate contacts based on (person_id, contact_type, contact_value).
    Uses case-insensitive matching for email addresses."""
    print("=" * 80)
    print("CHECKING CONTACTS.CSV FOR DUPLICATES")
    print("=" * 80)

    seen = {}  # key: (person_id, contact_type, normalized_value), value: row_info
    duplicates = []

    with open('final/contacts.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # start=2 because row 1 is header
            person_id = row['person_id']
            contact_type = row['contact_type']
            contact_value = row['contact_value']

            # Normalize for case-insensitive email comparison (per CLAUDE.md)
            if 'email' in contact_type.lower():
                normalized_value = contact_value.lower()
            else:
                normalized_value = contact_value

            key = (person_id, contact_type, normalized_value)

            if key in seen:
                duplicates.append({
                    'key': key,
                    'first_row': seen[key],
                    'duplicate_row': i,
                    'id_first': seen[key]['id'],
                    'id_dup': row['id']
                })
                print(f"\n⚠️  DUPLICATE FOUND:")
                print(f"   Person ID: {person_id}")
                print(f"   Contact Type: {contact_type}")
                print(f"   Contact Value: {contact_value} (original: {seen[key]['original_value']})")
                print(f"   First occurrence: row {seen[key]['row_num']}, id={seen[key]['id']}")
                print(f"   Duplicate: row {i}, id={row['id']}")
                if contact_value != seen[key]['original_value']:
                    print(f"   ⚠️  Case mismatch detected!")
            else:
                seen[key] = {'row_num': i, 'id': row['id'], 'original_value': contact_value}

    print(f"\n" + "=" * 80)
    print(f"SUMMARY: Found {len(duplicates)} duplicate contact entries")
    print("=" * 80)

    return duplicates

def check_attendance_duplicates():
    """Check for duplicate attendance based on (person_id, event_id)."""
    print("\n" + "=" * 80)
    print("CHECKING ATTENDANCE.CSV FOR DUPLICATES")
    print("=" * 80)

    seen = {}  # key: (person_id, event_id), value: row_number
    duplicates = []

    with open('final/attendance.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            key = (row['person_id'], row['event_id'])

            if key in seen:
                duplicates.append({
                    'key': key,
                    'first_row': seen[key],
                    'duplicate_row': i
                })
                print(f"\n⚠️  DUPLICATE FOUND:")
                print(f"   Person ID: {row['person_id']}")
                print(f"   Event ID: {row['event_id']}")
                print(f"   First occurrence: row {seen[key]}")
                print(f"   Duplicate: row {i}")
            else:
                seen[key] = i

    print(f"\n" + "=" * 80)
    print(f"SUMMARY: Found {len(duplicates)} duplicate attendance entries")
    print("=" * 80)

    return duplicates

def check_people_duplicates():
    """Check for duplicate people based on id."""
    print("\n" + "=" * 80)
    print("CHECKING PEOPLE.CSV FOR DUPLICATES")
    print("=" * 80)

    seen = {}  # key: person_id, value: row_number
    duplicates = []

    with open('final/people.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            person_id = row['id']

            if person_id in seen:
                duplicates.append({
                    'person_id': person_id,
                    'first_row': seen[person_id],
                    'duplicate_row': i
                })
                print(f"\n⚠️  DUPLICATE FOUND:")
                print(f"   Person ID: {person_id}")
                print(f"   First occurrence: row {seen[person_id]}")
                print(f"   Duplicate: row {i}")
            else:
                seen[person_id] = i

    print(f"\n" + "=" * 80)
    print(f"SUMMARY: Found {len(duplicates)} duplicate people entries")
    print("=" * 80)

    return duplicates

if __name__ == '__main__':
    contacts_dups = check_contacts_duplicates()
    attendance_dups = check_attendance_duplicates()
    people_dups = check_people_duplicates()

    print("\n" + "=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)
    print(f"Contacts duplicates: {len(contacts_dups)}")
    print(f"Attendance duplicates: {len(attendance_dups)}")
    print(f"People duplicates: {len(people_dups)}")
    print(f"Total issues: {len(contacts_dups) + len(attendance_dups) + len(people_dups)}")
