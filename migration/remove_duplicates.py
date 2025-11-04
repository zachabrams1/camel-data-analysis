#!/usr/bin/env python3
"""Remove duplicate entries from contacts.csv while preserving the first occurrence."""

import csv
import shutil
from datetime import datetime

def remove_contacts_duplicates():
    """Remove duplicate contacts, handling case-insensitive emails. Keeps lowercase versions."""

    # Create backup
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f'final/contacts.csv.bak.{timestamp}'
    shutil.copy('final/contacts.csv', backup_file)
    print(f"✓ Created backup: {backup_file}")

    seen = {}  # Maps normalized key to row index in kept_rows
    kept_rows = []
    duplicate_count = 0

    with open('final/contacts.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            person_id = row['person_id']
            contact_type = row['contact_type']
            contact_value = row['contact_value']

            # Create case-insensitive key for emails (per CLAUDE.md line 139)
            if 'email' in contact_type.lower():
                normalized_key = (person_id, contact_type, contact_value.lower())
            else:
                normalized_key = (person_id, contact_type, contact_value)

            if normalized_key in seen:
                # Duplicate found
                existing_idx = seen[normalized_key]
                existing_row = kept_rows[existing_idx]

                # For emails, prefer lowercase version
                if 'email' in contact_type.lower():
                    if contact_value.islower() and not existing_row['contact_value'].islower():
                        # Current is lowercase, existing is not - replace existing
                        print(f"Replacing '{existing_row['contact_value']}' (id={existing_row['id']}) "
                              f"with lowercase '{contact_value}' (id={row['id']}) for person {person_id}")
                        kept_rows[existing_idx] = row
                    else:
                        # Keep existing
                        print(f"Removing duplicate '{contact_value}' (id={row['id']}) for person {person_id}")
                else:
                    print(f"Removing duplicate: person_id={person_id}, "
                          f"type={contact_type}, value={contact_value}, id={row['id']}")

                duplicate_count += 1
            else:
                # New entry
                seen[normalized_key] = len(kept_rows)
                kept_rows.append(row)

    # Write cleaned data
    with open('final/contacts.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)

    print(f"\n" + "=" * 80)
    print(f"✓ Removed {duplicate_count} duplicate contact entries")
    print(f"✓ Kept {len(kept_rows)} unique contact entries")
    print(f"✓ Updated final/contacts.csv")
    print("=" * 80)

    return duplicate_count

if __name__ == '__main__':
    print("=" * 80)
    print("REMOVING DUPLICATE CONTACTS")
    print("=" * 80)
    print()

    removed = remove_contacts_duplicates()

    print(f"\n✅ Done! Removed {removed} duplicates from contacts.csv")
    print(f"   The original file has been backed up with a timestamp.")
