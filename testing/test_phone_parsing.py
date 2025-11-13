#!/usr/bin/env python3
"""Test how pandas is reading phone numbers"""

import pandas as pd

# Test with the actual CSV
df = pd.read_csv('Raw/DRAFT_KINGS_x_CAMEL.csv')

print("=== PHONE NUMBER COLUMN ANALYSIS ===\n")

print(f"Column name: 'Phone Number'")
print(f"Data type: {df['Phone Number'].dtype}")
print()

print("First 10 phone numbers:")
for idx in range(min(10, len(df))):
    raw_value = df.iloc[idx]['Phone Number']
    print(f"  Row {idx}: {raw_value} (type: {type(raw_value).__name__})")
    if pd.notna(raw_value):
        str_value = str(raw_value)
        print(f"           → str() = '{str_value}'")
print()

# Check what percentage are stored as floats
print("Value types breakdown:")
value_types = df['Phone Number'].apply(lambda x: type(x).__name__ if pd.notna(x) else 'NaN')
print(value_types.value_counts())

# Test the cleaning logic
print("\n=== TESTING CLEANING LOGIC ===\n")
for idx in [0, 1, 2, 5]:
    raw_phone = df.iloc[idx]['Phone Number']
    phone_clean = str(raw_phone).strip() if pd.notna(raw_phone) else ""
    print(f"Row {idx}: raw={raw_phone} → clean='{phone_clean}'")
