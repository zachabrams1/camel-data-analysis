# Database Migration Guide

## Circular Dependency Issue - FIXED

### Problem Identified
The original `dbdesign.txt` had a **circular dependency**:
- `Attendance` table (defined first) referenced `InviteTokens(id)` via foreign key
- `InviteTokens` table was defined **after** `Attendance`

This caused SQL execution to fail because you can't reference a table that doesn't exist yet.

### Solution
The `dbdesign_fixed.sql` file resolves this by reordering the table creation:

**Correct order:**
1. `People` (no dependencies)
2. `Contacts` (depends on People)
3. `Events` (no dependencies)
4. **`InviteTokens`** (depends on Events) ← **MOVED HERE**
5. `Attendance` (depends on People, Events, InviteTokens)
6. `Expenses` (depends on Events)

## Prerequisites

1. **Install PostgreSQL** (if not already installed):
   ```bash
   # macOS with Homebrew
   brew install postgresql
   brew services start postgresql

   # Or use PostgreSQL.app
   # Download from: https://postgresapp.com/
   ```

2. **Install psycopg2**:
   ```bash
   pip install psycopg2-binary
   ```

3. **Create a database**:
   ```bash
   createdb event_analytics
   ```

## Usage

### Option 1: Using environment variables (recommended)
```bash
export PGDATABASE=event_analytics
export PGUSER=postgres
export PGPASSWORD=your_password  # if needed
export PGHOST=localhost
export PGPORT=5432

python csv_to_postgres.py
```

### Option 2: Using command-line arguments
```bash
python csv_to_postgres.py \
  --dbname event_analytics \
  --user postgres \
  --password your_password \
  --host localhost \
  --port 5432
```

### Option 3: Drop and recreate (WARNING: destroys existing data)
```bash
python csv_to_postgres.py --drop-existing
```

## What the Script Does

1. **Connects to PostgreSQL** using provided credentials
2. **Creates schema** from `dbdesign_fixed.sql` (with correct table order)
3. **Imports CSV data** in dependency order:
   - People → Contacts
   - Events → InviteTokens → Attendance
4. **Updates sequences** so new inserts get correct IDs
5. **Prints summary** of imported records

## Data Type Conversions

The script handles the following conversions from CSV:

- **Booleans**: `True/1/yes` → `TRUE`, `False/0/no` → `FALSE`
- **Integers**: Handles `2028.0` format, converts `NA`/empty to `NULL`
- **Timestamps**: Preserves ISO format strings
- **Gender**: Normalizes to `M`, `F`, `O`, or `NULL`
- **School**: Normalizes to `harvard`, `mit`, `other`, or `NULL`
- **Emails**: Stored in lowercase for consistency

## Verification

After import, verify the data:

```bash
psql event_analytics
```

```sql
-- Check row counts
SELECT COUNT(*) FROM People;
SELECT COUNT(*) FROM Contacts;
SELECT COUNT(*) FROM Events;
SELECT COUNT(*) FROM InviteTokens;
SELECT COUNT(*) FROM Attendance;

-- Verify relationships
SELECT p.first_name, p.last_name, e.event_name, a.checked_in
FROM Attendance a
JOIN People p ON a.person_id = p.id
JOIN Events e ON a.event_id = e.id
LIMIT 10;

-- Check for data integrity
SELECT * FROM People WHERE id IS NULL;
SELECT * FROM Attendance WHERE person_id NOT IN (SELECT id FROM People);
```

## Files Created

1. **`dbdesign_fixed.sql`** - Fixed database schema with correct table order
2. **`csv_to_postgres.py`** - Python migration script
3. **`DATABASE_MIGRATION.md`** - This documentation

## Troubleshooting

### "database does not exist"
```bash
createdb event_analytics
```

### "permission denied"
Make sure your PostgreSQL user has CREATE privileges, or connect as superuser:
```bash
python csv_to_postgres.py --user postgres
```

### "relation already exists"
Use `--drop-existing` flag to drop and recreate tables:
```bash
python csv_to_postgres.py --drop-existing
```

### CSV column name mismatches
The script expects these exact column names in the CSV files:
- `people.csv`: id, first_name, last_name, preferred_name, gender, class_year, is_jewish, school
- `contacts.csv`: id, person_id, type, value, is_verified
- `events.csv`: id, name, category, location, start_datetime, description
- `invite_tokens.csv`: id, event_id, value, category, description
- `attendance.csv`: id, person_id, event_id, rsvp, approved, checked_in, rsvp_datetime, is_first_event, invite_token_id

## Next Steps

After successful import:
1. Set up indexes for frequently queried columns
2. Configure database backups
3. Update your Python scripts to use PostgreSQL instead of CSV files
4. Consider adding `Expenses` table data if available
