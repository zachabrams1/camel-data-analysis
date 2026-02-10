# Database Consolidation Migration Scripts

This folder contains scripts to consolidate data from `contacts`, `mailinglist`, and `allmailing` tables into the `people` table, eliminating redundancy and establishing a single source of truth.

## Problem Being Solved

The current database has redundant data across multiple tables:
- **people** table: Core person records
- **contacts** table: Emails and phone numbers (linked by person_id)
- **mailinglist** table: Denormalized person data with contact info
- **allmailing** table: Email list (one row per email)

This redundancy causes data sync issues and mixups. The solution: consolidate everything into the `people` table.

## Migration Overview

This migration adds contact columns to the `people` table and transfers all data from the three redundant tables, ensuring no information is lost.

### New Columns Added to `people` Table:
- `school_email` VARCHAR(100)
- `personal_email` VARCHAR(100)
- `preferred_email` VARCHAR(100) - Computed as COALESCE(school_email, personal_email)
- `phone_number` VARCHAR(15)
- `event_attendance_count` INTEGER
- `event_rsvp_count` INTEGER

## Execution Steps

### Step 1: Backup Your Database

**CRITICAL:** Before running any migrations, create a backup:

```bash
# Using Railway CLI (if using Railway)
railway run pg_dump > backup_$(date +%Y%m%d_%H%M%S).sql

# Or using pg_dump directly
pg_dump -h your-host -U your-user -d railway > backup.sql
```

### Step 2: Run SQL Schema Migration

This adds the new columns to the `people` table:

```bash
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f 01_add_columns_to_people.sql
```

Or using Railway CLI:
```bash
railway run psql < 01_add_columns_to_people.sql
```

### Step 3: Run Python Migration Scripts (in order!)

Make sure your `.env` file has the database credentials:
```
PGHOST=your-host
PGPORT=5432
PGDATABASE=railway
PGUSER=postgres
PGPASSWORD=your-password
```

**Run with --dry-run first to preview changes:**

```bash
# Script 1: Migrate contacts table (run FIRST - most reliable, uses person_id)
python3 02_migrate_contacts_to_people.py --dry-run

# Script 2: Migrate mailinglist table (run SECOND - most complete records)
python3 03_migrate_mailinglist_to_people.py --dry-run

# Script 3: Migrate allmailing table (run THIRD - catches remaining orphans)
python3 04_migrate_allmailing_to_people.py --dry-run
```

**After reviewing dry-run output, run for real:**

```bash
python3 02_migrate_contacts_to_people.py
python3 03_migrate_mailinglist_to_people.py
python3 04_migrate_allmailing_to_people.py
```

### Step 4: Create Views and Drop Old Tables

After migrating all data to the `people` table, create SQL views to maintain backward compatibility and drop the old redundant tables:

```bash
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f 06_create_views_and_drop_tables.sql
```

Or using Railway CLI:
```bash
railway run psql < 06_create_views_and_drop_tables.sql
```

**What this does:**
- Creates `mailinglist` VIEW that queries the `people` table
- Creates `allmailing` VIEW that denormalizes emails from the `people` table
- Drops the old `contacts`, `mailinglist`, and `allmailing` tables

**After this step:**
- ✅ All data is in the `people` table (single source of truth)
- ✅ `mailinglist` and `allmailing` still work as views (backward compatible)
- ✅ Old redundant tables are removed

### Step 5: Verify Data Migration

Run verification queries to ensure all data was transferred:

```sql
-- Check that all emails from contacts are in people
SELECT COUNT(*) FROM contacts
WHERE contact_type IN ('school email', 'personal email');

SELECT COUNT(*) FROM people
WHERE school_email IS NOT NULL OR personal_email IS NOT NULL;

-- Check that all mailinglist records were migrated
SELECT COUNT(*) FROM mailinglist;
SELECT COUNT(*) FROM people;

-- Check that all allmailing emails are in people
SELECT COUNT(DISTINCT contact_value) FROM allmailing;
SELECT COUNT(*) FROM people
WHERE school_email IS NOT NULL OR personal_email IS NOT NULL;

-- Spot check: Find any emails in allmailing not in people
SELECT am.contact_value, am.first_name, am.last_name
FROM allmailing am
WHERE NOT EXISTS (
    SELECT 1 FROM people p
    WHERE LOWER(p.school_email) = LOWER(am.contact_value)
       OR LOWER(p.personal_email) = LOWER(am.contact_value)
)
LIMIT 10;
```

## What Each Script Does

### 02_migrate_contacts_to_people.py

**Purpose:** Transfer contact information from `contacts` table to `people` table

**Logic:**
- Uses `person_id` foreign key for reliable matching
- Maps contact_type to appropriate people column:
  - 'school email' → `people.school_email`
  - 'personal email' → `people.personal_email`
  - 'phone' → `people.phone_number`
- Handles conflicts by keeping existing data (warns but doesn't overwrite)
- Sets `preferred_email` = COALESCE(school_email, personal_email)

**Output:**
- Count of emails and phones added
- Warnings for any conflicts detected
- Summary of changes

### 03_migrate_mailinglist_to_people.py

**Purpose:** Merge demographic and contact data from `mailinglist` table into `people` table

**Logic:**
- Matches people by email (school_email or personal_email) or name
- Fills in missing demographic fields: gender, class_year, is_jewish, school
- Adds missing contact info: emails, phone, preferred_email
- Updates event attendance/RSVP counts
- Creates new person records if no match found
- Converts is_jewish from CHAR(1) 'J'/'N' to BOOLEAN

**Output:**
- Count of matched vs. created records
- List of fields updated for each person
- Summary statistics

### 04_migrate_allmailing_to_people.py

**Purpose:** Catch any remaining emails and people from `allmailing` table

**Logic:**
- Matches by email first, then by name
- Auto-detects school from email domain (@harvard.edu → harvard, @mit.edu → mit)
- Determines if email is school vs. personal based on domain
- Fills in missing names if person exists but unnamed
- Adds emails to appropriate field (school_email vs personal_email)
- Creates new person records for orphan emails (e.g., from deprecated newsletter signups)
- Sets preferred_email for anyone missing it

**Output:**
- Count of email matches vs. name matches
- Count of new people created
- List of names/emails/schools updated

## Expected Results

After running all migration scripts (steps 1-4), you should see:

1. **No data loss:** All emails, phone numbers, and person records from the three tables are now in `people`
2. **Filled gaps:** Missing demographic fields populated where data was available
3. **New people created:** Orphan emails (e.g., from newsletter signups) have new person records
4. **Preferred email set:** Everyone with an email has a preferred_email value

## Common Issues

### Issue: "column does not exist"
**Solution:** Make sure you ran the SQL migration first (Step 2)

### Issue: Many conflicts reported
**Cause:** Data inconsistencies between tables (e.g., same person has different emails in different tables)
**Solution:** Review conflict warnings and manually resolve if needed

### Issue: Fewer people in `people` table than sum of all source tables
**Explanation:** This is expected! Many records in the source tables are duplicates. The scripts deduplicate by matching on email/name.

### Issue: Script fails with psycopg2 error
**Solution:** Check that your `.env` file has correct database credentials and you have network access to the database

## Next Steps

After successfully running these migrations:

1. ✅ **SQL views created** - The `mailinglist` and `allmailing` tables are now views on the `people` table
2. ✅ **Old tables dropped** - Redundant `contacts`, `mailinglist`, and `allmailing` tables have been removed
3. **Update application scripts** - Modify any scripts that directly query the old table structures
4. **Monitor performance** - The new views should perform well, but monitor query performance and add indexes if needed

## Rollback Plan

If something goes wrong:

1. **Stop immediately** - Don't run subsequent scripts
2. **Restore from backup:**
   ```bash
   psql -h $PGHOST -U $PGUSER -d $PGDATABASE < backup.sql
   ```
3. **Report the issue** - Review script output to identify what went wrong
4. **Re-run with --dry-run** to debug before trying again

## Support

If you encounter issues:
1. Check the script output for error messages
2. Run verification queries to identify missing data
3. Review the --dry-run output to understand what changes will be made
4. Check that your database credentials are correct in `.env`
