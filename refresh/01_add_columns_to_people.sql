-- Migration: Add contact and event count columns to people table
-- Run this FIRST before executing the migration scripts
--
-- This adds columns that currently exist in contacts, mailinglist, and allmailing tables
-- to consolidate all person data into a single people table.

-- Add contact information columns (from contacts table)
ALTER TABLE people ADD COLUMN IF NOT EXISTS school_email VARCHAR(100);
ALTER TABLE people ADD COLUMN IF NOT EXISTS personal_email VARCHAR(100);
ALTER TABLE people ADD COLUMN IF NOT EXISTS preferred_email VARCHAR(100);
ALTER TABLE people ADD COLUMN IF NOT EXISTS phone_number VARCHAR(15);

-- Add event count columns (from mailinglist table)
ALTER TABLE people ADD COLUMN IF NOT EXISTS event_attendance_count INTEGER DEFAULT 0;
ALTER TABLE people ADD COLUMN IF NOT EXISTS event_rsvp_count INTEGER DEFAULT 0;

-- Create indexes for email lookups (improves migration script performance)
CREATE INDEX IF NOT EXISTS idx_people_school_email ON people(school_email);
CREATE INDEX IF NOT EXISTS idx_people_personal_email ON people(personal_email);
CREATE INDEX IF NOT EXISTS idx_people_preferred_email ON people(preferred_email);

-- Create index for name lookups
CREATE INDEX IF NOT EXISTS idx_people_name ON people(first_name, last_name);

COMMIT;

-- Verification: Check that columns were added
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'people'
    AND column_name IN (
        'school_email',
        'personal_email',
        'preferred_email',
        'phone_number',
        'event_attendance_count',
        'event_rsvp_count'
    )
ORDER BY column_name;
