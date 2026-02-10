-- =====================================================================
-- Step 6: Create Views and Drop Old Tables
-- =====================================================================
-- This script:
-- 1. Creates SQL views to replace mailinglist and allmailing tables
-- 2. Drops the old redundant tables (contacts, mailinglist, allmailing)
--
-- After this migration, all data lives in the people table, and the
-- mailinglist/allmailing views provide backward compatibility for
-- existing queries.
-- =====================================================================

BEGIN;

-- =====================================================================
-- Drop Old Tables First
-- =====================================================================
-- We must drop the existing tables before creating views with the same names

DROP TABLE IF EXISTS contacts CASCADE;
DROP TABLE IF EXISTS mailinglist CASCADE;
DROP TABLE IF EXISTS allmailing CASCADE;

-- =====================================================================
-- Create mailinglist VIEW
-- =====================================================================
-- This view replicates the mailinglist table structure by querying
-- the people table directly. All columns map 1-to-1.

CREATE VIEW mailinglist AS
SELECT
    id,
    first_name,
    last_name,
    gender,
    class_year,
    is_jewish,
    school,
    event_attendance_count,
    event_rsvp_count,
    school_email,
    personal_email,
    preferred_email,
    phone_number
FROM people;

-- =====================================================================
-- Create allmailing VIEW
-- =====================================================================
-- This view replicates the allmailing table structure, which is
-- denormalized: one row per email address.
--
-- For each person with a school_email or personal_email, we create
-- a separate row. If someone has both emails, they appear twice.

CREATE VIEW allmailing AS
SELECT
    ROW_NUMBER() OVER (ORDER BY id, email_type) AS id,
    first_name,
    last_name,
    school,
    email AS contact_value,
    event_attendance_count::NUMERIC(10,1) AS event_count
FROM (
    -- School emails
    SELECT
        id,
        first_name,
        last_name,
        school,
        school_email AS email,
        event_attendance_count,
        1 AS email_type
    FROM people
    WHERE school_email IS NOT NULL

    UNION ALL

    -- Personal emails
    SELECT
        id,
        first_name,
        last_name,
        school,
        personal_email AS email,
        event_attendance_count,
        2 AS email_type
    FROM people
    WHERE personal_email IS NOT NULL
) AS emails
ORDER BY id, email_type;

-- =====================================================================
-- Verification
-- =====================================================================
-- After running this script, verify that:
-- 1. SELECT * FROM mailinglist; returns data from people table
-- 2. SELECT * FROM allmailing; returns denormalized email rows
-- 3. Old tables (contacts, mailinglist, allmailing) no longer exist

COMMIT;
