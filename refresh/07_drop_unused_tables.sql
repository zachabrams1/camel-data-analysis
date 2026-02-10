-- =====================================================================
-- Step 7: Drop Unused Tables
-- =====================================================================
-- This script drops tables that are no longer needed:
-- - expenses
-- - opportunities
-- - subscribers
-- - verification_codes
--
-- These tables are not part of the core event/attendee tracking system.
-- =====================================================================

BEGIN;

DROP TABLE IF EXISTS expenses CASCADE;
DROP TABLE IF EXISTS opportunities CASCADE;
DROP TABLE IF EXISTS subscribers CASCADE;
DROP TABLE IF EXISTS verification_codes CASCADE;

COMMIT;
