# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is an event analytics and attendee management system for a student organization. The system tracks people, events, attendance, RSVPs, and contact information using **CSV files as the data store** (not an actual SQL database), with tools for data import and analysis.

## Data Model

The system follows a relational structure defined in `dbdesign.txt` but implemented as CSV files:

- **People** (`final/people.csv`): Core person records with demographics (name, gender, class year, school, is_jewish)
- **Contacts** (`final/contacts.csv`): Email addresses and phone numbers linked to people (supports multiple contacts per person)
- **Events** (`final/events.csv`): Event details (name, category, location, start_datetime, description)
- **Attendance** (`final/attendance.csv`): Links people to events with RSVP/attendance status and invite tracking
- **InviteTokens** (`final/invite_tokens.csv`): Tracks invitation sources (personal outreach, mailing list, club collaboration)

**Key relationships** (maintained through IDs in CSV files):
- Each attendance record references a person_id, event_id, and invite_token_id
- The `is_first_event` flag in Attendance tracks first-time attendees
- Contacts link to people via person_id and specify type ("school email", "personal email", or "phone")

## Data Files Structure

The repository uses CSV files in the `final/` directory as the canonical data store:
- `final/people.csv` - Person records
- `final/contacts.csv` - Contact information
- `final/events.csv` - Event details
- `final/attendance.csv` - Attendance/RSVP records
- `final/invite_tokens.csv` - Invitation tracking tokens
- `final/mailing_list.csv` - Exported mailing list (generated)

Raw data imports are stored in `Raw/` directory (e.g., `Raw/BCV_Event.csv`).

## Key Scripts

### Event Analysis (`event_analysis.py`)

**Purpose**: Generate comprehensive analytics on event performance, retention, and attendee patterns.

**Usage**:
```bash
python event_analysis.py --attendance final/attendance.csv --events final/events.csv --people final/people.csv --outdir analysis_outputs
```

**What it does**:
1. Creates a master dataset by merging attendance, events, and people data
2. Tracks both first-time attendees AND first-time RSVPs who didn't attend
3. Analyzes retention patterns (who returns after each event)
4. Generates visualizations for:
   - Retention by event (RSVPs → attendance → return patterns)
   - New members by event and category (including first RSVP tracking)
   - Party-specific funnel analysis
   - RSVP-to-attendance conversion (with and without parties)
   - Summary statistics

**Key analysis concepts**:
- `is_first_attendance`: Person's first event where they actually checked in
- `is_first_rsvp`: Person's first RSVP (may not have attended)
- Retention tracking: Counts how many attendees/RSVPers returned to later events
- Party events are identified by keywords: 'launch', 'sababa nights', 'bsmnt', 'fall 2025'

### Data Import Notebook (`insert_into_db.ipynb`)

**Purpose**: Import new event data from CSV files into the database.

**Workflow**:
1. **Cell 1-2**: Create new event record interactively
2. **Cell 3-5**: Fuzzy name matching and person ID resolution functions
3. **Cell 6**: Data normalization for gender, school, and class year
4. **Cell 7**: Main import logic (matches people, creates contacts, tracks attendance)
5. **Cell 8**: Save updated CSVs
6. **Cell 11**: Export mailing list

**Key features**:
- Fuzzy name matching with auto-accept/manual-review logic
- Email and phone-based person matching (prioritizes school email)
- Auto-detects school from email domains (@harvard.edu, @mit.edu, etc.)
- Handles invite token tracking for attribution
- Creates new person records when no match found
- Updates names to longer version when one is substring of another

**School detection priority**: school email > general email > explicit school field

**Running the import**:
```bash
jupyter notebook insert_into_db.ipynb
```
Then run cells sequentially. The script will prompt for user input when:
- Creating a new event (category, name, datetime, location, description)
- Resolving ambiguous name matches
- Confirming fuzzy name matches

## Data Normalization Rules

**Gender normalization**:
- Input: "f"/"female"/"woman"/"girl" → "F"
- Input: "m"/"male"/"man"/"boy" → "M"
- Everything else → NA

**School normalization**:
- Harvard undergraduate: "Harvard"
- Harvard graduate schools (HBS, HMS, HSPH): "Other"
- MIT: "MIT" (same domain for undergrad/grad)
- Other Boston schools: "Boston University", "Northeastern", "Tufts", "Wellesley", "Brandeis", "Emerson", "Suffolk", "Berklee", "Simmons"
- Prioritizes school email over general email

**Class year normalization**:
- Fixed mapping for 2025-26 academic year: Freshman=2029, Sophomore=2028, Junior=2027, Senior=2026
- Accepts: "2029", "'27", "Class of 2029", "Freshman", etc.
- Returns 4-digit year or NA

## Development Workflow

**To add a new event and import attendees**:
1. Place raw event CSV in `Raw/` directory
2. Run `insert_into_db.ipynb` cells 1-2 to create event
3. Update cell 7 configuration variables to match CSV column names
4. Run cells 3-8 to import attendees
5. Run cell 11 to export updated mailing list

**To generate analytics**:
```bash
python event_analysis.py
```
Results will be in `analysis_outputs/` directory with PNG visualizations and CSV summaries.

**To test with specific date ranges or events**:
Modify the master dataset filtering in `event_analysis.py` after line 59.

## Important Notes

- All CSV files in `final/` are the source of truth
- Name matching is case-insensitive and uses fuzzy logic (80% threshold)
- First-time tracking distinguishes between "first RSVP" and "first attendance"
- Party events use different analytics (identified by event name keywords)
- The system handles duplicate prevention via unique constraints (person_id, event_id) in attendance
- Contact emails are stored in lowercase for consistent matching
