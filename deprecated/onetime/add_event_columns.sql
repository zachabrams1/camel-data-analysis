-- Add speaker and core event columns to Events table

-- Speaker columns
ALTER TABLE Events
ADD COLUMN IF NOT EXISTS speaker_name VARCHAR(100),
ADD COLUMN IF NOT EXISTS speaker_org VARCHAR(100),
ADD COLUMN IF NOT EXISTS speaker_bio_short TEXT,
ADD COLUMN IF NOT EXISTS speaker_headshot_url TEXT,
ADD COLUMN IF NOT EXISTS speaker_links TEXT;

-- Core columns
ALTER TABLE Events
ADD COLUMN IF NOT EXISTS rsvp_link TEXT,
ADD COLUMN IF NOT EXISTS attendance INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS photos_dropbox_url TEXT;

-- Update existing events with actual attendance counts from checked-in attendees
UPDATE Events
SET attendance = (
    SELECT COUNT(*)
    FROM Attendance
    WHERE Attendance.event_id = Events.id
    AND Attendance.checked_in = TRUE
)
WHERE attendance = 0 OR attendance IS NULL;

-- Add comments to document the columns
COMMENT ON COLUMN Events.speaker_name IS 'Name of the event speaker';
COMMENT ON COLUMN Events.speaker_org IS 'Organization/affiliation of the speaker';
COMMENT ON COLUMN Events.speaker_bio_short IS 'Short biography of the speaker (2-3 sentences)';
COMMENT ON COLUMN Events.speaker_headshot_url IS 'URL to speaker headshot image';
COMMENT ON COLUMN Events.speaker_links IS 'Speaker links (LinkedIn, org site, etc.)';
COMMENT ON COLUMN Events.rsvp_link IS 'RSVP link for the event';
COMMENT ON COLUMN Events.attendance IS 'Total number of attendees who checked in';
COMMENT ON COLUMN Events.photos_dropbox_url IS 'Dropbox URL for event photos';
