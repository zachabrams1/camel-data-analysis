-- Fixed database design with circular dependency resolved
-- InviteTokens table moved before Attendance table

CREATE TABLE IF NOT EXISTS People (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    preferred_name VARCHAR(50),
    gender CHAR(1),
    class_year SMALLINT,
    is_jewish BOOLEAN,
    school VARCHAR(20),
    additional_info JSON,

    CHECK (gender IS NULL OR gender IN ('M', 'F', 'O')),
    CHECK (school IS NULL OR school IN ('harvard', 'mit', 'other'))
);

CREATE TABLE IF NOT EXISTS Contacts (
    id SERIAL PRIMARY KEY,
    person_id INTEGER NOT NULL,
    contact_type VARCHAR(20) NOT NULL,
    contact_value VARCHAR(100) NOT NULL,
    is_verified BOOLEAN NOT NULL,
    CONSTRAINT fk_contacts_person
        FOREIGN KEY (person_id)
        REFERENCES People(id)
        ON DELETE CASCADE,
    UNIQUE (person_id, contact_type, contact_value),
    CHECK (contact_type IN ('school email', 'personal email', 'phone'))
);

CREATE TABLE IF NOT EXISTS Events (
    id SERIAL PRIMARY KEY,
    event_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    location VARCHAR(100) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    description TEXT
);

-- MOVED BEFORE ATTENDANCE TO RESOLVE CIRCULAR DEPENDENCY
CREATE TABLE IF NOT EXISTS InviteTokens (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    category VARCHAR(50) NOT NULL,
    description TEXT,
    value VARCHAR(100),

    CHECK(category IN ('personal outreach', 'mailing list', 'club collaboration')),
    CONSTRAINT fk_invite_tokens_event
        FOREIGN KEY (event_id)
        REFERENCES Events(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Attendance (
    id SERIAL PRIMARY KEY,
    person_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    rsvp BOOLEAN NOT NULL,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    checked_in BOOLEAN NOT NULL DEFAULT FALSE,
    rsvp_datetime TIMESTAMP,
    is_first_event BOOLEAN NOT NULL DEFAULT FALSE,
    invite_token_id INTEGER NOT NULL,

    CONSTRAINT fk_attendance_person
        FOREIGN KEY (person_id)
        REFERENCES People(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_attendance_event
        FOREIGN KEY (event_id)
        REFERENCES Events(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_attendance_invite_token
        FOREIGN KEY (invite_token_id)
        REFERENCES InviteTokens(id)
        ON DELETE CASCADE,
    UNIQUE (person_id, event_id)
);

CREATE TABLE IF NOT EXISTS Expenses (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL,
    category VARCHAR(50) NOT NULL,
    amount NUMERIC NOT NULL,

    CONSTRAINT fk_expenses_event
        FOREIGN KEY (event_id)
        REFERENCES Events(id)
        ON DELETE CASCADE
);

-- Subscribers table for newsletter/marketing subscriptions
CREATE TABLE IF NOT EXISTS Subscribers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

-- Mailing list is a denormalized reporting table
-- Contains aggregated contact info and event participation metrics
CREATE TABLE IF NOT EXISTS MailingList (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    gender CHAR(1),
    class_year SMALLINT,
    is_jewish CHAR(1),
    school VARCHAR(20),
    event_attendance_count INTEGER DEFAULT 0,
    event_rsvp_count INTEGER DEFAULT 0,
    school_email VARCHAR(100),
    personal_email VARCHAR(100),
    preferred_email VARCHAR(100),
    phone_number VARCHAR(15),

    CHECK (gender IS NULL OR gender IN ('M', 'F', 'O')),
    CHECK (is_jewish IS NULL OR is_jewish IN ('J', 'N'))
);

-- AllMailing is a simplified mailing list for mass email distribution
-- Contains essential fields: name, school, email, and event attendance count
CREATE TABLE IF NOT EXISTS AllMailing (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    school VARCHAR(20),
    contact_value VARCHAR(100) NOT NULL,
    event_count NUMERIC DEFAULT 0
);
