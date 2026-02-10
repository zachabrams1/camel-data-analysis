#!/usr/bin/env python3
"""
Generate an email list for a specific event.

This script connects to the Railway database, allows the user to select an event,
and exports a CSV file containing all attendees who checked in (not just RSVPed)
with their contact information and lifetime event attendance count.

Output columns: first_name, last_name, school, contact_value, event_count
"""

import os
import sys
import re
import csv
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT', 58300),
    'database': os.getenv('PGDATABASE', 'postgres'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD'),
}


def get_db_connection():
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def fetch_all_events(conn):
    """
    Fetch all events from the database, ordered by start_datetime (most recent first).

    Returns:
        list: List of event dictionaries with id, event_name, start_datetime, category, attendance
    """
    query = """
    SELECT
        id,
        event_name,
        start_datetime,
        category,
        attendance,
        location
    FROM Events
    ORDER BY start_datetime DESC
    """

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            events = cur.fetchall()
            return events
    except psycopg2.Error as e:
        print(f"Error fetching events: {e}")
        sys.exit(1)


def display_event_menu(events):
    """
    Display an interactive menu of events and prompt user to select one.

    Args:
        events (list): List of event dictionaries

    Returns:
        dict: Selected event dictionary
    """
    if not events:
        print("No events found in the database.")
        sys.exit(1)

    print("\n" + "="*80)
    print("SELECT AN EVENT")
    print("="*80)

    for idx, event in enumerate(events, 1):
        # Format the datetime
        event_date = event['start_datetime'].strftime('%Y-%m-%d %H:%M') if event['start_datetime'] else 'No date'
        attendance = event['attendance'] if event['attendance'] else 0

        print(f"\n{idx}. {event['event_name']}")
        print(f"   Date: {event_date} | Category: {event['category']} | Attendance: {attendance}")
        if event['location']:
            print(f"   Location: {event['location']}")

    print("\n" + "="*80)

    # Prompt for selection
    while True:
        try:
            selection = input(f"\nEnter event number (1-{len(events)}) or 'q' to quit: ").strip()

            if selection.lower() == 'q':
                print("Exiting...")
                sys.exit(0)

            selection_num = int(selection)

            if 1 <= selection_num <= len(events):
                selected_event = events[selection_num - 1]
                print(f"\nSelected: {selected_event['event_name']}")
                return selected_event
            else:
                print(f"Please enter a number between 1 and {len(events)}")
        except ValueError:
            print("Invalid input. Please enter a number.")


def fetch_attendee_data(conn, event_id):
    """
    Fetch attendee data for the specified event.

    Only includes people who actually checked in (not just RSVPed).
    Includes lifetime event_count (total events attended across all time).
    Prefers school email over personal email for contact_value.

    Args:
        conn: Database connection
        event_id (int): Event ID to fetch attendees for

    Returns:
        list: List of dictionaries with keys: first_name, last_name, school, contact_value, event_count
    """
    query = """
    WITH event_attendance_counts AS (
        -- Calculate lifetime attendance count for each person
        SELECT
            person_id,
            COUNT(*) as event_count
        FROM Attendance
        WHERE checked_in = TRUE
        GROUP BY person_id
    ),
    preferred_contacts AS (
        -- Get preferred email (school email first, personal email as fallback)
        SELECT
            person_id,
            COALESCE(
                MAX(CASE WHEN contact_type = 'school email' THEN contact_value END),
                MAX(CASE WHEN contact_type = 'personal email' THEN contact_value END)
            ) as contact_value
        FROM Contacts
        WHERE contact_type IN ('school email', 'personal email')
        GROUP BY person_id
    )
    SELECT
        p.first_name,
        p.last_name,
        p.school,
        c.contact_value,
        COALESCE(e.event_count, 0) as event_count
    FROM Attendance a
    JOIN People p ON a.person_id = p.id
    LEFT JOIN event_attendance_counts e ON p.id = e.person_id
    LEFT JOIN preferred_contacts c ON p.id = c.person_id
    WHERE a.event_id = %s
      AND a.checked_in = TRUE
    ORDER BY p.last_name, p.first_name
    """

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (event_id,))
            attendees = cur.fetchall()
            return attendees
    except psycopg2.Error as e:
        print(f"Error fetching attendee data: {e}")
        sys.exit(1)


def sanitize_filename(filename):
    """
    Sanitize a filename by removing special characters and replacing spaces with underscores.

    Args:
        filename (str): Original filename

    Returns:
        str: Sanitized filename
    """
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    # Remove special characters (keep alphanumeric, underscores, hyphens)
    filename = re.sub(r'[^a-zA-Z0-9_-]', '', filename)
    # Collapse multiple underscores into one
    filename = re.sub(r'_+', '_', filename)
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    return filename


def export_to_csv(attendees, event_name, output_dir):
    """
    Export attendee data to a CSV file.

    Args:
        attendees (list): List of attendee dictionaries
        event_name (str): Name of the event (used for filename)
        output_dir (str): Directory to save the CSV file

    Returns:
        str: Path to the created CSV file
    """
    # Sanitize event name for filename
    sanitized_name = sanitize_filename(event_name)
    filename = f"{sanitized_name}_attendees.csv"
    filepath = os.path.join(output_dir, filename)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Define CSV columns
    fieldnames = ['first_name', 'last_name', 'school', 'contact_value', 'event_count']

    # Track attendees without email
    no_email_count = 0
    rows_to_write = []

    for attendee in attendees:
        if not attendee['contact_value']:
            no_email_count += 1
            # Skip attendees without email addresses
            continue

        rows_to_write.append({
            'first_name': attendee['first_name'] or '',
            'last_name': attendee['last_name'] or '',
            'school': attendee['school'] or '',
            'contact_value': attendee['contact_value'],
            'event_count': attendee['event_count']
        })

    # Write to CSV
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_to_write)

        print(f"\n{'='*80}")
        print("EXPORT SUCCESSFUL")
        print(f"{'='*80}")
        print(f"File saved to: {filepath}")
        print(f"Total attendees exported: {len(rows_to_write)}")

        if no_email_count > 0:
            print(f"⚠️  Skipped {no_email_count} attendee(s) without email addresses")

        print(f"{'='*80}\n")

        return filepath

    except IOError as e:
        print(f"Error writing CSV file: {e}")
        sys.exit(1)


def main():
    """Main program execution."""
    print("\n" + "="*80)
    print("EVENT ATTENDEE EMAIL LIST GENERATOR")
    print("="*80)
    print("This program generates an email list for attendees of a specific event.")
    print("Output includes: first_name, last_name, school, contact_value, event_count")
    print("="*80)

    # Connect to database
    print("\nConnecting to database...")
    conn = get_db_connection()
    print("✓ Connected successfully")

    try:
        # Fetch all events
        print("\nFetching events from database...")
        events = fetch_all_events(conn)
        print(f"✓ Found {len(events)} events")

        # Display menu and get user selection
        selected_event = display_event_menu(events)

        # Fetch attendee data for selected event
        print(f"\nFetching attendee data for '{selected_event['event_name']}'...")
        attendees = fetch_attendee_data(conn, selected_event['id'])
        print(f"✓ Found {len(attendees)} attendees who checked in")

        if not attendees:
            print("\nNo attendees found for this event.")
            print("Make sure attendees have 'checked_in = TRUE' in the database.")
            sys.exit(0)

        # Export to CSV
        output_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '.'  # Current directory (/master/event_mail/)
        )
        export_to_csv(attendees, selected_event['event_name'], output_dir)

    finally:
        # Close database connection
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
