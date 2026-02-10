#!/usr/bin/env python3
"""
Posh VIP Event Report Downloader

This script automates downloading event reports from Posh VIP.
It will automatically launch Chrome with remote debugging if not already running.

USAGE:
Simply run: python download_event.py

The script will:
- Automatically launch Chrome with remote debugging (if not already running)
- Prompt you to log in to Posh VIP (if needed)
- Show you a list of events to choose from
- Download the selected event report to the Raw/ folder

Workflow:
1. Navigate to events page
2. Show events one by one (top to bottom)
3. User confirms which event to download
4. Download the event report
5. Rename and move to Raw/ folder
"""

import os
import time
import subprocess
import socket
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import glob

# Configuration
EVENTS_URL = "https://posh.vip/admin/groups/679a73a11aa0bc30f065f1d7/overview/all-events"
DOWNLOAD_DIR = str(Path.home() / "Downloads")  # Default Chrome download location
RAW_DIR = Path(__file__).parent.parent / "Raw"  # ../Raw relative to this script
CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DEBUG_PORT = 9222

def is_chrome_debugging_running():
    """Check if Chrome is running with remote debugging on the specified port"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(('localhost', DEBUG_PORT))
        sock.close()
        return True
    except (socket.error, ConnectionRefusedError):
        return False

def launch_chrome_with_debugging():
    """Launch Chrome with remote debugging enabled"""
    print(f"Launching Chrome with remote debugging on port {DEBUG_PORT}...")

    # Use a dedicated user data directory for debugging to avoid conflicts
    user_data_dir = Path.home() / ".chrome-debug-profile"
    user_data_dir.mkdir(exist_ok=True)

    # Launch Chrome in a separate process
    subprocess.Popen(
        [
            CHROME_PATH,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check"
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Wait for Chrome to start and the debugging port to become available
    max_wait = 10  # seconds
    start_time = time.time()
    while time.time() - start_time < max_wait:
        if is_chrome_debugging_running():
            print("✓ Chrome is ready!")
            time.sleep(1)  # Give it one more second to fully initialize
            return True
        time.sleep(0.5)

    raise RuntimeError(f"Chrome failed to start with remote debugging after {max_wait} seconds")

def ensure_chrome_ready():
    """Ensure Chrome is running with remote debugging, launch if needed"""
    if is_chrome_debugging_running():
        print("✓ Chrome with remote debugging is already running")
        return

    print("Chrome with remote debugging is not running")
    launch_chrome_with_debugging()

    # Prompt user to log in if needed
    print("\n" + "="*60)
    print("IMPORTANT: If you're not already logged in to Posh VIP,")
    print("please log in now in the Chrome window that just opened.")
    print("="*60)
    input("\nPress Enter when you're logged in and ready to continue...")

def setup_driver():
    """Connect to existing Chrome instance via remote debugging"""
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "localhost:9222")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_event_list(driver):
    """
    Get list of events from the events page

    Structure: div.EventTable > div.EventTable-rows > a (event links)
    Returns: list of event <a> elements
    """
    wait = WebDriverWait(driver, 10)
    # Wait for the EventTable-rows container to load
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.EventTable-rows")))

    # Find all event links (a tags within EventTable-rows)
    events = driver.find_elements(By.CSS_SELECTOR, "div.EventTable-rows a")
    return events

def get_event_name(event_element):
    """
    Extract event name from event element

    Within each <a> tag, find <h5 class="hover:underline"> with event title
    """
    try:
        name_element = event_element.find_element(By.CSS_SELECTOR, "h5.hover\\:underline")
        return name_element.text.strip()
    except Exception as e:
        print(f"Warning: Could not extract event name - {e}")
        return "Unknown Event"

def click_event(event_element):
    """Click on an event to navigate to its page"""
    event_element.click()
    time.sleep(2)  # Wait for page to load

def navigate_to_settings(driver):
    """
    Click on Settings in the left menu bar

    Settings is an <a> tag containing an <img> with id="settings"
    """
    wait = WebDriverWait(driver, 10)
    # Find the <a> tag that contains an <img> with id="settings"
    settings_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a img[id='settings']")))
    # Click the parent <a> tag, not the img
    settings_link.find_element(By.XPATH, "..").click()
    time.sleep(2)

def export_event_report(driver):
    """
    Export the event report:
    1. Click "Export Event Report" button (class="poshBtn", contains "event" in text)
    2. Click "Select All" checkbox (label.CheckBoxList-Option with text "Select All")
    3. Click "By checking this box..." checkbox (input#marketing-agreement-checkbox)
    4. Click "Export Report" button (class="poshBtn gold", text "Export Report")
    """
    wait = WebDriverWait(driver, 10)

    # 1. Click "Export Event Report" button
    # Use XPath to find button with class "poshBtn" that contains "event" in text
    export_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//button[contains(@class, 'poshBtn') and contains(translate(., 'EVENT', 'event'), 'event')]")
    ))
    export_button.click()
    time.sleep(1)

    # 2. Click "Select All" checkbox
    # Find label with class "CheckBoxList-Option" that has text "Select All"
    # Note: Clicking the label should work; if not, try clicking the svg or input inside
    select_all = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//label[contains(@class, 'CheckBoxList-Option') and contains(., 'Select All')]")
    ))
    select_all.click()
    time.sleep(0.5)

    # 3. Click "By checking this box..." marketing agreement checkbox
    # Click the label that contains the checkbox (the input itself is often hidden)
    agree_checkbox = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//label[contains(@class, 'CheckBoxList-Option')]//input[@id='marketing-agreement-checkbox']/..")
    ))
    agree_checkbox.click()
    time.sleep(0.5)

    # 4. Click final "Export Report" button
    # Find button with classes "poshBtn gold" and text "Export Report"
    final_export = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//button[contains(@class, 'poshBtn') and contains(@class, 'gold') and contains(., 'Export Report')]")
    ))
    final_export.click()

def get_event_title_from_page(driver):
    """
    Get the event title from the current event page
    (Used for renaming the downloaded file)

    We'll use the title we already captured when clicking the event
    This function is kept for potential future use
    """
    # For now, we'll pass the title from the previous step
    # If needed later, we can add a selector here
    pass

def wait_for_download(download_dir, timeout=30):
    """Wait for the most recent .csv file to finish downloading"""
    end_time = time.time() + timeout

    while time.time() < end_time:
        # Look for .csv files (not .crdownload - Chrome's temp download extension)
        csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
        if csv_files:
            # Get the most recent CSV file
            latest_file = max(csv_files, key=os.path.getctime)
            # Make sure it's not still downloading
            if not os.path.exists(latest_file + ".crdownload"):
                return latest_file
        time.sleep(1)

    raise TimeoutError("Download did not complete within timeout")

def sanitize_filename(filename):
    """Remove characters that aren't safe for filenames"""
    # Replace spaces and special characters
    safe_chars = '-_.'
    return "".join(c if c.isalnum() or c in safe_chars else '_' for c in filename)

def main():
    driver = None
    try:
        print("Starting Posh VIP Event Downloader...")
        print(f"Target URL: {EVENTS_URL}")
        print(f"Output directory: {RAW_DIR}\n")

        # Ensure Raw directory exists
        RAW_DIR.mkdir(exist_ok=True)

        # Ensure Chrome is running with remote debugging
        ensure_chrome_ready()

        # Set up browser
        driver = setup_driver()
        driver.get(EVENTS_URL)

        print("Navigated to events page. Waiting for page to load...")
        time.sleep(3)  # Give page time to load

        # Get all events
        events = get_event_list(driver)
        print(f"Found {len(events)} events on the page.\n")

        # Iterate through events and ask user
        event_selected = False
        for i, event in enumerate(events):
            event_name = get_event_name(event)
            print(f"Event {i+1}: {event_name}")

            response = input("Do you want to scrape this event? (yes/no): ").strip().lower()

            if response in ['yes', 'y']:
                print(f"\n✓ Selected: {event_name}")
                event_selected = True

                # Store event title for filename (we already have it)
                event_title = event_name

                # Click on event
                print("Navigating to event page...")
                click_event(event)
                print(f"Event title: {event_title}")

                # Navigate to settings
                print("Navigating to settings...")
                navigate_to_settings(driver)
                time.sleep(2)

                # Export report
                print("Exporting event report...")
                export_event_report(driver)

                # Wait for download
                print("Waiting for download to complete...")
                downloaded_file = wait_for_download(DOWNLOAD_DIR)
                print(f"Downloaded: {downloaded_file}")

                # Rename and move file
                safe_title = sanitize_filename(event_title)
                new_filename = f"{safe_title}.csv"
                new_path = RAW_DIR / new_filename

                os.rename(downloaded_file, new_path)
                print(f"\n✓ Success! File saved to: {new_path}")
                print(f"OUTPUT_FILE:{new_path}")  # Marker for orchestrator script

                break
            else:
                print("Skipping...\n")

        if not event_selected:
            print("No event was selected for download.")

    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Note: We intentionally don't close Chrome (driver.quit())
        # so it stays open with remote debugging for future runs
        if driver:
            print("\n✓ Disconnecting from browser (Chrome will stay open)...")
        print("Done!")

if __name__ == "__main__":
    main()
