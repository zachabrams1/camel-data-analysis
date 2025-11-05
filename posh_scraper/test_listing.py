#!/usr/bin/env python3
"""
Quick test to verify event listing works

SETUP INSTRUCTIONS:
1. Close all Chrome windows
2. Open Terminal and run:
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222
3. Log in to Posh VIP in that Chrome window
4. Run this script (Chrome will stay open, script will control it)
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

EVENTS_URL = "https://posh.vip/admin/groups/679a73a11aa0bc30f065f1d7/overview/all-events"

def setup_driver():
    """Connect to existing Chrome instance via remote debugging"""
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "localhost:9222")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

def get_event_list(driver):
    """Get list of events from the events page"""
    wait = WebDriverWait(driver, 10)
    # Wait for the EventTable-rows container to load
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.EventTable-rows")))

    # Find all event links (a tags within EventTable-rows)
    events = driver.find_elements(By.CSS_SELECTOR, "div.EventTable-rows a")
    return events

def get_event_name(event_element):
    """Extract event name from event element"""
    try:
        name_element = event_element.find_element(By.CSS_SELECTOR, "h5.hover\\:underline")
        return name_element.text.strip()
    except Exception as e:
        print(f"Warning: Could not extract event name - {e}")
        return "Unknown Event"

def main():
    driver = None
    try:
        print("Testing Posh VIP Event Listing...")
        print(f"URL: {EVENTS_URL}\n")

        # Set up browser
        driver = setup_driver()
        driver.get(EVENTS_URL)

        print("Navigated to events page. Waiting for page to load...")
        time.sleep(3)

        # Get all events
        print("Attempting to find events...\n")
        events = get_event_list(driver)
        print(f"✓ Found {len(events)} events!\n")

        # List all events
        print("Events found:")
        print("-" * 60)
        for i, event in enumerate(events):
            event_name = get_event_name(event)
            print(f"{i+1}. {event_name}")
        print("-" * 60)

        print("\n✓ Test successful! Event listing is working.")

    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            input("\nPress Enter to close browser...")
            driver.quit()
        print("Done!")

if __name__ == "__main__":
    main()
