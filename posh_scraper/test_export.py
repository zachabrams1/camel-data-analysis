#!/usr/bin/env python3
"""
Test script for export functionality selectors

SETUP INSTRUCTIONS:
1. Close all Chrome windows
2. Open Terminal and run:
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222
3. Log in to Posh VIP in that Chrome window
4. Navigate to a specific event page manually
5. Run this script to test the export flow

This script tests:
- Settings button click
- Export Event Report button
- Select All checkbox
- Marketing agreement checkbox
- Export Report button
"""

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """Connect to existing Chrome instance via remote debugging"""
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "localhost:9222")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def test_settings_button(driver):
    """Test clicking the settings button"""
    print("\n1. Testing Settings button...")
    try:
        wait = WebDriverWait(driver, 10)
        # Find the <a> tag that contains an <img> with id="settings"
        settings_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a img[id='settings']")))
        # Click the parent <a> tag, not the img
        settings_link.find_element(By.XPATH, "..").click()
        time.sleep(2)
        print("   ✓ Settings button clicked successfully")
        return True
    except Exception as e:
        print(f"   ✗ Failed to click settings button: {e}")
        return False

def test_export_flow(driver):
    """Test the complete export flow"""
    wait = WebDriverWait(driver, 10)

    # 1. Click "Export Event Report" button
    print("\n2. Testing 'Export Event Report' button...")
    try:
        export_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class, 'poshBtn') and contains(translate(., 'EVENT', 'event'), 'event')]")
        ))
        export_button.click()
        time.sleep(1)
        print("   ✓ Export Event Report button clicked")
    except Exception as e:
        print(f"   ✗ Failed to click Export Event Report button: {e}")
        return False

    # 2. Click "Select All" checkbox
    print("\n3. Testing 'Select All' checkbox...")
    try:
        select_all = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//label[contains(@class, 'CheckBoxList-Option') and contains(., 'Select All')]")
        ))
        select_all.click()
        time.sleep(0.5)
        print("   ✓ Select All checkbox clicked")
    except Exception as e:
        print(f"   ✗ Failed to click Select All: {e}")
        return False

    # 3. Click marketing agreement checkbox
    print("\n4. Testing marketing agreement checkbox...")
    try:
        agree_checkbox = wait.until(EC.element_to_be_clickable(
            (By.ID, "marketing-agreement-checkbox")
        ))
        agree_checkbox.click()
        time.sleep(0.5)
        print("   ✓ Marketing agreement checkbox clicked")
    except Exception as e:
        print(f"   ✗ Failed to click marketing checkbox: {e}")
        return False

    # 4. Click final "Export Report" button
    print("\n5. Testing final 'Export Report' button...")
    try:
        final_export = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(@class, 'poshBtn') and contains(@class, 'gold') and contains(., 'Export Report')]")
        ))
        print("   ✓ Found Export Report button (NOT clicking to avoid actual download)")
        print("   → To test actual download, uncomment the line below:")
        print("   → final_export.click()")
        # final_export.click()  # Uncomment to test actual download
        return True
    except Exception as e:
        print(f"   ✗ Failed to find final Export Report button: {e}")
        return False

def main():
    driver = None
    try:
        print("=" * 60)
        print("Testing Posh VIP Export Selectors")
        print("=" * 60)

        # Set up browser
        driver = setup_driver()
        current_url = driver.current_url
        print(f"\nCurrent page: {current_url}")

        # Check if we're on an event page
        if "events" not in current_url:
            print("\n⚠ WARNING: You don't appear to be on an event page.")
            print("Please navigate to a specific event page before running this test.")
            response = input("\nContinue anyway? (yes/no): ").strip().lower()
            if response not in ['yes', 'y']:
                print("Test cancelled.")
                return

        # Run tests
        print("\n" + "=" * 60)
        print("Starting Tests")
        print("=" * 60)

        # Test settings button
        if not test_settings_button(driver):
            print("\n✗ Settings button test failed. Stopping.")
            return

        # Test export flow
        if test_export_flow(driver):
            print("\n" + "=" * 60)
            print("✓ All tests passed successfully!")
            print("=" * 60)
        else:
            print("\n✗ Export flow test failed.")

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
