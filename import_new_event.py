#!/usr/bin/env python3
"""
Import New Event - Orchestrator Script

This script runs the complete workflow for importing a new event:
1. Runs posh_scraper/download_event.py to scrape event data from Posh
2. Runs raw_csv_to_sql.py to import the data into PostgreSQL

Usage:
    python import_new_event.py
"""

import subprocess
import sys
import re
from pathlib import Path

def main():
    print("=" * 60)
    print("  NEW EVENT IMPORT PIPELINE")
    print("=" * 60)
    print()

    # Step 1: Run download_event.py
    print("STEP 1: Downloading event from Posh...")
    print("-" * 60)

    download_script = Path(__file__).parent / "posh_scraper" / "download_event.py"

    try:
        result = subprocess.run(
            [sys.executable, str(download_script)],
            capture_output=True,
            text=True
        )

        # Check for errors
        if result.returncode != 0:
            print("✗ Download failed!")
            print(result.stderr)
            return 1

        # Print the output (so user sees the interactive prompts)
        print(result.stdout)

        # Parse the output to find the CSV file path
        csv_path = None
        for line in result.stdout.split('\n'):
            if line.startswith('OUTPUT_FILE:'):
                csv_path = line.split('OUTPUT_FILE:')[1].strip()
                break

        if not csv_path:
            print("✗ Could not find downloaded CSV file path")
            return 1

        print(f"\n✓ Downloaded: {csv_path}\n")

    except Exception as e:
        print(f"✗ Error running download script: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Step 2: Run raw_csv_to_sql.py
    print("\nSTEP 2: Importing data to PostgreSQL...")
    print("-" * 60)

    import_script = Path(__file__).parent / "raw_csv_to_sql.py"

    try:
        result = subprocess.run(
            [sys.executable, str(import_script), csv_path],
            check=True
        )

        print("\n" + "=" * 60)
        print("  ✓ IMPORT COMPLETE!")
        print("=" * 60)
        return 0

    except subprocess.CalledProcessError as e:
        print(f"\n✗ Import failed with exit code {e.returncode}")
        return 1
    except Exception as e:
        print(f"\n✗ Error running import script: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n✗ Process interrupted by user")
        sys.exit(1)
