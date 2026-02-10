# Posh VIP Event Report Downloader

Automated script to download event reports from Posh VIP and save them to the `Raw/` directory.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. **Start Chrome with Remote Debugging:**
   - Close all Chrome windows
   - Open Terminal and run:
     ```bash
     /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222
     ```
   - Log in to Posh VIP in the Chrome window that opens
   - Keep this Chrome window open

## Usage

Run the test script first to verify setup:
```bash
python test_listing.py
```

Then run the download script:
```bash
python download_event.py
```

The script will:
1. Open your Chrome browser (using existing session)
2. Navigate to the events page
3. Show you events one by one (from top to bottom)
4. Ask if you want to download each event
5. Download the first event you confirm
6. Rename it to the event title
7. Save it to `../Raw/` directory

## Workflow

1. Navigate to events list page
2. For each event (top to bottom):
   - Display event name
   - Ask for user confirmation
   - If "yes": download and exit
   - If "no": show next event
3. Click on selected event
4. Navigate to Settings
5. Click "Export Event Report"
6. Select all checkboxes
7. Download report
8. Rename and move to Raw folder

## Notes

- Only downloads ONE event per run
- Requires Chrome browser to be logged into Posh VIP
- Downloaded files are automatically renamed to the event title
- Files are saved to the `../Raw/` directory
