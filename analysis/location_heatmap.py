"""
Location Heat Map Generator
Creates a heat map of event locations colored by Harvard/MIT student ratio.
Heat intensity is based on total attendance count.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Circle
import numpy as np
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
from dotenv import load_dotenv
import contextily as cx
from pyproj import Transformer

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def geocode_location(location_name, geolocator, cache={}):
    """
    Geocode a location name to latitude/longitude.
    Uses cache to avoid repeated API calls for same location.
    Includes manual mappings for known Boston/Cambridge venues.
    """
    # Manual mappings for known Boston/Cambridge venues
    MANUAL_LOCATIONS = {
        'sunshine residence': (42.3731, -71.1190),  # Harvard Square area
        'russell house': (42.3656, -71.0989),  # Cambridge/Harvard area
        'viale': (42.3505, -71.0459),  # North End, Boston
        'bar enza': (42.3505, -71.0520),  # North End, Boston
        'daedalus': (42.3732, -71.1188),  # Harvard Square
        'bsmnt': (42.3554, -71.0606),  # Boston nightclub
        'gufo': (42.3505, -71.0520),  # North End, Boston
        'dx': (42.3554, -71.0605),  # Boston
        'harvest patio': (42.3732, -71.1188),  # Harvard Square
        'big night live': (42.3654, -71.0587),  # Boston
        'sulmona': (42.3640, -71.0555),  # Boston
        'sheraton commander': (42.3773, -71.1235),  # Cambridge
        'prudential center': (42.3471, -71.0817),  # Boston
        'various': (42.3601, -71.0942),  # Default to MIT
    }

    # Check manual mapping first
    location_lower = location_name.lower().strip()
    if location_lower in MANUAL_LOCATIONS:
        cache[location_name] = MANUAL_LOCATIONS[location_lower]
        return MANUAL_LOCATIONS[location_lower]

    # Check cache
    if location_name in cache:
        return cache[location_name]

    # Try geocoding with retries
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Add "Boston, MA" context to improve accuracy for local venues
            search_query = f"{location_name}, Boston, MA, USA"
            location = geolocator.geocode(search_query, timeout=10)

            if location:
                result = (location.latitude, location.longitude)
                cache[location_name] = result
                time.sleep(1)  # Rate limiting
                return result
            else:
                # Try Cambridge context
                search_query = f"{location_name}, Cambridge, MA, USA"
                location = geolocator.geocode(search_query, timeout=10)
                if location:
                    result = (location.latitude, location.longitude)
                    cache[location_name] = result
                    time.sleep(1)
                    return result
                else:
                    print(f"Warning: Could not geocode '{location_name}'")
                    cache[location_name] = None
                    return None

        except (GeocoderTimedOut, GeocoderServiceError) as e:
            if attempt < max_retries - 1:
                print(f"Geocoding timeout for '{location_name}', retrying...")
                time.sleep(2)
            else:
                print(f"Error geocoding '{location_name}': {e}")
                cache[location_name] = None
                return None

    return None


def fetch_event_data():
    """
    Fetch event data with attendance counts by school.
    Only includes events where students actually checked in.
    """
    query = """
    SELECT
        e.id as event_id,
        e.event_name,
        e.location,
        e.start_datetime,
        COUNT(DISTINCT CASE WHEN p.school = 'harvard' AND a.checked_in = TRUE THEN a.person_id END) as harvard_count,
        COUNT(DISTINCT CASE WHEN p.school = 'mit' AND a.checked_in = TRUE THEN a.person_id END) as mit_count,
        COUNT(DISTINCT CASE WHEN a.checked_in = TRUE THEN a.person_id END) as total_checkins
    FROM Events e
    LEFT JOIN Attendance a ON e.id = a.event_id
    LEFT JOIN People p ON a.person_id = p.id
    GROUP BY e.id, e.event_name, e.location, e.start_datetime
    HAVING COUNT(DISTINCT CASE WHEN a.checked_in = TRUE THEN a.person_id END) > 0
    ORDER BY e.start_datetime;
    """

    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


def calculate_harvard_ratio(row):
    """
    Calculate Harvard percentage of (Harvard + MIT) students.
    Returns None if no Harvard or MIT students attended.
    """
    total_hm = row['harvard_count'] + row['mit_count']

    if total_hm == 0:
        return None

    return (row['harvard_count'] / total_hm) * 100


def create_heatmap(df, output_path):
    """
    Create a location heat map with:
    - Color: Red (Harvard) to Blue (MIT) gradient based on ratio
    - Size: Based on total attendance
    - Map overlay: OpenStreetMap basemap
    """
    print("Processing event data...")

    # CSV already has latitude and longitude
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("Error: CSV must have latitude and longitude columns")
        return

    if len(df) == 0:
        print("Error: No events with valid coordinates and Harvard/MIT attendance found.")
        return

    # Convert to Web Mercator projection (EPSG:3857) for basemap
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    df['x'], df['y'] = transformer.transform(df['longitude'].values, df['latitude'].values)

    # Create the plot
    fig, ax = plt.subplots(figsize=(18, 14))

    # Create color map: Blue (0% Harvard/100% MIT) to Purple (50/50) to Bright Red (100% Harvard/0% MIT)
    colors = ['#0000FF', '#8B00FF', '#FF0000']  # Blue -> Purple -> Bright Red
    n_bins = 100
    cmap = mcolors.LinearSegmentedColormap.from_list('harvard_mit', colors, N=n_bins)

    # Normalize sizes based on total check-ins (INCREASED SCALE)
    min_size = 300
    max_size = 4000
    sizes = df['total_checkins'].values
    if sizes.max() > sizes.min():
        normalized_sizes = min_size + (sizes - sizes.min()) / (sizes.max() - sizes.min()) * (max_size - min_size)
    else:
        normalized_sizes = np.full(len(sizes), min_size)

    # Add size column to dataframe for later use
    df['circle_size'] = normalized_sizes

    # Handle overlapping locations by adding small offsets
    # Group by location to find duplicates
    location_counts = df.groupby(['x', 'y']).size()
    duplicate_locations = location_counts[location_counts > 1].index

    # Add offset for overlapping events
    offset_distance = 100  # meters in Web Mercator
    for loc in duplicate_locations:
        mask = (df['x'] == loc[0]) & (df['y'] == loc[1])
        overlapping_events = df[mask].index.tolist()

        # Apply circular offsets
        n_events = len(overlapping_events)
        for i, idx in enumerate(overlapping_events):
            angle = 2 * np.pi * i / n_events
            df.loc[idx, 'x'] += offset_distance * np.cos(angle)
            df.loc[idx, 'y'] += offset_distance * np.sin(angle)

    # Plot each event as a scatter point using Web Mercator coordinates
    scatter = ax.scatter(
        df['x'],
        df['y'],
        c=df['harvard_ratio'],
        s=normalized_sizes,
        cmap=cmap,
        alpha=0.7,
        edgecolors='black',
        linewidth=2,
        vmin=0,
        vmax=100,
        zorder=5  # Plot on top of basemap
    )

    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax, label='Harvard % of (Harvard + MIT)', pad=0.02)
    cbar.ax.set_ylabel('Harvard % of (Harvard + MIT)', rotation=270, labelpad=20, fontsize=12)

    # Add labels for ALL events
    # Alternate label positions to reduce overlap
    label_positions = [
        (25, 25),   # upper right
        (-25, 25),  # upper left
        (25, -25),  # lower right
        (-25, -25), # lower left
        (35, 15),   # right
        (-35, 15),  # left
        (15, 35),   # top
        (15, -35),  # bottom
        (30, 30),   # upper right (far)
        (-30, 30),  # upper left (far)
        (30, -30),  # lower right (far)
        (-30, -30), # lower left (far)
        (40, 0),    # far right
        (-40, 0),   # far left
        (0, 40),    # far top
        (0, -40),   # far bottom
    ]

    # Custom positions for specific events
    custom_positions = {
        'Netflix CFO': (-35, 35),  # higher and to the left
        'Bain Capital Ventures and Crosby': (-50, 25),  # more to the left and up
        'secret sip': (-45, -35),  # further to the left and down
        'prelaunch': (25, -35),  # down
        'amplify': (40, 35),  # up a bit
        'viale': (5, -20),  # down a bit
        'alma lasers': (30, -35),  # down a bit
        'launch': (30, 35),  # up a bit
    }

    # Custom display names (shorter versions)
    custom_names = {
        'Bain Capital Ventures and Crosby': 'Bain Capital',
    }

    print(f"Adding labels for all {len(df)} events...")
    for i, (idx, row) in enumerate(df.iterrows()):
        # Check if this event has a custom position
        event_name = row['event_name']
        if event_name in custom_positions:
            pos = custom_positions[event_name]
        else:
            pos = label_positions[i % len(label_positions)]

        # Use custom name if available
        display_name = custom_names.get(event_name, event_name)

        ax.annotate(
            f"{display_name}\n({int(row['total_checkins'])} attended)",
            xy=(row['x'], row['y']),
            xytext=pos,
            textcoords='offset points',
            fontsize=9,
            bbox=dict(boxstyle='round,pad=0.4', facecolor='yellow', alpha=0.85, edgecolor='black', linewidth=1),
            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2', lw=1.5, color='black'),
            zorder=10
        )

    # Add reference locations for Harvard and MIT
    # Harvard: 42.3770° N, 71.1167° W
    # MIT: 42.3601° N, 71.0942° W
    harvard_lat, harvard_lon = 42.3770, -71.1167
    mit_lat, mit_lon = 42.3601, -71.0942

    # Convert to Web Mercator
    harvard_x, harvard_y = transformer.transform(harvard_lon, harvard_lat)
    mit_x, mit_y = transformer.transform(mit_lon, mit_lat)

    ax.scatter(harvard_x, harvard_y, marker='*', s=1500, c='#FF0000',
               edgecolors='white', linewidth=4, label='Harvard', zorder=100)
    ax.scatter(mit_x, mit_y, marker='*', s=1500, c='darkblue',
               edgecolors='white', linewidth=4, label='MIT', zorder=100)

    # Add basemap using contextily
    print("\nAdding map overlay...")
    cx.add_basemap(
        ax,
        crs="EPSG:3857",
        source=cx.providers.OpenStreetMap.Mapnik,
        zoom=13,
        alpha=0.6
    )

    # Styling (no xlabel/ylabel needed with map)
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_title('Event Location Heat Map: Harvard vs MIT Student Ratio\n' +
                 'Size = Total Attendance | Color = Harvard % (Red=Harvard, Blue=MIT)\n' +
                 'Basemap: OpenStreetMap',
                 fontsize=15, fontweight='bold', pad=20)

    # Create simple, beautiful legend with only Harvard and MIT stars
    from matplotlib.lines import Line2D
    legend_elements = []

    # Add only university markers with bigger stars
    legend_elements.append(Line2D([0], [0], marker='*', color='w',
                                  markerfacecolor='#FF0000', markersize=24,
                                  label='Harvard', markeredgecolor='white', markeredgewidth=2.5))
    legend_elements.append(Line2D([0], [0], marker='*', color='w',
                                  markerfacecolor='darkblue', markersize=24,
                                  label='MIT', markeredgecolor='white', markeredgewidth=2.5))

    ax.legend(handles=legend_elements, loc='lower left', fontsize=14,
              framealpha=0.95, edgecolor='black', fancybox=True, shadow=True,
              labelspacing=1.8, borderpad=1.5, handletextpad=1.2)

    # Turn off axis ticks for cleaner map view
    ax.set_xticks([])
    ax.set_yticks([])

    plt.tight_layout()

    # Save
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nHeat map saved to: {output_path}")

    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total events plotted: {len(df)}")
    print(f"Average Harvard %: {df['harvard_ratio'].mean():.1f}%")
    print(f"Events with >50% Harvard: {len(df[df['harvard_ratio'] > 50])}")
    print(f"Events with >50% MIT: {len(df[df['harvard_ratio'] < 50])}")
    print(f"Total attendance (all events): {int(df['total_checkins'].sum())}")


def main():
    """Main execution function."""
    print("Starting location heat map generation...")

    # Read data from CSV file instead of database
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'analysis_outputs')
    csv_path = os.path.join(output_dir, 'location_heatmap_data.csv')

    print(f"\nReading event data from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        print("Please run the script once to generate the CSV file.")
        return

    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} events in CSV file")

    if len(df) == 0:
        print("No events found. Exiting.")
        return

    # CSV already has harvard_ratio calculated
    print(f"All {len(df)} events have Harvard/MIT students")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, 'location_heatmap.png')

    # Create heat map
    create_heatmap(df, output_path)

    print("\n✓ Heat map generation complete!")


if __name__ == "__main__":
    main()
