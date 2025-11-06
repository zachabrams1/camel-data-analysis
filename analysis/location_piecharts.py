"""
Location Pie Chart Generator
Creates a map with pie charts at each event location showing school distribution.
Size of pie charts is based on total attendance count.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyproj import Transformer
import contextily as cx


def create_pie_at_location(ax, center_x, center_y, data, radius, colors):
    """
    Draw a pie chart at a specific location on the map.

    Args:
        ax: matplotlib axes
        center_x, center_y: center coordinates in Web Mercator
        data: dict with school names as keys and counts as values
        radius: radius of the pie chart in map units
        colors: dict mapping school names to colors
    """
    # Filter out schools with 0 count
    data = {k: v for k, v in data.items() if v > 0}

    if not data:
        return

    # Convert to lists for pie chart
    labels = list(data.keys())
    sizes = list(data.values())
    pie_colors = [colors.get(label, '#CCCCCC') for label in labels]

    # Calculate angles for each slice
    total = sum(sizes)
    angles = []
    start_angle = 90  # Start at top

    for size in sizes:
        angle = (size / total) * 360
        angles.append((start_angle, angle))
        start_angle += angle

    # Draw each pie slice as a wedge
    for i, (label, size) in enumerate(zip(labels, sizes)):
        start_angle, angle = angles[i]

        # Create wedge vertices
        n_points = max(3, int(angle / 5))  # More points for larger slices
        theta = np.linspace(np.radians(start_angle),
                           np.radians(start_angle + angle),
                           n_points)

        # Wedge vertices: center + arc points
        x_coords = [center_x] + list(center_x + radius * np.cos(theta))
        y_coords = [center_y] + list(center_y + radius * np.sin(theta))

        ax.fill(x_coords, y_coords, color=pie_colors[i],
               edgecolor='black', linewidth=1.5, alpha=0.8, zorder=5)


def create_piecharts_map(df, output_path):
    """
    Create a location map with pie charts showing school distribution at each event.
    - Pie chart composition: Shows proportion of students from each school
    - Size: Based on total attendance
    - Map overlay: OpenStreetMap basemap
    """
    print("Processing event data...")

    # CSV already has latitude and longitude
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("Error: CSV must have latitude and longitude columns")
        return

    if len(df) == 0:
        print("Error: No events with valid coordinates found.")
        return

    # Convert to Web Mercator projection (EPSG:3857) for basemap
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    df['x'], df['y'] = transformer.transform(df['longitude'].values, df['latitude'].values)

    # Create the plot
    fig, ax = plt.subplots(figsize=(18, 14))

    # Define colors for each school
    school_colors = {
        'harvard': '#FF0000',  # Red
        'mit': '#8B00FF',      # Purple
    }

    # Calculate radius for each pie chart based on total check-ins
    min_radius = 80    # meters in Web Mercator
    max_radius = 300   # meters in Web Mercator
    sizes = df['total_checkins'].values
    if sizes.max() > sizes.min():
        radii = min_radius + (sizes - sizes.min()) / (sizes.max() - sizes.min()) * (max_radius - min_radius)
    else:
        radii = np.full(len(sizes), min_radius)

    df['radius'] = radii

    # Handle overlapping locations by adding offsets
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

    # Draw pie chart for each event
    print(f"Drawing pie charts for {len(df)} events...")
    for idx, row in df.iterrows():
        # Only include Harvard and MIT students
        pie_data = {
            'harvard': row['harvard_count'],
            'mit': row['mit_count'],
        }

        create_pie_at_location(
            ax,
            row['x'],
            row['y'],
            pie_data,
            row['radius'],
            school_colors
        )

    # Add labels for ALL events
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

    # Custom positions for specific events (to avoid overlap)
    custom_positions = {
        'Netflix CFO': (-35, 35),
        'Bain Capital Ventures and Crosby': (-50, 25),
        # 'secret sip': (-45, -35),  # Filtered out
        # 'prelaunch': (25, -35),  # Filtered out
        'amplify': (30, 30),
        'viale': (15, -30),  # Moved right and down
        'alma lasers': (0, -50),
        'launch': (30, 35),
        'camel x biotech': (30, -25),  # Moved right and down
        'Fall 2025 BNL Party': (-130, 0),  # Moved to the left
        'zelnick dinner': (-75, 25),
        'zelnick speaker': (25, 25),
        'russell house': (0, 25),
    }

    # Custom display names (shorter versions)
    custom_names = {
        'Bain Capital Ventures and Crosby': 'Bain Capital',
    }

    print(f"Adding labels for all {len(df)} events...")
    for i, (idx, row) in enumerate(df.iterrows()):
        event_name = row['event_name']
        if event_name in custom_positions:
            pos = custom_positions[event_name]
        else:
            pos = label_positions[i % len(label_positions)]

        display_name = custom_names.get(event_name, event_name)

        # Simple label without percentages
        total = row['total_checkins']
        label_text = f"{display_name}\n({int(total)} attended)"

        ax.annotate(
            label_text,
            xy=(row['x'], row['y']),
            xytext=pos,
            textcoords='offset points',
            fontsize=8,
            bbox=dict(boxstyle='round,pad=0.4', facecolor='yellow', alpha=0.85,
                     edgecolor='black', linewidth=1),
            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2',
                          lw=1.5, color='black'),
            zorder=10
        )

    # Add reference locations for Harvard and MIT
    harvard_lat, harvard_lon = 42.3770, -71.1167
    mit_lat, mit_lon = 42.3601, -71.0942

    # Convert to Web Mercator
    harvard_x, harvard_y = transformer.transform(harvard_lon, harvard_lat)
    mit_x, mit_y = transformer.transform(mit_lon, mit_lat)

    ax.scatter(harvard_x, harvard_y, marker='*', s=1500, c='#FF0000',
               edgecolors='white', linewidth=4, label='Harvard', zorder=100)
    ax.scatter(mit_x, mit_y, marker='*', s=1500, c='#8B00FF',
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

    # Styling
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_title('Event Location Pie Charts: Harvard vs MIT Student Distribution\n' +
                 'Size = Total Attendance | Colors: Red=Harvard, Purple=MIT\n' +
                 'Basemap: OpenStreetMap',
                 fontsize=15, fontweight='bold', pad=20)

    # Create legend
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    legend_elements = []

    # Add university markers
    legend_elements.append(Line2D([0], [0], marker='*', color='w',
                                  markerfacecolor='#FF0000', markersize=24,
                                  label='Harvard Campus', markeredgecolor='white',
                                  markeredgewidth=2.5))
    legend_elements.append(Line2D([0], [0], marker='*', color='w',
                                  markerfacecolor='#8B00FF', markersize=24,
                                  label='MIT Campus', markeredgecolor='white',
                                  markeredgewidth=2.5))

    # Add school color legend
    legend_elements.append(Patch(facecolor='#FF0000', edgecolor='black',
                                 label='Harvard Students'))
    legend_elements.append(Patch(facecolor='#8B00FF', edgecolor='black',
                                 label='MIT Students'))

    ax.legend(handles=legend_elements, loc='lower left', fontsize=12,
              framealpha=0.95, edgecolor='black', fancybox=True, shadow=True,
              labelspacing=1.5, borderpad=1.2, handletextpad=1.0)

    # Turn off axis ticks for cleaner map view
    ax.set_xticks([])
    ax.set_yticks([])

    plt.tight_layout()

    # Save
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPie chart map saved to: {output_path}")

    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total events plotted: {len(df)}")
    print(f"Total attendance (all events): {int(df['total_checkins'].sum())}")
    print(f"Total Harvard students: {int(df['harvard_count'].sum())}")
    print(f"Total MIT students: {int(df['mit_count'].sum())}")
    other_total = df['total_checkins'].sum() - df['harvard_count'].sum() - df['mit_count'].sum()
    print(f"Total students from other schools: {int(other_total)}")

    # Calculate average school distribution
    avg_harvard_pct = (df['harvard_count'].sum() / df['total_checkins'].sum() * 100)
    avg_mit_pct = (df['mit_count'].sum() / df['total_checkins'].sum() * 100)
    avg_other_pct = 100 - avg_harvard_pct - avg_mit_pct
    print(f"\nOverall Distribution:")
    print(f"  Harvard: {avg_harvard_pct:.1f}%")
    print(f"  MIT: {avg_mit_pct:.1f}%")
    print(f"  Other: {avg_other_pct:.1f}%")


def main():
    """Main execution function."""
    print("Starting location pie chart map generation...")

    # Read data from CSV file
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'analysis_outputs')
    csv_path = os.path.join(output_dir, 'location_heatmap_data.csv')

    print(f"\nReading event data from: {csv_path}")

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        print("Please ensure the location_heatmap_data.csv file exists.")
        return

    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} events in CSV file")

    # Filter out specific events
    df = df[~df['event_name'].str.lower().isin(['prelaunch', 'secret sip'])]
    print(f"After filtering: {len(df)} events remaining")

    if len(df) == 0:
        print("No events found. Exiting.")
        return

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, 'location_piecharts.png')

    # Create pie charts map
    create_piecharts_map(df, output_path)

    print("\n✓ Pie chart map generation complete!")


if __name__ == "__main__":
    main()
