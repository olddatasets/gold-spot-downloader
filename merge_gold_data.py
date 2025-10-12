#!/usr/bin/env python3
"""Merge backfill and latest gold price data into final dataset."""

import pandas as pd
from datetime import datetime
import os
import sys
import json
import glob


def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Config file {config_path} not found, using defaults")
        return {"merge_strategy": {"prefer_higher_granularity": True}}


def load_existing_data():
    """Load existing historical data from the website or local file."""
    # Try to load from published website first
    website_url = "https://freeprice.gold/data/latest.csv"
    local_path = "data/latest.csv"

    try:
        print(f"Loading existing data from {website_url}...")
        df = pd.read_csv(website_url)
        from datetime import datetime as dt
        df['date'] = df['date'].apply(lambda x: dt.strptime(str(x), '%Y-%m-%d').date())
        print(f"Loaded {len(df)} existing records from website")
        return df
    except Exception as e:
        print(f"Could not load from website: {e}")

        # Fall back to local file if it exists
        if os.path.exists(local_path):
            try:
                print(f"Loading existing data from {local_path}...")
                df = pd.read_csv(local_path)
                from datetime import datetime as dt
                df['date'] = df['date'].apply(lambda x: dt.strptime(str(x), '%Y-%m-%d').date())
                print(f"Loaded {len(df)} existing records from local file")
                return df
            except Exception as e:
                print(f"Could not load from local file: {e}")

        # Return empty DataFrame if nothing works
        print("No existing data found, starting fresh")
        return pd.DataFrame(columns=['date', 'price'])


def load_backfill_data(backfill_dir='data/backfill'):
    """Load all backfill data files in priority order."""
    if not os.path.exists(backfill_dir):
        print(f"Backfill directory {backfill_dir} not found")
        return []

    # Define priority order for backfill sources (higher priority = later in list)
    # Sources added later in the list will override earlier sources for duplicate dates
    priority_order = [
        'measuringworth_british',  # 1257-1717, oldest data, lowest priority
        'measuringworth_london',    # 1718-1959, annual data
        'worldbank',                # 1960-2024, monthly data
        'yahoo_finance',            # 2025-present, daily data, highest priority
    ]

    dataframes = []
    for source_name in priority_order:
        filepath = os.path.join(backfill_dir, f'{source_name}_latest.csv')
        if os.path.exists(filepath):
            try:
                print(f"Loading backfill data from {source_name}...")
                df = pd.read_csv(filepath)
                # Parse dates as strings first, then convert using Python's datetime
                from datetime import datetime as dt
                df['date'] = df['date'].apply(lambda x: dt.strptime(x, '%Y-%m-%d').date())
                df['source'] = source_name
                print(f"  Loaded {len(df)} records (range: {df['date'].min()} to {df['date'].max()})")
                dataframes.append(df)
            except Exception as e:
                print(f"Error loading {filepath}: {e}")

    return dataframes


def load_latest_data(latest_dir='data/latest'):
    """Load latest/current data (deprecated - now using backfill sources only)."""
    # This function is kept for backward compatibility but is no longer used
    # All data sources are now loaded through backfill
    return None


def merge_dataframes(dataframes, strategy='prefer_higher_granularity'):
    """Merge multiple dataframes with different granularities.

    Returns both the merged dataframe and source statistics.
    """
    if not dataframes:
        return pd.DataFrame(columns=['date', 'price']), {}, {}

    # Combine all dataframes (later dataframes have higher priority)
    df_combined = pd.concat(dataframes, ignore_index=True)

    # Calculate full source ranges BEFORE deduplication
    full_source_ranges = {}
    if 'source' in df_combined.columns:
        for source in df_combined['source'].unique():
            if pd.notna(source):  # Skip NaN sources
                source_df = df_combined[df_combined['source'] == source]
                full_source_ranges[source] = {
                    'count': len(source_df),
                    'start': str(source_df['date'].min()),
                    'end': str(source_df['date'].max())
                }

    if strategy == 'prefer_higher_granularity':
        # Remove duplicates, keeping the LAST occurrence (higher priority source)
        # Since we added dataframes in priority order (lowest to highest)
        df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')

    # Sort by date
    df_combined = df_combined.sort_values('date').reset_index(drop=True)

    # Calculate source statistics AFTER deduplication (what's actually used)
    source_stats = {}
    if 'source' in df_combined.columns:
        for source in df_combined['source'].unique():
            if pd.notna(source):  # Skip NaN sources
                source_df = df_combined[df_combined['source'] == source]
                source_stats[source] = {
                    'count': len(source_df),
                    'start': str(source_df['date'].min()),
                    'end': str(source_df['date'].max())
                }

    return df_combined, source_stats, full_source_ranges


def save_csv(df, source_stats=None, output_dir='data'):
    """Save DataFrame to CSV with timestamp in filename."""
    os.makedirs(output_dir, exist_ok=True)

    # Keep date, price, and currency columns; drop the source column
    columns_to_save = ['date', 'price']
    if 'currency' in df.columns:
        columns_to_save.append('currency')
    df_output = df[columns_to_save].copy()

    # Generate filename with current date
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"gold_spot_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Save to CSV
    df_output.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

    # Also save as latest.csv for easy reference
    latest_path = os.path.join(output_dir, 'latest.csv')
    df_output.to_csv(latest_path, index=False)
    print(f"Data also saved to {latest_path}")

    # Save source statistics as JSON
    if source_stats:
        stats_path = os.path.join(output_dir, 'source_stats.json')
        with open(stats_path, 'w') as f:
            json.dump(source_stats, f, indent=2)
        print(f"Source statistics saved to {stats_path}")

    return filename


def update_index_html(latest_filename, source_stats):
    """Update index.html to redirect to the latest CSV file with proper attribution."""
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="0; url=data/latest.csv">
    <title>Gold Spot Price Data</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 40px; }}
        .stat {{ background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>Gold Spot Price Historical Data</h1>
    <p>Redirecting to the latest data...</p>
    <p>If not redirected, <a href="data/latest.csv">click here for the latest data</a></p>
    <p>Latest file: <a href="data/{latest_filename}">{latest_filename}</a></p>

    <h2>Data Sources</h2>
    <p><a href="sources.html"><strong>→ View interactive data source timeline</strong></a></p>

    <div class="stat">
        <h3>Coverage Summary</h3>
        <ul>"""

    # Add source statistics if available
    if source_stats:
        for source, stats in source_stats.items():
            source_name = {
                'measuringworth_british': 'MeasuringWorth British Official Price',
                'measuringworth_london': 'MeasuringWorth London Market Price',
                'worldbank': 'World Bank Commodity Prices',
                'fred': 'Federal Reserve (FRED)',
                'yahoo_finance': 'Yahoo Finance',
                'metalpriceapi': 'MetalpriceAPI (daily)'
            }.get(source, source)
            html_content += f"""
            <li><strong>{source_name}</strong>: {stats['count']:,} records ({stats['start']} to {stats['end']})</li>"""

    html_content += """
        </ul>
    </div>

    <h3>Source Attribution</h3>
    <ul>
        <li><strong>Recent prices</strong>: <a href="https://metalpriceapi.com/">MetalpriceAPI</a> (daily, last 365 days)</li>
        <li><strong>London Market Price</strong>: <a href="https://www.measuringworth.com/datasets/gold/">MeasuringWorth</a> (annual, 1718-2024)</li>
        <li><strong>British Official Price</strong>: <a href="https://www.measuringworth.com/datasets/gold/">MeasuringWorth</a> (annual, 1257-1945)
            <br><small>Citation: Lawrence H. Officer and Samuel H. Williamson, 'The Price of Gold, 1257-2014,' MeasuringWorth, 2025</small>
        </li>
    </ul>

    <p>Source code available on <a href="https://github.com/posix4e/gold-spot-downloader">GitHub</a></p>

    <footer>
        <p><small>This data is compiled for non-profit educational purposes. MeasuringWorth data used with proper attribution as per their terms of use.</small></p>
    </footer>
</body>
</html>"""

    with open('index.html', 'w') as f:
        f.write(html_content)
    print(f"Updated index.html with data source attribution")


def main():
    """Main execution function."""
    config = load_config()

    print("=== Starting data merge ===\n")

    # Load all data sources in priority order
    dataframes = []

    # 1. Load latest data (highest priority - daily granularity)
    df_latest = load_latest_data()
    if df_latest is not None:
        dataframes.append(df_latest)

    # 2. Load backfill data (monthly/annual granularity)
    backfill_dfs = load_backfill_data()
    dataframes.extend(backfill_dfs)

    # 3. Load existing published data (fallback) - only if we have no other data
    if not dataframes:
        df_existing = load_existing_data()
        if not df_existing.empty:
            dataframes.append(df_existing)

    if not dataframes:
        print("ERROR: No data sources available to merge")
        sys.exit(1)

    # Merge all data
    print("\n--- Merging all data sources ---")
    merge_strategy = config.get('merge_strategy', {}).get('prefer_higher_granularity', True)
    strategy = 'prefer_higher_granularity' if merge_strategy else 'keep_all'
    df_merged, source_stats, full_source_ranges = merge_dataframes(dataframes, strategy)

    print(f"\n=== Merge Summary ===")
    print(f"Total records: {len(df_merged)}")
    if not df_merged.empty:
        print(f"Date range: {df_merged['date'].min()} to {df_merged['date'].max()}")

    # Print source statistics
    if source_stats:
        print("\n=== Source Statistics (Used in Final Dataset) ===")
        for source, stats in source_stats.items():
            print(f"{source}: {stats['count']:,} records ({stats['start']} to {stats['end']})")

    if full_source_ranges:
        print("\n=== Full Source Coverage (Before Merge) ===")
        for source, stats in full_source_ranges.items():
            print(f"{source}: {stats['count']:,} records ({stats['start']} to {stats['end']})")

    # Save to CSV
    filename = save_csv(df_merged, source_stats)

    # Save full source ranges for timeline visualization
    if full_source_ranges:
        full_ranges_path = 'data/source_ranges_full.json'
        with open(full_ranges_path, 'w') as f:
            json.dump(full_source_ranges, f, indent=2)
        print(f"Full source ranges saved to {full_ranges_path}")

    # Update index.html with attribution
    update_index_html(filename, source_stats)

    print("\n=== Merge complete ===")


if __name__ == "__main__":
    main()
