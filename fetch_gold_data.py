#!/usr/bin/env python3
"""Fetch gold spot price history and save as CSV."""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import json
from bs4 import BeautifulSoup
import re

def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Config file {config_path} not found, using defaults")
        return {
            "sources": {
                "metalpriceapi": {"enabled": True, "priority": 1}
            },
            "merge_strategy": {
                "prefer_higher_granularity": True
            }
        }

def merge_dataframes(dataframes, strategy='prefer_higher_granularity'):
    """Merge multiple dataframes with different granularities."""
    if not dataframes:
        return pd.DataFrame(columns=['date', 'price'])

    # Combine all dataframes
    df_combined = pd.concat(dataframes, ignore_index=True)

    if strategy == 'prefer_higher_granularity':
        # Remove duplicates, keeping the first occurrence (higher priority source)
        df_combined = df_combined.drop_duplicates(subset=['date'], keep='first')

    # Sort by date
    df_combined = df_combined.sort_values('date').reset_index(drop=True)

    return df_combined

def load_existing_data():
    """Load existing historical data from the website or local file."""
    # Try to load from published website first
    website_url = "https://freeprice.gold/data/latest.csv"
    local_path = "data/latest.csv"

    try:
        print(f"Loading existing data from {website_url}...")
        df = pd.read_csv(website_url)
        df['date'] = pd.to_datetime(df['date']).dt.date
        print(f"Loaded {len(df)} existing records from website")
        return df
    except Exception as e:
        print(f"Could not load from website: {e}")

        # Fall back to local file if it exists
        if os.path.exists(local_path):
            try:
                print(f"Loading existing data from {local_path}...")
                df = pd.read_csv(local_path)
                df['date'] = pd.to_datetime(df['date']).dt.date
                print(f"Loaded {len(df)} existing records from local file")
                return df
            except Exception as e:
                print(f"Could not load from local file: {e}")

        # Return empty DataFrame if nothing works
        print("Starting with empty dataset")
        return pd.DataFrame(columns=['date', 'price'])

def fetch_current_gold_price(api_key):
    """Fetch current gold price from MetalpriceAPI."""
    url = "https://api.metalpriceapi.com/v1/latest"
    params = {
        "api_key": api_key,
        "base": "USD",
        "currencies": "XAU"
    }

    try:
        print("Fetching current gold price from MetalpriceAPI...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data.get('success'):
            print(f"API returned error: {data}")
            sys.exit(1)

        # MetalpriceAPI returns rates in the format: base currency per 1 unit of XAU
        # We need to invert to get USD per troy ounce of gold
        rate = data['rates']['XAU']
        price = 1 / rate  # Convert to USD per ounce
        today = datetime.now().date()

        print(f"Current gold price: ${price:,.2f} per troy ounce")
        return pd.DataFrame([{'date': today, 'price': price}])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching current price: {e}")
        sys.exit(1)

def fetch_gold_history(api_key, start_date, end_date):
    """Fetch gold price history for a date range using MetalpriceAPI timeframe endpoint."""
    url = "https://api.metalpriceapi.com/v1/timeframe"

    params = {
        "api_key": api_key,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "base": "USD",
        "currencies": "XAU"
    }

    try:
        print(f"Fetching gold history from {start_date} to {end_date}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data.get('success'):
            print(f"API returned error: {data}")
            return None

        # Parse the timeframe data
        rates_data = []
        for date_str, rates in data.get('rates', {}).items():
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            if 'XAU' in rates:
                price = 1 / rates['XAU']  # Convert to USD per ounce
                rates_data.append({'date': date_obj, 'price': price})

        if rates_data:
            df = pd.DataFrame(rates_data)
            print(f"Successfully fetched {len(df)} days of historical data")
            return df
        else:
            print("No historical data returned")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching historical data: {e}")
        return None

def fetch_wikipedia_data():
    """Fetch historical gold price data from Wikipedia (5-year intervals, 1970-2010)."""
    url = "https://en.wikipedia.org/wiki/Gold_as_an_investment"

    try:
        print("Fetching historical data from Wikipedia...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table with gold price data
        # The table should contain columns for Year and Gold price
        tables = soup.find_all('table', {'class': 'wikitable'})

        for table in tables:
            # Check if this table has gold price data
            headers = [th.text.strip() for th in table.find_all('th')]
            if any('gold' in h.lower() for h in headers):
                rows = []
                for tr in table.find_all('tr')[1:]:  # Skip header row
                    cells = tr.find_all('td')
                    if len(cells) >= 2:
                        year_text = cells[0].text.strip()
                        price_text = cells[1].text.strip()

                        # Extract year (handle formats like "1970" or "Jan 1970")
                        year_match = re.search(r'(\d{4})', year_text)
                        if year_match:
                            year = int(year_match.group(1))

                            # Extract price (remove $ and commas)
                            price_match = re.search(r'[\$]?\s*([\d,]+(?:\.\d+)?)', price_text)
                            if price_match:
                                price = float(price_match.group(1).replace(',', ''))

                                # Use January 1st of that year as the date
                                date = datetime(year, 1, 1).date()
                                rows.append({'date': date, 'price': price})

                if rows:
                    df = pd.DataFrame(rows)
                    print(f"Fetched {len(df)} data points from Wikipedia (5-year intervals)")
                    return df

        print("Could not find gold price table in Wikipedia")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Error fetching Wikipedia data: {e}")
        return None

def fetch_datahub_data():
    """Fetch monthly gold prices from DataHub.io (1833 onwards)."""
    url = "https://datahub.io/core/gold-prices/r/monthly.csv"

    try:
        print("Fetching monthly historical data from DataHub.io...")
        df = pd.read_csv(url)

        # DataHub format: columns are 'date' and 'price'
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date']).dt.date

        print(f"Fetched {len(df)} monthly data points from DataHub.io")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")

        return df

    except Exception as e:
        print(f"Error fetching DataHub.io data: {e}")
        return None

def fetch_measuringworth_data(series='london', start_year=1718, end_year=None):
    """
    Fetch historical gold prices from MeasuringWorth.com.

    Available series:
    - 'london': London Market Price (1718-2023, £ until 1949 then $)
    - 'us': U.S. Official Price (1786-2020)
    - 'newyork': New York Market Price (1791-present)
    - 'british': British Official Price (1257-1945, oldest!)
    - 'goldsilver': Gold/Silver Ratio (1687-present)
    """
    if end_year is None:
        end_year = datetime.now().year

    # Build URL with selected series
    base_url = "https://www.measuringworth.com/datasets/gold/export.php"
    params = {
        'year_source': start_year,
        'year_result': end_year,
        series: 'on'
    }

    # Convert params to query string
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    try:
        print(f"Fetching {series} series from MeasuringWorth ({start_year}-{end_year})...")
        # Skip first row (citation line) and use second row as header
        df = pd.read_csv(url, skiprows=1, thousands=',')

        # MeasuringWorth format: 'Year' and price column
        # Rename columns to standardize
        df.columns = ['year', 'price']

        # Clean price column - remove any remaining formatting
        df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(',', ''), errors='coerce')

        # Convert year to date (use January 1st of each year)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df = df.dropna(subset=['year', 'price'])
        df['date'] = pd.to_datetime(df['year'].astype(int), format='%Y').dt.date
        df = df[['date', 'price']]

        print(f"Fetched {len(df)} annual data points from MeasuringWorth")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")

        return df

    except Exception as e:
        print(f"Error fetching MeasuringWorth data: {e}")
        return None

def backfill_missing_dates(df_existing, api_key):
    """Backfill any missing dates in the existing dataset."""
    if df_existing.empty:
        # If completely empty, start from a reasonable historical date
        # MetalpriceAPI free tier may have limitations, so start from 1 year ago
        start_date = datetime.now().date() - timedelta(days=365)
        print(f"No existing data, starting from {start_date}")
    else:
        # Find the earliest and latest dates
        min_date = df_existing['date'].min()
        max_date = df_existing['date'].max()
        print(f"Existing data range: {min_date} to {max_date}")

        # Check for gaps
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        existing_dates = set(df_existing['date'])
        missing_dates = [d.date() for d in date_range if d.date() not in existing_dates]

        if not missing_dates:
            print("No missing dates to backfill")
            return df_existing

        print(f"Found {len(missing_dates)} missing dates to backfill")
        # For simplicity, we'll fetch the entire range again if there are gaps
        start_date = min_date

    # Fetch historical data
    end_date = datetime.now().date()
    df_historical = fetch_gold_history(api_key, start_date, end_date)

    if df_historical is not None and not df_historical.empty:
        # Merge with existing data, preferring new data
        df_combined = pd.concat([df_existing, df_historical], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
        df_combined = df_combined.sort_values('date').reset_index(drop=True)
        return df_combined

    return df_existing

def save_csv(df, output_dir='data'):
    """Save DataFrame to CSV with timestamp in filename."""
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with current date
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"gold_spot_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Save to CSV
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

    # Also save as latest.csv for easy reference
    latest_path = os.path.join(output_dir, 'latest.csv')
    df.to_csv(latest_path, index=False)
    print(f"Data also saved to {latest_path}")

    return filename

def update_index_html(latest_filename):
    """Update index.html to redirect to the latest CSV file."""
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="0; url=data/latest.csv">
    <title>Gold Spot Price Data</title>
</head>
<body>
    <h1>Gold Spot Price Historical Data</h1>
    <p>Redirecting to the latest data...</p>
    <p>If not redirected, <a href="data/latest.csv">click here for the latest data</a></p>
    <p>Latest file: <a href="data/{latest_filename}">{latest_filename}</a></p>
    <p>Source code available on <a href="https://github.com/posix4e/freeprice.gold">GitHub</a></p>
</body>
</html>"""

    with open('index.html', 'w') as f:
        f.write(html_content)
    print(f"Updated index.html to point to {latest_filename}")

def fetch_from_sources(config, api_key=None):
    """Fetch data from all enabled sources based on config."""
    sources = config.get('sources', {})
    dataframes = []

    # Sort sources by priority
    sorted_sources = sorted(
        [(name, cfg) for name, cfg in sources.items() if cfg.get('enabled', False)],
        key=lambda x: x[1].get('priority', 999)
    )

    for source_name, source_config in sorted_sources:
        print(f"\n--- Fetching from {source_name} (priority {source_config.get('priority')}) ---")

        if source_name == 'metalpriceapi':
            if not api_key:
                print("Skipping MetalpriceAPI: no API key provided")
                continue
            # Fetch recent data from MetalpriceAPI
            df = fetch_gold_history(
                api_key,
                datetime.now().date() - timedelta(days=365),
                datetime.now().date()
            )
            if df is not None:
                dataframes.append(df)

        elif source_name == 'datahub':
            df = fetch_datahub_data()
            if df is not None:
                dataframes.append(df)

        elif source_name == 'measuringworth':
            series = source_config.get('series', 'london')
            df = fetch_measuringworth_data(series=series)
            if df is not None:
                dataframes.append(df)

        elif source_name == 'wikipedia':
            df = fetch_wikipedia_data()
            if df is not None:
                dataframes.append(df)

    return dataframes

def main():
    """Main execution function."""
    # Load configuration
    config = load_config()

    # Get API key (optional now, depends on enabled sources)
    api_key = os.environ.get('METALPRICE_API_KEY')

    # Load existing historical data
    df_existing = load_existing_data()

    # Fetch data from all enabled sources
    print("\n=== Fetching data from configured sources ===")
    source_dataframes = fetch_from_sources(config, api_key)

    # Merge all source data
    merge_strategy = config.get('merge_strategy', {}).get('prefer_higher_granularity', True)
    strategy = 'prefer_higher_granularity' if merge_strategy else 'keep_all'
    df_new = merge_dataframes(source_dataframes, strategy)

    # Merge with existing data
    if not df_existing.empty and not df_new.empty:
        df_combined = merge_dataframes([df_new, df_existing], strategy)
    elif not df_new.empty:
        df_combined = df_new
    else:
        df_combined = df_existing

    # Fetch today's price from MetalpriceAPI if available
    if api_key:
        try:
            df_today = fetch_current_gold_price(api_key)
            today = df_today['date'].iloc[0]

            # Check if today's data already exists
            if today in df_combined['date'].values:
                print(f"Price for {today} already exists, updating...")
                df_combined = df_combined[df_combined['date'] != today]

            # Append today's price
            df_combined = pd.concat([df_combined, df_today], ignore_index=True)
            df_combined = df_combined.sort_values('date').reset_index(drop=True)
        except Exception as e:
            print(f"Could not fetch today's price: {e}")

    print(f"\n=== Summary ===")
    print(f"Total records: {len(df_combined)}")
    if not df_combined.empty:
        print(f"Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")

    # Save to CSV
    filename = save_csv(df_combined)

    # Update index.html
    update_index_html(filename)

    print("\nDone!")

if __name__ == "__main__":
    main()
