#!/usr/bin/env python3
"""Fetch latest/current gold price data from MetalpriceAPI."""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys


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


def save_latest_data(df, output_dir='data/latest'):
    """Save latest data to file."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"metalpriceapi_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} records to {filepath}")

    # Also save as latest
    latest_path = os.path.join(output_dir, "metalpriceapi_latest.csv")
    df.to_csv(latest_path, index=False)
    print(f"Also saved to {latest_path}")


def main():
    """Main execution function."""
    api_key = os.environ.get('METALPRICE_API_KEY')

    if not api_key:
        print("ERROR: METALPRICE_API_KEY environment variable not set")
        sys.exit(1)

    print("=== Fetching latest gold price data ===\n")

    # Fetch recent history (last 365 days) plus today
    start_date = datetime.now().date() - timedelta(days=365)
    end_date = datetime.now().date()

    df_history = fetch_gold_history(api_key, start_date, end_date)

    # Fetch today's price
    df_today = fetch_current_gold_price(api_key)

    # Combine and deduplicate
    if df_history is not None:
        df_combined = pd.concat([df_history, df_today], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
        df_combined = df_combined.sort_values('date').reset_index(drop=True)
    else:
        df_combined = df_today

    print(f"\nTotal records: {len(df_combined)}")
    print(f"Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")

    save_latest_data(df_combined)

    print("\n=== Latest data fetch complete ===")


if __name__ == "__main__":
    main()
