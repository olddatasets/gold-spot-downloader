#!/usr/bin/env python3
"""Backfill historical gold price data from various sources."""

import requests
import pandas as pd
from datetime import datetime, timedelta, date
from bs4 import BeautifulSoup
import re
import json
import os
import sys
import yfinance as yf


def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def fetch_measuringworth_data(series="london", start_year=None, end_year=None):
    """
    Fetch historical gold prices from MeasuringWorth.com.

    Available series:
    - 'london': London Market Price (1718-2023, £ until 1949 then $)
    - 'us': U.S. Official Price (1786-2020)
    - 'newyork': New York Market Price (1791-present)
    - 'british': British Official Price (1257-1945, oldest!)
    - 'goldsilver': Gold/Silver Ratio (1687-present)
    """
    # Set default start and end years based on series
    if start_year is None:
        series_start_years = {
            "British": 1257,
            "goldsilver": 1687,
            "london": 1718,
            "us": 1786,
            "newyork": 1791,
        }
        start_year = series_start_years.get(series, 1718)

    if end_year is None:
        # Some series have specific end dates
        series_end_years = {
            "British": 1945,
            "us": 2020,
        }
        end_year = series_end_years.get(series, datetime.now().year)

    # Build URL with selected series
    base_url = "https://www.measuringworth.com/datasets/gold/export.php"
    params = {"year_source": start_year, "year_result": end_year, series: "on"}

    # Convert params to query string
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    print(f"Fetching {series} series from MeasuringWorth ({start_year}-{end_year})...")
    # MeasuringWorth exports CSV with a note and citation in the first 2 rows
    df = pd.read_csv(url, skiprows=2, thousands=",")

    # MeasuringWorth format: First column is Year, second is the price
    # Rename to standardize regardless of the actual column names
    df.columns = ["year", "price"]

    # Clean price column - remove any remaining formatting
    df["price"] = pd.to_numeric(
        df["price"].astype(str).str.replace(",", ""), errors="coerce"
    )

    # Convert year to date (use January 1st of each year)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year", "price"])

    # Use datetime.date directly to handle years before 1677 (pandas limitation)
    from datetime import date
    df["date"] = df["year"].astype(int).apply(lambda y: date(y, 1, 1))

    # Add currency column based on series and year
    # British series: always GBP
    # London series: GBP until 1949, USD from 1950 onwards
    if series == "British":
        df["currency"] = "GBP"
    elif series == "london":
        df["currency"] = df["date"].apply(lambda d: "GBP" if d.year < 1950 else "USD")
    else:
        df["currency"] = "USD"  # Default for other series

    df = df[["date", "price", "currency"]]

    print(f"Fetched {len(df)} annual data points from MeasuringWorth")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    return df


def fetch_yahoo_finance_data(start_date=None, end_date=None):
    """Fetch gold futures price data from Yahoo Finance (GC=F ticker)."""
    # Default to 2025-01-01 to today if no dates specified (fills gap after World Bank)
    if start_date is None:
        start_date = date(2025, 1, 1)
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold futures from Yahoo Finance ({start_date} to {end_date})...")

    # GC=F is the ticker for gold futures
    ticker = "GC=F"

    # Download data using yfinance
    gold_data = yf.download(
        ticker,
        start=start_date.strftime('%Y-%m-%d'),
        end=end_date.strftime('%Y-%m-%d'),
        progress=False
    )

    if gold_data.empty:
        raise ValueError("No data returned from Yahoo Finance")

    # Reset index to get dates as a column
    gold_data = gold_data.reset_index()

    # Extract date and close price, handling Series properly
    dates = gold_data['Date'].dt.date.tolist()

    # Close might be a DataFrame column or Series, flatten it
    if 'Close' in gold_data.columns:
        prices = gold_data['Close'].values.flatten()
    else:
        raise ValueError("Close price column not found in Yahoo Finance data")

    # Create dataframe
    df = pd.DataFrame({
        'date': dates,
        'price': prices,
        'currency': 'USD'  # Yahoo Finance gold futures are in USD
    })

    # Remove any NaN values
    df = df.dropna()

    print(f"Fetched {len(df)} daily data points from Yahoo Finance")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_fred_data(api_key=None, start_date=None, end_date=None):
    """Fetch gold price data from Federal Reserve Economic Data (FRED)."""
    # FRED series: GOLDPMGBD228NLBM - Gold Fixing Price 3:00 P.M. (London time) in London Bullion Market (USD per troy ounce)
    # Available from 1968-04-01 to present (daily)

    if api_key is None:
        api_key = os.environ.get('FRED_API_KEY')
        if not api_key:
            raise ValueError("FRED_API_KEY environment variable not set")

    if start_date is None:
        start_date = date(1968, 4, 1)  # FRED gold data starts here
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold price data from FRED ({start_date} to {end_date})...")

    # Use FRED API with API key
    series_id = "GOLDPMGBD228NLBM"
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date.strftime('%Y-%m-%d'),
        "observation_end": end_date.strftime('%Y-%m-%d')
    }

    response = requests.get(url, params=params, timeout=30)

    # Check response before raising status
    if response.status_code != 200:
        try:
            error_data = response.json()
            raise ValueError(f"FRED API error ({response.status_code}): {error_data}")
        except:
            response.raise_for_status()

    data = response.json()

    if 'observations' not in data:
        raise ValueError(f"No observations returned from FRED API: {data}")

    # Parse observations
    observations = []
    for obs in data['observations']:
        # Skip missing values (indicated by '.')
        if obs['value'] != '.':
            date_obj = datetime.strptime(obs['date'], '%Y-%m-%d').date()
            price = float(obs['value'])
            observations.append({'date': date_obj, 'price': price, 'currency': 'USD'})

    if not observations:
        raise ValueError("No valid data returned from FRED")

    df = pd.DataFrame(observations)
    df = df.sort_values('date').reset_index(drop=True)

    print(f"Fetched {len(df)} daily data points from FRED")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_worldbank_data(start_date=None, end_date=None):
    """Fetch gold price data from World Bank Commodity Prices (Pink Sheet)."""
    # World Bank commodity price data for gold
    # Available from 1960-present (monthly)
    # Downloaded from: https://www.worldbank.org/en/research/commodity-markets

    if start_date is None:
        start_date = date(1960, 1, 1)  # World Bank data starts here
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold price data from World Bank ({start_date} to {end_date})...")

    # World Bank provides commodity data as Excel file
    # Direct download link for monthly historical data
    url = "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    # Save to temporary file and read with pandas
    import tempfile
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
        tmp_file.write(response.content)
        tmp_path = tmp_file.name

    try:
        # Read Excel file - gold prices are typically in a sheet named "Monthly Prices"
        # Skip the first few rows which contain headers
        df_excel = pd.read_excel(tmp_path, sheet_name='Monthly Prices', header=4)

        # Find the gold column - typically labeled "GOLD" or similar
        gold_col = None
        for col in df_excel.columns:
            if 'GOLD' in str(col).upper() and 'OUNCE' not in str(col).upper():
                gold_col = col
                break

        if gold_col is None:
            raise ValueError(f"Could not find gold price column. Available columns: {df_excel.columns.tolist()}")

        # Extract date and gold price
        observations = []
        for idx, row in df_excel.iterrows():
            try:
                # Date is typically in first column
                date_val = row.iloc[0]
                price_val = row[gold_col]

                # Skip if missing
                if pd.isna(date_val) or pd.isna(price_val):
                    continue

                # Convert date
                if isinstance(date_val, str):
                    date_obj = datetime.strptime(date_val, '%YM%m').date()
                else:
                    date_obj = pd.to_datetime(date_val).date()

                price = float(price_val)

                # Filter by date range
                if start_date <= date_obj <= end_date:
                    observations.append({'date': date_obj, 'price': price, 'currency': 'USD'})
            except Exception as e:
                # Skip rows that don't parse correctly
                continue

    finally:
        # Clean up temp file
        os.unlink(tmp_path)

    if not observations:
        raise ValueError("No valid data returned from World Bank")

    df = pd.DataFrame(observations)
    df = df.sort_values('date').reset_index(drop=True)

    print(f"Fetched {len(df)} monthly data points from World Bank")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_metalpriceapi_data(api_key=None, start_date=None, end_date=None):
    """Fetch gold price data from MetalpriceAPI (2024 to now)."""
    if api_key is None:
        api_key = os.environ.get('METALPRICE_API_KEY')
        if not api_key:
            raise ValueError("METALPRICE_API_KEY environment variable not set")

    # Default to 2024-01-01 to today if no dates specified
    if start_date is None:
        start_date = date(2024, 1, 1)
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold history from MetalpriceAPI ({start_date} to {end_date})...")

    url = "https://api.metalpriceapi.com/v1/timeframe"
    params = {
        "api_key": api_key,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "base": "USD",
        "currencies": "XAU"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not data.get('success'):
        raise ValueError(f"API returned error: {data}")

    # Parse the timeframe data
    rates_data = []
    for date_str, rates in data.get('rates', {}).items():
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        if 'XAU' in rates:
            price = 1 / rates['XAU']  # Convert to USD per ounce
            rates_data.append({'date': date_obj, 'price': price})

    if not rates_data:
        raise ValueError("No data returned from MetalpriceAPI")

    df = pd.DataFrame(rates_data)
    df = df.sort_values('date').reset_index(drop=True)

    print(f"Fetched {len(df)} daily data points from MetalpriceAPI")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_wikipedia_data():
    """Fetch historical gold price data from Wikipedia (5-year intervals, 1970-2010)."""
    url = "https://en.wikipedia.org/wiki/Gold_as_an_investment"

    print("Fetching historical data from Wikipedia...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the table with gold price data
    # The table should contain columns for Year and Gold price
    tables = soup.find_all("table", {"class": "wikitable"})

    for table in tables:
        # Check if this table has gold price data
        headers = [th.text.strip() for th in table.find_all("th")]
        if any("gold" in h.lower() for h in headers):
            rows = []
            for tr in table.find_all("tr")[1:]:  # Skip header row
                cells = tr.find_all("td")
                if len(cells) >= 2:
                    year_text = cells[0].text.strip()
                    price_text = cells[1].text.strip()

                    # Extract year (handle formats like "1970" or "Jan 1970")
                    year_match = re.search(r"(\d{4})", year_text)
                    if year_match:
                        year = int(year_match.group(1))

                        # Extract price (remove $ and commas)
                        price_match = re.search(
                            r"[\$]?\s*([\d,]+(?:\.\d+)?)", price_text
                        )
                        if price_match:
                            price = float(price_match.group(1).replace(",", ""))

                            # Use January 1st of that year as the date
                            date = datetime(year, 1, 1).date()
                            rows.append({"date": date, "price": price})

            if rows:
                df = pd.DataFrame(rows)
                print(
                    f"Fetched {len(df)} data points from Wikipedia (5-year intervals)"
                )
                return df

    raise ValueError("Could not find gold price table in Wikipedia")


def save_backfill_data(df, source_name, output_dir="data/backfill"):
    """Save backfill data to a source-specific file."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"{source_name}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} records to {filepath}")

    # Also save as latest for this source
    latest_path = os.path.join(output_dir, f"{source_name}_latest.csv")
    df.to_csv(latest_path, index=False)
    print(f"Also saved to {latest_path}")


def main():
    """Main execution function."""
    config = load_config()
    backfill_sources = config.get("backfill_sources", {})

    print("=== Starting historical data backfill ===\n")

    for source_name, source_config in backfill_sources.items():
        if not source_config.get("enabled", False):
            print(f"Skipping {source_name} (disabled)")
            continue

        print(f"\n--- Fetching from {source_name} ---")

        df = None
        if source_name.startswith("measuringworth"):
            series = source_config.get("series", "london")
            df = fetch_measuringworth_data(series=series)

        elif source_name == "yahoo_finance":
            df = fetch_yahoo_finance_data()

        elif source_name == "worldbank":
            df = fetch_worldbank_data()

        elif source_name == "fred":
            df = fetch_fred_data()

        elif source_name == "wikipedia":
            df = fetch_wikipedia_data()

        if df is not None and not df.empty:
            save_backfill_data(df, source_name)
        else:
            print(f"No data fetched from {source_name}")

    print("\n=== Backfill complete ===")


if __name__ == "__main__":
    main()
