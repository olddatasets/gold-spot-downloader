#!/usr/bin/env python3
"""Fetch and merge historical gold price data from multiple sources."""

import json
import os
import sys
import tempfile
from datetime import date, datetime
from io import StringIO

import pandas as pd
import requests
import yfinance as yf


def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def fetch_measuringworth_data(series="london", start_year=None, end_year=None):
    """Fetch historical gold prices from MeasuringWorth.com."""
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
        series_end_years = {
            "British": 1945,
            "us": 2020,
        }
        end_year = series_end_years.get(series, datetime.now().year)

    base_url = "https://www.measuringworth.com/datasets/gold/export.php"
    params = {"year_source": start_year, "year_result": end_year, series: "on"}
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    print(f"Fetching {series} series from MeasuringWorth ({start_year}-{end_year})...")
    df = pd.read_csv(url, skiprows=2, thousands=",")
    df.columns = ["year", "price"]

    df["price"] = pd.to_numeric(
        df["price"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year", "price"])

    df["date"] = df["year"].astype(int).apply(lambda y: date(y, 1, 1))

    if series == "British":
        df["currency"] = "GBP"
    elif series == "london":
        df["currency"] = df["date"].apply(lambda d: "GBP" if d.year < 1950 else "USD")
    else:
        df["currency"] = "USD"

    df = df[["date", "price", "currency"]]
    print(f"Fetched {len(df)} annual data points from MeasuringWorth")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_yahoo_finance_data(start_date=None, end_date=None):
    """Fetch gold futures price data from Yahoo Finance (GC=F ticker)."""
    if start_date is None:
        start_date = date(2025, 1, 1)
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold futures from Yahoo Finance ({start_date} to {end_date})...")
    ticker = "GC=F"
    gold_data = yf.download(
        ticker,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        progress=False,
    )

    if gold_data.empty:
        raise ValueError("No data returned from Yahoo Finance")

    gold_data = gold_data.reset_index()
    dates = gold_data["Date"].dt.date.tolist()

    if "Close" in gold_data.columns:
        prices = gold_data["Close"].values.flatten()
    else:
        raise ValueError("Close price column not found in Yahoo Finance data")

    df = pd.DataFrame({"date": dates, "price": prices, "currency": "USD"})
    df = df.dropna()

    print(f"Fetched {len(df)} daily data points from Yahoo Finance")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_worldbank_data(start_date=None, end_date=None):
    """Fetch gold price data from World Bank Commodity Prices (Pink Sheet)."""
    if start_date is None:
        start_date = date(1960, 1, 1)
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching gold price data from World Bank ({start_date} to {end_date})...")
    url = "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx"

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
        tmp_file.write(response.content)
        tmp_path = tmp_file.name

    try:
        df_excel = pd.read_excel(tmp_path, sheet_name="Monthly Prices", header=4)

        gold_col = None
        for col in df_excel.columns:
            if "GOLD" in str(col).upper() and "OUNCE" not in str(col).upper():
                gold_col = col
                break

        if gold_col is None:
            raise ValueError(
                f"Could not find gold price column. Available columns: {df_excel.columns.tolist()}"
            )

        observations = []
        for idx, row in df_excel.iterrows():
            try:
                date_val = row.iloc[0]
                price_val = row[gold_col]

                if pd.isna(date_val) or pd.isna(price_val):
                    continue

                if isinstance(date_val, str):
                    date_obj = datetime.strptime(date_val, "%YM%m").date()
                else:
                    date_obj = pd.to_datetime(date_val).date()

                price = float(price_val)

                if start_date <= date_obj <= end_date:
                    observations.append(
                        {"date": date_obj, "price": price, "currency": "USD"}
                    )
            except Exception:
                continue
    finally:
        os.unlink(tmp_path)

    if not observations:
        raise ValueError("No valid data returned from World Bank")

    df = pd.DataFrame(observations)
    df = df.sort_values("date").reset_index(drop=True)

    print(f"Fetched {len(df)} monthly data points from World Bank")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_from_website(source_name, base_url="https://freegoldapi.com"):
    """Try to fetch backfill data from the published website first."""
    url = f"{base_url}/data/backfill/{source_name}_latest.csv"
    print(f"Attempting to fetch {source_name} from website: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        df["date"] = pd.to_datetime(df["date"]).dt.date

        print(f"Successfully fetched {len(df)} records from website")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        return df
    except Exception as e:
        print(f"Could not fetch from website: {e}")
        return None


def save_backfill_data(df, source_name, output_dir="data/backfill"):
    """Save backfill data to a source-specific file."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"{source_name}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} records to {filepath}")

    latest_path = os.path.join(output_dir, f"{source_name}_latest.csv")
    df.to_csv(latest_path, index=False)
    print(f"Also saved to {latest_path}")


def load_backfill_data(backfill_dir="data/backfill"):
    """Load all backfill data files in priority order."""
    if not os.path.exists(backfill_dir):
        print(f"Backfill directory {backfill_dir} not found")
        return []

    priority_order = [
        "measuringworth_british",
        "measuringworth_london",
        "worldbank",
        "yahoo_finance",
    ]

    dataframes = []
    for source_name in priority_order:
        filepath = os.path.join(backfill_dir, f"{source_name}_latest.csv")
        if os.path.exists(filepath):
            try:
                print(f"Loading backfill data from {source_name}...")
                df = pd.read_csv(filepath)
                from datetime import datetime as dt

                df["date"] = df["date"].apply(
                    lambda x: dt.strptime(x, "%Y-%m-%d").date()
                )
                df["source"] = source_name
                print(
                    f"  Loaded {len(df)} records (range: {df['date'].min()} to {df['date'].max()})"
                )
                dataframes.append(df)
            except Exception as e:
                print(f"Error loading {filepath}: {e}")

    return dataframes


def merge_dataframes(dataframes, strategy="prefer_higher_granularity"):
    """Merge multiple dataframes with different granularities."""
    if not dataframes:
        return pd.DataFrame(columns=["date", "price"]), {}, {}

    df_combined = pd.concat(dataframes, ignore_index=True)

    full_source_ranges = {}
    if "source" in df_combined.columns:
        for source in df_combined["source"].unique():
            if pd.notna(source):
                source_df = df_combined[df_combined["source"] == source]
                full_source_ranges[source] = {
                    "count": len(source_df),
                    "start": str(source_df["date"].min()),
                    "end": str(source_df["date"].max()),
                }

    if strategy == "prefer_higher_granularity":
        df_combined = df_combined.drop_duplicates(subset=["date"], keep="last")

    df_combined = df_combined.sort_values("date").reset_index(drop=True)

    source_stats = {}
    if "source" in df_combined.columns:
        for source in df_combined["source"].unique():
            if pd.notna(source):
                source_df = df_combined[df_combined["source"] == source]
                source_stats[source] = {
                    "count": len(source_df),
                    "start": str(source_df["date"].min()),
                    "end": str(source_df["date"].max()),
                }

    return df_combined, source_stats, full_source_ranges


def save_csv(df, source_stats=None, output_dir="data"):
    """Save DataFrame to CSV with timestamp in filename."""
    os.makedirs(output_dir, exist_ok=True)

    columns_to_save = ["date", "price"]
    if "currency" in df.columns:
        columns_to_save.append("currency")
    df_output = df[columns_to_save].copy()

    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"gold_spot_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    df_output.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")

    latest_path = os.path.join(output_dir, "latest.csv")
    df_output.to_csv(latest_path, index=False)
    print(f"Data also saved to {latest_path}")

    if source_stats:
        stats_path = os.path.join(output_dir, "source_stats.json")
        with open(stats_path, "w") as f:
            json.dump(source_stats, f, indent=2)
        print(f"Source statistics saved to {stats_path}")

    return filename


def main():
    """Main execution function."""
    config = load_config()

    print("=== Starting historical data backfill ===\n")

    # Fetch data from all enabled sources
    backfill_sources = config.get("backfill_sources", {})
    for source_name, source_config in backfill_sources.items():
        if not source_config.get("enabled", False):
            print(f"Skipping {source_name} (disabled)")
            continue

        print(f"\n--- Fetching from {source_name} ---")

        # Only use website fallback for historical sources that don't change
        # Always fetch fresh data for Yahoo Finance and World Bank
        use_website_fallback = source_name.startswith("measuringworth")

        df = None
        if use_website_fallback:
            # Try to fetch from website first for historical data
            df = fetch_from_website(source_name)

        # If website fetch failed or skipped, fetch from original source
        if df is None:
            if use_website_fallback:
                print(f"Falling back to normal backfill for {source_name}...")
            if source_name.startswith("measuringworth"):
                series = source_config.get("series", "london")
                df = fetch_measuringworth_data(series=series)
            elif source_name == "yahoo_finance":
                df = fetch_yahoo_finance_data()
            elif source_name == "worldbank":
                df = fetch_worldbank_data()

        if df is not None and not df.empty:
            save_backfill_data(df, source_name)
        else:
            print(f"No data fetched from {source_name}")

    print("\n=== Backfill complete ===")

    # Merge all data
    print("\n=== Starting data merge ===\n")

    dataframes = load_backfill_data()

    if not dataframes:
        print("ERROR: No data sources available to merge")
        sys.exit(1)

    print("\n--- Merging all data sources ---")
    merge_strategy = config.get("merge_strategy", {}).get(
        "prefer_higher_granularity", True
    )
    strategy = "prefer_higher_granularity" if merge_strategy else "keep_all"
    df_merged, source_stats, full_source_ranges = merge_dataframes(dataframes, strategy)

    print(f"\n=== Merge Summary ===")
    print(f"Total records: {len(df_merged)}")
    if not df_merged.empty:
        print(f"Date range: {df_merged['date'].min()} to {df_merged['date'].max()}")

    if source_stats:
        print("\n=== Source Statistics (Used in Final Dataset) ===")
        for source, stats in source_stats.items():
            print(
                f"{source}: {stats['count']:,} records ({stats['start']} to {stats['end']})"
            )

    if full_source_ranges:
        print("\n=== Full Source Coverage (Before Merge) ===")
        for source, stats in full_source_ranges.items():
            print(
                f"{source}: {stats['count']:,} records ({stats['start']} to {stats['end']})"
            )

    filename = save_csv(df_merged, source_stats)

    if full_source_ranges:
        full_ranges_path = "data/source_ranges_full.json"
        with open(full_ranges_path, "w") as f:
            json.dump(full_source_ranges, f, indent=2)
        print(f"Full source ranges saved to {full_ranges_path}")

    print("\n=== Complete ===")


if __name__ == "__main__":
    main()
