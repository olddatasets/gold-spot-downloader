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


def fetch_dollar_pound_exchange_rate(start_year=None, end_year=None):
    """Fetch historical USD/GBP exchange rate from MeasuringWorth.com."""
    if start_year is None:
        start_year = 1791
    if end_year is None:
        end_year = datetime.now().year

    base_url = "https://www.measuringworth.com/datasets/exchangepound/export.php"
    params = {"year_source": start_year, "year_result": end_year, "exchangepound": "on"}
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    print(f"Fetching dollar-pound exchange rate from MeasuringWorth ({start_year}-{end_year})...")

    # Find the header row dynamically
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    lines = response.text.split('\n')
    header_row_idx = None
    for i, line in enumerate(lines):
        if '"Year"' in line:
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Could not find header row in CSV")

    # Read CSV starting from header row
    from io import StringIO
    df = pd.read_csv(StringIO(response.text), skiprows=header_row_idx)

    # The CSV has headers: Year, Unit, Rate
    df.columns = ["year", "unit", "rate"]

    df["rate"] = pd.to_numeric(
        df["rate"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year", "rate"])

    df["date"] = df["year"].astype(int).apply(lambda y: date(y, 1, 1))
    df = df[["date", "rate"]]
    df.columns = ["date", "usd_per_gbp"]

    print(f"Fetched {len(df)} annual data points from MeasuringWorth")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    return df


def fetch_gold_silver_ratio(start_year=None, end_year=None):
    """Fetch historical gold/silver price ratio from MeasuringWorth.com."""
    if start_year is None:
        start_year = 1687
    if end_year is None:
        end_year = datetime.now().year

    base_url = "https://www.measuringworth.com/datasets/gold/export.php"
    params = {"year_source": start_year, "year_result": end_year, "goldsilver": "on"}
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"

    print(f"Fetching gold/silver ratio from MeasuringWorth ({start_year}-{end_year})...")

    # Find the header row dynamically
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    lines = response.text.split('\n')
    header_row_idx = None
    for i, line in enumerate(lines):
        if '"Year"' in line:
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError("Could not find header row in CSV")

    # Read CSV starting from header row
    from io import StringIO
    df = pd.read_csv(StringIO(response.text), skiprows=header_row_idx)

    # The CSV has headers: Year, Gold/Silver Price Ratio (ounces of silver per ounce of gold)
    # Rename columns for easier access
    df.columns = ["year", "ratio"]

    df["ratio"] = pd.to_numeric(
        df["ratio"].astype(str).str.replace(",", ""), errors="coerce"
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year", "ratio"])

    df["date"] = df["year"].astype(int).apply(lambda y: date(y, 1, 1))
    df = df[["date", "ratio"]]
    df.columns = ["date", "gold_silver_ratio"]

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


def fetch_yahoo_silver_data(start_date=None, end_date=None):
    """Fetch silver futures price data from Yahoo Finance (SI=F ticker)."""
    if start_date is None:
        start_date = date(2025, 1, 1)
    if end_date is None:
        end_date = datetime.now().date()

    print(f"Fetching silver futures from Yahoo Finance ({start_date} to {end_date})...")
    ticker = "SI=F"
    silver_data = yf.download(
        ticker,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        progress=False,
    )

    if silver_data.empty:
        raise ValueError("No data returned from Yahoo Finance for silver")

    silver_data = silver_data.reset_index()
    dates = silver_data["Date"].dt.date.tolist()

    if "Close" in silver_data.columns:
        prices = silver_data["Close"].values.flatten()
    else:
        raise ValueError("Close price column not found in Yahoo Finance silver data")

    df = pd.DataFrame({"date": dates, "price": prices})
    df = df.dropna()
    df.columns = ["date", "silver_price"]

    print(f"Fetched {len(df)} daily data points from Yahoo Finance (silver)")
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


def normalize_gold_prices_to_usd(df_gold, df_exchange):
    """Normalize gold prices to USD using exchange rate data.

    Args:
        df_gold: DataFrame with columns [date, price, currency, source]
        df_exchange: DataFrame with columns [date, usd_per_gbp]

    Returns:
        DataFrame with normalized prices in USD and updated source annotations
    """
    df = df_gold.copy()

    # Merge exchange rate data
    df = df.merge(df_exchange, on="date", how="left")

    # Convert GBP prices to USD
    # For rows where currency is GBP and we have exchange rate data
    mask = (df["currency"] == "GBP") & df["usd_per_gbp"].notna()
    df.loc[mask, "price"] = df.loc[mask, "price"] * df.loc[mask, "usd_per_gbp"]
    df.loc[mask, "currency"] = "USD"

    # Update source column to indicate normalization
    if "source" in df.columns:
        df.loc[mask, "source"] = df.loc[mask, "source"] + " (GBP->USD)"
        # Mark rows that remain in GBP
        gbp_mask = df["currency"] == "GBP"
        df.loc[gbp_mask, "source"] = df.loc[gbp_mask, "source"] + " (GBP)"

    # Drop the exchange rate column
    df = df.drop(columns=["usd_per_gbp"])

    print(f"Normalized {mask.sum()} GBP prices to USD")
    return df


def convert_gold_to_silver_ounces(df_gold, df_ratio):
    """Convert gold prices to silver ounce equivalents using gold/silver ratio.

    Args:
        df_gold: DataFrame with columns [date, price, currency]
        df_ratio: DataFrame with columns [date, gold_silver_ratio]

    Returns:
        DataFrame with gold price expressed in ounces of silver
    """
    df = df_gold.copy()

    # Merge gold/silver ratio data
    df = df.merge(df_ratio, on="date", how="left")

    # For rows with ratio data, calculate silver equivalent
    # If gold costs $X and ratio is R (oz silver per oz gold),
    # then 1 oz gold = R oz silver, so price in silver = price_usd / R?
    # Actually, the ratio tells us how many oz of silver = 1 oz of gold
    # So if we want to express the USD price in terms of silver ounces:
    # We need silver price, but we only have the ratio
    #
    # Better approach: express as "oz of silver per oz of gold" directly from ratio
    mask = df["gold_silver_ratio"].notna()
    df.loc[mask, "silver_oz_per_gold_oz"] = df.loc[mask, "gold_silver_ratio"]

    # Drop intermediate columns
    df = df.drop(columns=["gold_silver_ratio"])

    print(f"Added silver ratio for {mask.sum()} records")
    return df


def normalize_gold_prices_to_silver(df_gold, df_ratio):
    """Normalize gold prices to silver ounces instead of currency.

    This creates an alternative view where the "price" of gold is expressed
    in ounces of silver, providing a metallic standard that transcends
    fiat currency fluctuations.

    The gold/silver ratio data is typically annual (Jan 1 each year) from
    MeasuringWorth, and daily from Yahoo Finance for 2025+. Annual ratios
    are applied to all dates within the same year.

    Args:
        df_gold: DataFrame with columns [date, price, currency, source]
        df_ratio: DataFrame with columns [date, gold_silver_ratio]

    Returns:
        DataFrame with columns [date, price, source] where price
        represents how many ounces of silver equal one ounce of gold
    """
    df = df_gold.copy()

    # Add year column for merging
    df["year"] = df["date"].apply(lambda d: d.year)

    # Create ratio lookup by year
    df_ratio_yearly = df_ratio.copy()
    df_ratio_yearly["year"] = df_ratio_yearly["date"].apply(lambda d: d.year)

    # For each year, keep the ratio (prefer latest if multiple exist)
    df_ratio_yearly = df_ratio_yearly.sort_values("date").drop_duplicates(subset=["year"], keep="last")
    df_ratio_yearly = df_ratio_yearly[["year", "gold_silver_ratio"]]

    # Merge gold/silver ratio data by year
    df = df.merge(df_ratio_yearly, on="year", how="left")

    # The gold/silver ratio directly tells us how many oz of silver = 1 oz of gold
    mask = df["gold_silver_ratio"].notna()

    # Create silver-normalized column
    df.loc[mask, "silver_oz"] = df.loc[mask, "gold_silver_ratio"]

    # Update source annotations
    if "source" in df.columns:
        df.loc[mask, "source"] = df.loc[mask, "source"].astype(str) + " (silver-normalized)"

    # Drop intermediate columns and currency (since we're now in silver)
    cols_to_drop = ["gold_silver_ratio", "price", "year"]
    if "currency" in df.columns:
        cols_to_drop.append("currency")
    df = df.drop(columns=cols_to_drop)

    # Rename for clarity
    df = df.rename(columns={"silver_oz": "price"})

    # Only keep records with ratio data
    df = df[df["price"].notna()].copy()

    print(f"Silver-normalized {len(df)} records (applied {len(df_ratio_yearly)} annual ratios to all dates within each year)")
    return df


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

    # Always save date, price, and source columns (USD-normalized)
    columns_to_save = ["date", "price"]
    if "source" in df.columns:
        columns_to_save.append("source")
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

        # Only Yahoo Finance sources should always fetch fresh
        # Everything else (MeasuringWorth, World Bank, etc.) can use cache
        use_website_fallback = not source_name.startswith("yahoo_")
        always_fetch_fresh = source_name in ["yahoo_finance", "yahoo_silver"]

        df = None
        if use_website_fallback:
            # Try to fetch from website first for cacheable data
            df = fetch_from_website(source_name)

        # If website fetch failed or skipped, fetch from original source
        # For yahoo_finance and yahoo_silver, ALWAYS fetch fresh (never use cache)
        if df is None or always_fetch_fresh:
            if use_website_fallback and df is None:
                print(f"Falling back to normal backfill for {source_name}...")
            if always_fetch_fresh and df is not None:
                print(f"Fetching fresh data for {source_name} (ignoring cached version)...")

            if source_name.startswith("measuringworth"):
                series = source_config.get("series", "london")
                df = fetch_measuringworth_data(series=series)
            elif source_name == "yahoo_finance":
                df = fetch_yahoo_finance_data()
            elif source_name == "yahoo_silver":
                df = fetch_yahoo_silver_data()
            elif source_name == "worldbank":
                df = fetch_worldbank_data()
            elif source_name == "dollar_pound_exchange":
                df = fetch_dollar_pound_exchange_rate()
            elif source_name == "gold_silver_ratio":
                df = fetch_gold_silver_ratio()

        if df is not None and not df.empty:
            save_backfill_data(df, source_name)
        else:
            print(f"No data fetched from {source_name}")

    print("\n=== Backfill complete ===")

    # Calculate current gold/silver ratios from Yahoo Finance data
    print("\n=== Calculating current gold/silver ratios ===\n")
    gold_yf_path = os.path.join("data/backfill", "yahoo_finance_latest.csv")
    silver_yf_path = os.path.join("data/backfill", "yahoo_silver_latest.csv")
    ratio_mw_path = os.path.join("data/backfill", "gold_silver_ratio_latest.csv")

    if os.path.exists(gold_yf_path) and os.path.exists(silver_yf_path):
        print("Loading gold and silver prices from Yahoo Finance...")
        df_gold_yf = pd.read_csv(gold_yf_path)
        df_silver_yf = pd.read_csv(silver_yf_path)

        from datetime import datetime as dt
        df_gold_yf["date"] = df_gold_yf["date"].apply(
            lambda x: dt.strptime(x, "%Y-%m-%d").date()
        )
        df_silver_yf["date"] = df_silver_yf["date"].apply(
            lambda x: dt.strptime(x, "%Y-%m-%d").date()
        )

        # Merge and calculate ratio
        df_merged_yf = df_gold_yf[["date", "price"]].merge(
            df_silver_yf[["date", "silver_price"]], on="date", how="inner"
        )
        df_merged_yf["gold_silver_ratio"] = df_merged_yf["price"] / df_merged_yf["silver_price"]

        # Keep only date and ratio
        df_calc_ratio = df_merged_yf[["date", "gold_silver_ratio"]].copy()

        print(f"Calculated {len(df_calc_ratio)} daily gold/silver ratios from Yahoo Finance")
        print(f"Date range: {df_calc_ratio['date'].min()} to {df_calc_ratio['date'].max()}")
        print(f"Average ratio: {df_calc_ratio['gold_silver_ratio'].mean():.2f}")

        # Merge with historical MeasuringWorth ratios
        if os.path.exists(ratio_mw_path):
            print("\nMerging with historical MeasuringWorth ratios...")
            df_ratio_mw = pd.read_csv(ratio_mw_path)
            df_ratio_mw["date"] = df_ratio_mw["date"].apply(
                lambda x: dt.strptime(x, "%Y-%m-%d").date()
            )

            # Combine: prefer calculated ratios for overlapping dates
            df_combined_ratio = pd.concat([df_ratio_mw, df_calc_ratio], ignore_index=True)
            df_combined_ratio = df_combined_ratio.drop_duplicates(subset=["date"], keep="last")
            df_combined_ratio = df_combined_ratio.sort_values("date").reset_index(drop=True)

            # Save combined ratio dataset
            combined_ratio_path = os.path.join("data/backfill", "gold_silver_ratio_combined.csv")
            df_combined_ratio.to_csv(combined_ratio_path, index=False)
            print(f"Combined ratio dataset saved to {combined_ratio_path}")
            print(f"Total records: {len(df_combined_ratio)} (historical + calculated)")
            print(f"Full date range: {df_combined_ratio['date'].min()} to {df_combined_ratio['date'].max()}")

            # Also update the latest.csv to point to combined
            df_combined_ratio.to_csv(ratio_mw_path, index=False)
            print(f"Updated {ratio_mw_path} with combined ratios")
        else:
            print("Historical ratio data not found, using only calculated ratios")
    else:
        print("Yahoo Finance gold or silver data not available, skipping ratio calculation")

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

    # Load exchange rate and ratio data for normalization
    print("\n=== Creating normalized datasets ===\n")
    exchange_path = os.path.join("data/backfill", "dollar_pound_exchange_latest.csv")
    ratio_path = os.path.join("data/backfill", "gold_silver_ratio_latest.csv")

    # Normalize to USD first, then save as latest.csv
    if os.path.exists(exchange_path):
        print("Loading exchange rate data...")
        df_exchange = pd.read_csv(exchange_path)
        from datetime import datetime as dt
        df_exchange["date"] = df_exchange["date"].apply(
            lambda x: dt.strptime(x, "%Y-%m-%d").date()
        )

        print("Creating USD-normalized dataset...")
        df_usd = normalize_gold_prices_to_usd(df_merged, df_exchange)

        # Count how many GBP prices remain (those before 1791)
        gbp_remaining = len(df_usd[df_usd["currency"] == "GBP"])
        if gbp_remaining > 0:
            print(f"Note: {gbp_remaining} prices remain in GBP (pre-1791, no exchange rate available)")
    else:
        print("Exchange rate data not found, using prices as-is")
        df_usd = df_merged

    # Save USD-normalized data as latest.csv (primary dataset)
    # This contains ONLY date and price columns
    print("\nSaving primary dataset (latest.csv)...")
    filename = save_csv(df_usd, source_stats)

    # Also save a copy with the old name for backwards compatibility
    usd_path = os.path.join("data", "gold_usd_normalized.csv")
    df_usd[["date", "price"]].to_csv(usd_path, index=False)
    print(f"Backwards-compatible copy saved to {usd_path}")

    if os.path.exists(ratio_path):
        print("\nLoading gold/silver ratio data...")
        df_ratio = pd.read_csv(ratio_path)
        from datetime import datetime as dt
        df_ratio["date"] = df_ratio["date"].apply(
            lambda x: dt.strptime(x, "%Y-%m-%d").date()
        )

        print("Creating silver-denominated dataset...")
        df_silver = convert_gold_to_silver_ounces(df_usd, df_ratio)

        # Save silver-denominated data
        silver_path = os.path.join("data", "gold_silver_ratio_enriched.csv")
        cols_to_save = ["date", "price"]
        if "currency" in df_silver.columns:
            cols_to_save.append("currency")
        if "silver_oz_per_gold_oz" in df_silver.columns:
            cols_to_save.append("silver_oz_per_gold_oz")
        df_silver[cols_to_save].to_csv(silver_path, index=False)
        print(f"Silver-enriched data saved to {silver_path}")

        # Count records with silver ratio
        ratio_count = df_silver["silver_oz_per_gold_oz"].notna().sum()
        print(f"Added silver ratio for {ratio_count} records")

        # Create silver-normalized dataset (alternative view)
        print("\nCreating silver-normalized dataset (metallic standard)...")
        df_silver_norm = normalize_gold_prices_to_silver(df_merged, df_ratio)

        # Save silver-normalized data
        silver_norm_path = os.path.join("data", "gold_silver_normalized.csv")
        cols_to_save_norm = ["date", "price"]
        if "source" in df_silver_norm.columns:
            cols_to_save_norm.append("source")
        df_silver_norm[cols_to_save_norm].to_csv(silver_norm_path, index=False)
        print(f"Silver-normalized data saved to {silver_norm_path}")
        print(f"This dataset expresses gold value in ounces of silver (transcends fiat currency)")
    else:
        print("\nGold/silver ratio data not found, skipping silver conversion")

    if full_source_ranges:
        full_ranges_path = "data/source_ranges_full.json"
        with open(full_ranges_path, "w") as f:
            json.dump(full_source_ranges, f, indent=2)
        print(f"\nFull source ranges saved to {full_ranges_path}")

    # Save generation metadata
    print("\n=== Saving generation metadata ===")
    metadata = {
        "generated_at": datetime.now().isoformat(),
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "total_records": len(df_merged),
        "date_range": {
            "earliest": str(df_merged['date'].min()) if not df_merged.empty else None,
            "latest": str(df_merged['date'].max()) if not df_merged.empty else None
        },
        "most_recent_data": {
            "yahoo_finance_gold": {
                "latest_date": str(df_merged[df_merged['source'] == 'yahoo_finance']['date'].max()) if 'yahoo_finance' in df_merged['source'].values else None,
                "latest_price": float(df_merged[df_merged['source'] == 'yahoo_finance']['price'].iloc[-1]) if 'yahoo_finance' in df_merged['source'].values else None
            } if 'source' in df_merged.columns else None
        },
        "source_statistics": source_stats,
        "datasets_generated": {
            "usd_normalized": "data/latest.csv",
            "usd_normalized_legacy": "data/gold_usd_normalized.csv",
            "silver_enriched": "data/gold_silver_ratio_enriched.csv",
            "silver_normalized": "data/gold_silver_normalized.csv"
        }
    }

    # Add silver ratio metadata if available
    if os.path.exists(ratio_path):
        df_ratio_check = pd.read_csv(ratio_path)
        if not df_ratio_check.empty:
            metadata["most_recent_data"]["gold_silver_ratio"] = {
                "latest_date": df_ratio_check['date'].max(),
                "latest_ratio": float(df_ratio_check['gold_silver_ratio'].iloc[-1]),
                "source": "calculated from Yahoo Finance" if df_ratio_check['date'].max() >= "2025-01-01" else "MeasuringWorth"
            }

    metadata_path = "data/generation_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Generation metadata saved to {metadata_path}")
    print(f"Run completed at: {metadata['generated_at']}")

    print("\n=== Complete ===")


if __name__ == "__main__":
    main()
