# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Python utility that fetches gold spot price data from multiple sources and maintains a comprehensive historical dataset. The system uses a **three-script architecture** to separate concerns: backfilling historical data, fetching latest data, and merging all sources. It's designed to run on different schedules (backfill rarely, latest daily) via GitHub Actions and publishes to GitHub Pages.

## Architecture

**Three-script design**: The application is split into three independent scripts with clear responsibilities:

### 1. **backfill_gold_data.py** - Historical Data Collection
Fetches comprehensive historical data from sources with good long-term coverage. Run infrequently (weekly/monthly).

**Data sources**:
- **MeasuringWorth** (annual, 1718-2023)
  - Default: London Market Price
  - Available series: london, us, british (back to 1257!), newyork, goldsilver
  - CSV export API, requires proper attribution
- **DataHub.io** (monthly, 1833-present)
  - Direct CSV download, no API key
- **Wikipedia** (5-year intervals, 1970-2010)
  - Disabled by default, scraped from table

**Output**: Saves to `data/backfill/{source}_latest.csv`

### 2. **fetch_latest_gold_price.py** - Current Price Updates
Fetches recent/current prices from MetalpriceAPI. Run daily.

**Data source**:
- **MetalpriceAPI** (daily, last 365 days + today)
  - Requires `METALPRICE_API_KEY` environment variable
  - Free tier: 100 requests/month
  - Inverts rates (`1 / rate`) because API returns "USD per 1 XAU"

**Output**: Saves to `data/latest/metalpriceapi_latest.csv`

### 3. **merge_gold_data.py** - Data Combination
Combines all data sources into final dataset with proper priority ordering. Run after backfill or latest updates.

**Merge priority** (first = highest):
1. Latest data (daily from MetalpriceAPI)
2. Backfill data (monthly from DataHub, annual from MeasuringWorth)
3. Existing published data (fallback from GitHub Pages)

**Output**:
- `data/gold_spot_YYYYMMDD.csv` (timestamped)
- `data/latest.csv` (always current)
- `index.html` (with proper source attribution and link to `sources.html`)

**Key behavior**: Merge script loads from published website first (`https://freeprice.gold/data/latest.csv`), ensuring fresh CI environments get complete history.

### Data Granularity & Coverage

The merge strategy prioritizes higher granularity data: **daily > monthly > annual > 5-year intervals**

- **1718-1832**: Annual (MeasuringWorth)
- **1833-present**: Monthly (DataHub) + Annual (MeasuringWorth)
- **Last 365 days**: Daily (MetalpriceAPI) overrides older sources
- **Today**: Current price from MetalpriceAPI

See `sources.html` for interactive visualization of data sources and timeline.

## Configuration

Edit `config.json` to control which sources are used by each script:

```json
{
  "backfill_sources": {
    "measuringworth": {
      "enabled": true,
      "series": "london",
      "granularity": "annual"
    },
    "datahub": {
      "enabled": true,
      "granularity": "monthly"
    }
  },
  "latest_source": {
    "metalpriceapi": {
      "enabled": true,
      "granularity": "daily"
    }
  },
  "merge_strategy": {
    "prefer_higher_granularity": true,
    "priority_order": ["latest", "backfill", "existing"]
  }
}
```

**MeasuringWorth series options**: Change the `series` field in backfill_sources to select different datasets:
- `"london"`: London Market Price (1718-2023) - **default**
- `"british"`: British Official Price (1257-1945) - **oldest available!**
- `"us"`: U.S. Official Price (1786-2020)
- `"newyork"`: New York Market Price (1791-present)
- `"goldsilver"`: Gold/Silver Ratio (1687-present)

## Running the Scripts

**Setup**:
```bash
pip install -r requirements.txt
export METALPRICE_API_KEY="your-api-key-here"  # Only needed for fetch_latest_gold_price.py
```

**1. Run backfill** (infrequent - weekly/monthly):
```bash
python backfill_gold_data.py
```
Fetches historical data from MeasuringWorth, DataHub, etc. No API key required.

**2. Fetch latest prices** (daily):
```bash
python fetch_latest_gold_price.py
```
Requires `METALPRICE_API_KEY` environment variable.

**3. Merge all data**:
```bash
python merge_gold_data.py
```
Combines backfill + latest + existing data into final dataset. No API key required.

**Full workflow**:
```bash
# One-time or occasional
python backfill_gold_data.py

# Daily
python fetch_latest_gold_price.py
python merge_gold_data.py
```

## Data Format

CSV files contain two columns:
- `date`: Date in YYYY-MM-DD format
- `price`: Gold price in USD per troy ounce

## Published Output

The repository is intended to be published via GitHub Pages with:
- **`index.html`**: Landing page with data source attribution and links
  - Auto-redirects to `data/latest.csv`
  - Links to `sources.html` visualization
  - Includes proper MeasuringWorth citation
- **`sources.html`**: Interactive timeline visualization showing data sources and granularity
  - Chart.js-based timeline of all data sources
  - Color-coded by source
  - Shows date ranges and granularity for each source
- **`data/latest.csv`**: Always points to the most recent complete dataset
- **`data/gold_spot_YYYYMMDD.csv`**: Date-stamped copies for historical reference
- **`data/backfill/`**: Individual source data files for debugging
- **`data/latest/`**: Latest data from MetalpriceAPI

Link structure assumes the site is published at `https://freeprice.gold/`.
