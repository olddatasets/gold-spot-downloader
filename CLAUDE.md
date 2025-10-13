# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based web service that aggregates and publishes historical gold price data spanning 768 years (1258-2025). It runs on GitHub Pages at [freegoldapi.com](https://freegoldapi.com) and automatically updates daily via GitHub Actions.

## Key Commands

### Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the main data update script (fetches, merges, and generates all outputs)
python3 update_gold_data.py

# View the website locally
python3 -m http.server 8000
# Then visit http://localhost:8000
```

### Code Quality
```bash
# Format code with Black (configured in .pre-commit-config.yaml)
black update_gold_data.py

# Check Python syntax
python3 -m py_compile update_gold_data.py
```

## Architecture

### Main Script: `update_gold_data.py`

The entire system operates through a single unified script that:

1. **Fetches data** from multiple sources based on `config.json` settings:
   - MeasuringWorth (British Official: 1258-1717, London Market: 1718-1959)
   - World Bank Commodity Prices (1960-2024, monthly)
   - Yahoo Finance Gold/Silver Futures (2025-present, daily)
   - Historical exchange rates and gold/silver ratios for conversions

2. **Merges data** with priority ordering:
   - Higher granularity overrides lower (daily > monthly > annual)
   - Recent data takes precedence over historical
   - Handles currency conversions (GBP to USD normalization)

3. **Generates outputs**:
   - `data/latest.csv` - Main dataset always current
   - `data/gold_spot_YYYYMMDD.csv` - Timestamped backups
   - `data/source_stats.json` - Data source statistics
   - Updates `index.html` with latest data embedded

### Key Functions in `update_gold_data.py`

- `fetch_measuringworth_data()` - Historical gold prices (line 22)
- `fetch_worldbank_data()` - World Bank commodity prices (line 241)
- `fetch_yahoo_finance_data()` - Yahoo Finance gold futures (line 170)
- `merge_dataframes()` - Intelligent data merging by granularity (line 509)
- `normalize_gold_prices_to_usd()` - Currency conversion (line 380)
- `save_csv()` - Output generation with metadata (line 546)
- `main()` - Orchestrates the entire pipeline (line 576)

### Data Flow

1. Script attempts to fetch from website cache first (data/backfill/)
2. Falls back to direct API calls if cache unavailable
3. Merges all sources respecting granularity priorities
4. Normalizes currencies to USD where needed
5. Generates both CSV outputs and updates HTML visualization

## Configuration

The `config.json` file controls:
- Which data sources are enabled/disabled
- Date ranges for each source
- Merge strategy and priority ordering
- Source-specific settings (series names, granularity)

## GitHub Actions

Daily automated updates run at 6 AM UTC via `.github/workflows/update-data.yml`:
- Fetches latest data from all enabled sources
- Regenerates all output files
- Deploys to GitHub Pages automatically

## Data Sources

All data sources are properly attributed:
- **MeasuringWorth**: Historical gold prices and exchange rates
- **World Bank**: Monthly commodity prices (Pink Sheet)
- **Yahoo Finance**: Current gold/silver futures for recent data