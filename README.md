# freegoldapi.com

**Free, comprehensive historical gold price data spanning 768 years (1258-2025)**

[![Update Gold Price Data](https://github.com/posix4e/gold-spot-downloader/actions/workflows/update-data.yml/badge.svg)](https://github.com/posix4e/gold-spot-downloader/actions/workflows/update-data.yml)

📊 **Live site:** [freegoldapi.com](https://freegoldapi.com)

## Dataset Overview

**Total: 1,678 records** with increasing granularity over time:

| Source | Period | Records | Granularity |
|--------|--------|---------|-------------|
| MeasuringWorth British Official Price | 1258-1717 | 460 | Annual |
| MeasuringWorth London Market Price | 1718-1959 | 242 | Annual |
| World Bank Commodity Prices | 1960-2024 | 780 | Monthly |
| Yahoo Finance Gold Futures (GC=F) | 2025-present | 196 | Daily |

## Quick Start

**Download the latest data:**
- **CSV:** [data/latest.csv](https://freegoldapi.com/data/latest.csv)

**CSV Format:**
```csv
date,price,currency
1258-01-01,0.89,GBP
1259-01-01,0.89,GBP
...
2025-10-10,4000.40,USD
```

## Features

- 📈 **768 years of data** from medieval Britain to modern futures markets
- 🔄 **Automatically updated** daily via GitHub Actions
- 📊 **Interactive timeline visualization** showing data source coverage
- 💾 **Raw data available** - download individual source files
- 🆓 **Completely free** - no API keys required for basic dataset
- 📜 **Proper attribution** - full citations for all data sources

## Data Sources & Attribution

### British Official Price (1258-1717)
- **Source:** [MeasuringWorth](https://www.measuringworth.com/datasets/gold/)
- **Citation:** Lawrence H. Officer and Samuel H. Williamson, 'The Price of Gold, 1257-1945,' MeasuringWorth, 2025
- **Granularity:** Annual
- **Currency:** GBP

### London Market Price (1718-1959)
- **Source:** [MeasuringWorth](https://www.measuringworth.com/datasets/gold/)
- **Citation:** Lawrence H. Officer and Samuel H. Williamson, 'The Price of Gold, 1718-2024,' MeasuringWorth, 2025
- **Granularity:** Annual
- **Currency:** GBP (until 1949), USD (1950+)

### World Bank Commodity Prices (1960-2024)
- **Source:** [World Bank Pink Sheet](https://www.worldbank.org/en/research/commodity-markets)
- **Granularity:** Monthly
- **Currency:** USD per troy ounce

### Yahoo Finance (2025-present)
- **Source:** [Gold Futures (GC=F)](https://finance.yahoo.com/quote/GC=F)
- **Granularity:** Daily
- **Currency:** USD per troy ounce

## Architecture

The system uses a **single unified script** that handles everything:

### `update_gold_data.py`

One script to rule them all - fetches, merges, and updates in a single run.

```bash
python update_gold_data.py
```

**What it does:**
1. **Fetches** data from all enabled sources:
   - **MeasuringWorth** (annual, 1258-2024): Tries website first, falls back to API
   - **World Bank** (monthly, 1960-present): Always fetches fresh from API
   - **Yahoo Finance** (daily, 2025-present): Always fetches fresh from API

2. **Merges** with proper priority ordering:
   - Higher granularity data overrides lower granularity
   - Priority: Daily > Monthly > Annual

3. **Generates** output:
   - `data/latest.csv` (always current)
   - `data/gold_spot_YYYYMMDD.csv` (timestamped)
   - `data/source_stats.json` (statistics)
   - `index.html` (updated with latest data)

### GitHub Actions (automated)
Runs daily at 6 AM UTC to fetch and publish updated data.

## Local Development

```bash
# Clone repository
git clone https://github.com/posix4e/gold-spot-downloader.git
cd gold-spot-downloader

# Install dependencies
pip install -r requirements.txt

# Fetch, merge, and update everything
python update_gold_data.py

# View locally
python -m http.server 8000
# Visit http://localhost:8000
```

## Configuration

Edit `config.json` to enable/disable data sources:

```json
{
  "backfill_sources": {
    "measuringworth_british": {
      "enabled": true,
      "series": "British",
      "granularity": "annual"
    },
    "worldbank": {
      "enabled": true,
      "granularity": "monthly"
    },
    "yahoo_finance": {
      "enabled": true,
      "granularity": "daily"
    }
  }
}
```

## GitHub Pages Setup

This repository is configured to deploy to GitHub Pages automatically:

1. **Enable GitHub Pages:**
   - Go to repository Settings → Pages
   - Source: GitHub Actions
   - The workflow will deploy automatically on push

2. **Automatic Updates:**
   - Runs daily at 6 AM UTC
   - Fetches latest data from all sources
   - Rebuilds and deploys site

## Use Cases

- 📊 Financial analysis and modeling
- 📈 Historical trend research
- 💹 Investment strategy backtesting
- 🎓 Educational projects
- 📉 Economic research

## License & Terms

- **Code:** MIT License
- **Data:** See individual source attributions above
- **Usage:** Non-profit educational purposes
- **MeasuringWorth data:** Used with proper attribution per their terms of use

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

Potential improvements:
- Additional data sources
- More granular recent data
- Data quality improvements
- Visualization enhancements

## Support

- 🐛 **Issues:** [GitHub Issues](https://github.com/posix4e/gold-spot-downloader/issues)
- 📧 **Contact:** Open an issue for questions

## Acknowledgments

Special thanks to:
- **MeasuringWorth** for providing comprehensive historical gold price data
- **World Bank** for maintaining the Commodity Price (Pink Sheet) database
- **Yahoo Finance** for current gold futures data

---

**Built with ❤️ for the open data community**

Last updated: 2025-10-12
