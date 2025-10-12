# freeprice.gold

**Free, comprehensive historical gold price data spanning 768 years (1258-2025)**

[![Update Gold Price Data](https://github.com/posix4e/gold-spot-downloader/actions/workflows/update-data.yml/badge.svg)](https://github.com/posix4e/gold-spot-downloader/actions/workflows/update-data.yml)

📊 **Live site:** [freeprice.gold](https://freeprice.gold) (or [posix4e.github.io/gold-spot-downloader](https://posix4e.github.io/gold-spot-downloader))

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
- **CSV:** [data/latest.csv](https://freeprice.gold/data/latest.csv)
- **Interactive timeline:** [sources.html](https://freeprice.gold/sources.html)

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

The system uses a **three-script architecture**:

### 1. `backfill_gold_data.py`
Fetches historical data from sources with good long-term coverage. Run infrequently (weekly/monthly).

```bash
python backfill_gold_data.py
```

**Sources:**
- MeasuringWorth (annual, 1258-2024)
- World Bank (monthly, 1960-present)
- Yahoo Finance (daily, 2025-present)

### 2. `merge_gold_data.py`
Combines all data sources with proper priority ordering.

```bash
python merge_gold_data.py
```

**Merge Strategy:**
- Higher granularity data overrides lower granularity for overlapping dates
- Priority: Daily > Monthly > Annual
- Generates `data/latest.csv` and timestamped snapshots

### 3. GitHub Actions (automated)
Runs daily at 6 AM UTC to fetch and publish updated data.

## Local Development

```bash
# Clone repository
git clone https://github.com/posix4e/gold-spot-downloader.git
cd gold-spot-downloader

# Install dependencies
pip install -r requirements.txt

# Fetch historical data
python backfill_gold_data.py

# Merge data sources
python merge_gold_data.py

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
