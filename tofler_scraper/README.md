# Tofler Scraper

A scalable web scraper for extracting directorship data from Tofler.in for up to 10,000 companies.

## Features

- ✅ Browser automation with Playwright
- ✅ 10 concurrent browser instances
- ✅ Proxy support (Decodo ISP)
- ✅ Anti-detection measures
- ✅ Resume capability after crashes
- ✅ Real-time progress tracking
- ✅ Incremental Excel saves
- ✅ Comprehensive logging

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Install Playwright browsers:
```bash
playwright install chromium
```

## Configuration

Edit `config.py` to adjust:
- Browser settings (headless mode, timeout)
- Delay ranges
- Proxy credentials
- File paths
- Selectors (if Tofler changes HTML structure)

## Usage

### Input Excel Format

Create an Excel file `companies.xlsx` with a sheet named `companies`:

| id | name |
|----|------|
| 001 | J S AUTO CAST FOUNDRY INDIA PRIVATE LIMITED |
| 002 | AUDIOBLYSS PRIVATE LIMITED |

### Run Scraper

**First run:**
```bash
python main.py
```

**Resume after interruption:**
```bash
python main.py --resume
```

### Output

The scraper will modify your input Excel file by adding/updating:

1. **`directors` sheet** (NEW): Contains directorship data
2. **`management` sheet** (EXISTING): Appends management data

### Logs

Check these log files in the `logs/` directory:
- `scraper.log` - Detailed debug log
- `companies_not_found.log` - Companies not found on Tofler
- `failed_companies.log` - Companies that failed after 2 retries

## Performance

- **Concurrent browsers**: 10
- **Expected rate**: 6-10 companies/minute
- **10,000 companies**: 16-28 hours

## Troubleshooting

**If you encounter blocking/CAPTCHAs:**
1. Increase delays in `config.py`
2. Reduce `MAX_CONCURRENT_BROWSERS`
3. Set `HEADLESS = False` to debug

**If selectors stop working:**
1. Update `SELECTORS` in `config.py`
2. Test with a single company first

**For memory issues:**
1. Reduce `MAX_CONCURRENT_BROWSERS`
2. Decrease `SAVE_THRESHOLD`

## Project Structure

```
tofler-scraper/
├── main.py                  # Entry point
├── config.py                # Configuration
├── requirements.txt         # Dependencies
├── scraper/
│   ├── browser_manager.py   # Browser pool
│   ├── tofler_scraper.py    # Core scraping logic
│   └── data_processor.py    # Data processing
├── utils/
│   ├── excel_handler.py     # Excel I/O
│   ├── logger.py            # Logging
│   └── progress.py          # Progress tracking
├── logs/                    # Log files
└── output/                  # Output files
```

## License

MIT
