# Quick Start Guide - Tofler Scraper

## Step-by-Step Setup

### 1. Install Dependencies

```bash
cd tofler-scraper
pip install -r requirements.txt
playwright install chromium
```

### 2. Prepare Your Excel File

Your Excel file must have:
- A sheet named `companies` with columns: `id`, `name`
- A sheet named `management` with columns: `id`, `company_id`, `name`, `designation`, `contact`

See `example_companies.xlsx` for reference.

### 3. Configure the Scraper

Edit `config.py` if needed:
- Update proxy credentials (if different)
- Adjust `HEADLESS = False` to see browser in action (for testing)
- Modify delays if you encounter blocking

### 4. Run the Scraper

**Test with a few companies first:**
```bash
# Use example file with only 3 companies
cp example_companies.xlsx companies.xlsx
python main.py
```

**Run with your full dataset:**
```bash
# Replace with your actual Excel file
python main.py
```

**Resume after interruption:**
```bash
python main.py --resume
```

## What to Expect

### During Execution:
- Progress bar showing companies/second
- Log messages in console
- Excel file updated incrementally every 100 companies
- `progress.json` updated after each company

### After Completion:
- **Excel file** with two new/updated sheets:
  - `directors`: Directorship data
  - `management`: Management data (appended)
- **Log files** in `logs/` directory:
  - `scraper.log`: Full debug log
  - `companies_not_found.log`: Companies not on Tofler
  - `failed_companies.log`: Failed companies

## Monitoring Progress

### Real-time:
Watch the console for the progress bar and log messages.

### Check Progress File:
```bash
# View current progress
cat progress.json
```

### Check Logs:
```bash
# View not found companies
cat logs/companies_not_found.log

# View failed companies
cat logs/failed_companies.log

# View detailed log (last 50 lines)
tail -n 50 logs/scraper.log
```

## Troubleshooting

### "Module not found" error:
```bash
pip install -r requirements.txt
```

### "Playwright not installed":
```bash
playwright install chromium
```

### Blocking/CAPTCHA issues:
1. Open `config.py`
2. Reduce `MAX_CONCURRENT_BROWSERS` from 10 to 5
3. Increase delays: `GENERAL_DELAY = (5, 12)`

### Check if it's working:
1. Set `HEADLESS = False` in `config.py`
2. Run with 1-2 companies
3. Watch the browser automate

## Performance Tips

### For 10,000 companies:
- Expected time: **16-28 hours**
- Leave computer running overnight
- Resumable if interrupted

### Speed vs Safety:
- **Faster** (risky): Reduce delays, increase browsers
- **Safer** (recommended): Use default settings
- **Slowest** (if blocked): Increase delays, reduce to 3-5 browsers

## Example Output Structure

```
tofler-scraper/
├── companies.xlsx (your input - modified with output)
├── progress.json (resume state)
├── logs/
│   ├── scraper.log
│   ├── companies_not_found.log
│   └── failed_companies.log
└── [other files...]
```

## Next Steps

1. Start with `example_companies.xlsx` (3 companies)
2. Verify output is correct
3. Use your full dataset
4. Monitor progress via logs
5. Review failed companies after completion

## Support

- Check `README.md` for detailed documentation
- Review `detailed.md` for technical details
- Check `tasklist.md` for implementation breakdown

Good luck! 🚀
