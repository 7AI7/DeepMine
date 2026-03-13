# Initial Implementation Plan (Updated Mid-Session)

## Project: Company Enrichment Pipeline

This document serves as the implementation and execution plan for the current scraping pipeline. It outlines the testing and adaptation workflow required to solidify the `GoogleSnippetScraper` as our primary extraction engine.

### Setup Phase
- [x] Read handoff report and all source files
- [x] Set `HEADLESS=False` for headed mode (for debugging and CAPTCHA handling)
- [x] Set `MAX_WORKERS=1` for single isolated test runs during development
- [x] Create test `companies.xlsx` with 1 company ("Pricol Limited")

### Execution Phase
- [x] Run `python main.py` from enrichment_pipeline dir
- [x] Monitor LinkedIn scraper output
  *   *Mid-Session Architectural Pivot:* Bing snippets failed to consistently provide LinkedIn follower counts (e.g., "64K+ followers"). We completely abandoned Bing for this step and successfully migrated `LinkedInScraper` to rely solely on `GoogleSnippetScraper`.
- [x] Adapt Google DOM parsing for Social Profiles
  *   *Resolution:* Google dynamically alters its DOM layout. We successfully updated `scrapers/google_snippet.py` to target the `div.N54PNb` rich-snippet container and updated regex `r'([\d,]+(?:\.\d+)?[KkMm]?\+?)\s*followers'` to extract counts from adjacent breadcrumb spans.
- [ ] Monitor Google/TCC (`TheCompanyCheck`) scraper output
  *   *Current Blocker Status:* Google is currently returning 0 results for `site:thecompanycheck.com` queries. The Javascript DOM selectors that successfully find LinkedIn URLs (`div.g`, `div.N54PNb`) are failing to locate the CompanyCheck elements.
- [ ] Monitor Google Maps scraper output
- [ ] Handle any CAPTCHAs manually if they appear during initial testing

### Verification & Handover Phase
- [x] Generate debug HTML dumps (`google_test_results.txt`) for the failing `TheCompanyCheck` queries so the next agent can analyze the dynamic structure without guessing.
- [x] Create `test_scraper.py` to isolate the `TheCompanyCheck` extraction logic from the full orchestration loop.
- [ ] Fix TheCompanyCheck DOM selectors within `GoogleSnippetScraper`.
- [ ] Verify output Excel correctly merges all scraped data incrementally.
- [ ] Analyze logs for persistent errors and stability.
