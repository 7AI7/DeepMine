import asyncio
import sys
import os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.append('.')

from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION JS — mirrors the logic in scrapers/google_snippet.py exactly.
# Keep these two in sync whenever google_snippet.py is updated.
# ─────────────────────────────────────────────────────────────────────────────
EXTRACTION_JS = """
    () => {
        const results = [];

        // Modern Google layout (2024+): each organic result lives in div.tF2Cxc.
        // Fallback to div.N54PNb / div.g for older/alternate layouts.
        let items = document.querySelectorAll('div.tF2Cxc');
        if (items.length === 0) {
            items = document.querySelectorAll('div.N54PNb, div.g');
        }

        const MAX = 5;
        for (let i = 0; i < Math.min(items.length, MAX); i++) {
            const item = items[i];

            // Title: prefer h3.LC20lb, fall back to any h3, then to bold span
            let titleEl = item.querySelector('h3.LC20lb')
                        || item.querySelector('h3');
            if (!titleEl) {
                for (const a of item.querySelectorAll('a[href]')) {
                    const span = a.querySelector('span');
                    if (span && span.textContent.trim().length > 5) {
                        titleEl = span;
                        break;
                    }
                }
            }

            // URL: use the dedicated title-link anchor (avoids favicon/breadcrumb anchors)
            const linkEl = item.querySelector('a[jsname="UWckNb"]')
                        || item.querySelector('a.zReHs')
                        || item.querySelector('a[href]');

            // Snippet
            const snippetEl = item.querySelector('div.VwiC3b')
                           || item.querySelector('div[data-sncf]')
                           || item.querySelector('span.st');

            // Followers (LinkedIn rich-snippet)
            let followersText = '';
            for (const el of item.querySelectorAll('cite, span.VuuXrf, span.st')) {
                const txt = el.textContent || '';
                if (txt.toLowerCase().includes('followers')) {
                    followersText = txt.trim();
                    break;
                }
            }

            if (titleEl && linkEl) {
                let url = linkEl.href;
                if (url.includes('/url?')) {
                    try {
                        const params = new URL(url).searchParams;
                        url = params.get('q') || params.get('url') || url;
                    } catch(e) {}
                }
                results.push({
                    title: titleEl.textContent.trim(),
                    url: url,
                    snippet: snippetEl ? snippetEl.textContent.trim() : '',
                    followers: followersText,
                });
            }
        }
        return results;
    }
"""


async def test_extraction_from_file():
    """
    Load the saved google_test_results.txt into a Playwright page and run the
    extraction JS.  No live network request required — validates the JS logic
    against the already-dumped SERP HTML.
    """
    html_file = os.path.join(os.path.dirname(__file__), 'google_test_results.txt')
    if not os.path.exists(html_file):
        print(f"ERROR: {html_file} not found — run test_dump_html() first.")
        return

    with open(html_file, 'r', encoding='utf-8') as f:
        raw = f.read()

    # Strip the "--- HTML DUMP ---" header line if present
    if raw.startswith('--- HTML DUMP ---'):
        raw = raw[raw.index('\n') + 1:]

    print(f"\n[file] Loaded {len(raw):,} chars from google_test_results.txt")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Inject the saved SERP HTML into a blank page so JS selectors work
        await page.set_content(raw, wait_until='domcontentloaded')

        results = await page.evaluate(EXTRACTION_JS)
        await browser.close()

    if not results:
        print("\n[FAIL] Extraction returned 0 results — selectors are still broken.")
        return

    print(f"\n[PASS] Extracted {len(results)} result(s):\n")
    for i, r in enumerate(results, 1):
        print(f"  Result {i}:")
        print(f"    title   : {r['title']}")
        print(f"    url     : {r['url']}")
        print(f"    snippet : {r['snippet'][:120]}...")
        if r['followers']:
            print(f"    followers: {r['followers']}")
        print()

    # Quick assertions
    tcc_found = any('thecompanycheck.com' in r['url'] for r in results)
    titles_ok = all(r['title'] for r in results)

    print(f"  thecompanycheck.com URL found : {'YES' if tcc_found else 'NO  <-- FAIL'}")
    print(f"  All results have titles       : {'YES' if titles_ok else 'NO  <-- FAIL'}")
    print()


async def test_dump_html():
    """
    Navigate to Google live, handle CAPTCHA, and dump the #search HTML to
    google_test_results.txt.  Use this to refresh the HTML dump.
    """
    from scrapers.google_snippet import GoogleSnippetScraper

    print("Initializing Google Scraper...")
    scraper = GoogleSnippetScraper(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
    await scraper.initialize()

    query = 'site:thecompanycheck.com "Pricol Limited"'
    print(f"\nNavigating to Google for: {query}")

    try:
        url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
        await scraper.page.goto(url, timeout=30000)
        await asyncio.sleep(4)

        if await scraper._check_captcha():
            print("CAPTCHA detected — please solve in the browser window...")
            for _ in range(24):
                await asyncio.sleep(5)
                if not await scraper._check_captcha():
                    print("CAPTCHA resolved")
                    await scraper.page.goto(url, timeout=30000)
                    await asyncio.sleep(4)
                    break

        await scraper.page.wait_for_selector('body', timeout=10000)

        search_el = scraper.page.locator('#search').first
        if await search_el.count():
            html = await search_el.inner_html()
            print("Found #search container")
        else:
            html = await scraper.page.content()
            print("No #search container — dumping full page")

        with open('google_test_results.txt', 'w', encoding='utf-8') as f:
            f.write("--- HTML DUMP ---\n")
            f.write(html + "\n")

        print("Done. HTML dumped to google_test_results.txt")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await scraper.close()


async def test_live():
    """
    Open a HEADED browser, navigate to Google live, run scraper.search(),
    and print extracted results.  Browser stays open 15s so you can see the page.
    Uses config.HEADLESS (currently False) so the window is visible.
    """
    from scrapers.google_snippet import GoogleSnippetScraper

    print("Initializing Google Scraper (headed mode)...")
    scraper = GoogleSnippetScraper(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
    await scraper.initialize()

    query = 'site:thecompanycheck.com "Pricol Limited"'
    print(f"\nSearching Google for: {query}")
    print("Watch the browser window ...\n")

    try:
        results = await scraper.search(query, max_results=5)

        if not results:
            print("[FAIL] search() returned 0 results.")
        elif results[0].get("error") == "captcha":
            print("[CAPTCHA] Please solve the CAPTCHA in the browser window, then re-run.")
        else:
            print(f"[PASS] Extracted {len(results)} result(s):\n")
            for i, r in enumerate(results, 1):
                print(f"  Result {i}:")
                print(f"    title   : {r['title']}")
                print(f"    url     : {r['url']}")
                print(f"    snippet : {r['snippet'][:120]}...")
                if r.get('followers'):
                    print(f"    followers: {r['followers']}")
                print()

            tcc_found  = any('thecompanycheck.com' in r['url'] for r in results)
            b_url      = next((r['url'] for r in results if '/company/b/' in r['url']), None)
            titles_ok  = all(r['title'] for r in results)

            print(f"  thecompanycheck.com URL found : {'YES' if tcc_found else 'NO  <-- FAIL'}")
            print(f"  /company/b/ URL found         : {b_url if b_url else 'NO  <-- FAIL'}")
            print(f"  All results have titles       : {'YES' if titles_ok else 'NO  <-- FAIL'}")

        print("\nBrowser will close in 15 seconds — watch the window now...")
        await asyncio.sleep(15)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await scraper.close()


async def test_tcc_full():
    """
    Full end-to-end test: Google search → navigate TCC company page → extract metrics.
    Headed browser (config.HEADLESS=False) so you can watch each step.
    """
    from scrapers.google_snippet import GoogleSnippetScraper
    from scrapers.thecompanycheck import CompanyCheckScraper

    company = "Pricol Limited"

    print("Initializing Google Scraper (headed mode)...")
    scraper = GoogleSnippetScraper(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
    await scraper.initialize()

    tcc = CompanyCheckScraper(google_scraper=scraper)

    print(f"\nRunning full TCC scrape for: {company}")
    print("Watch the browser — it will search Google then navigate to TheCompanyCheck...\n")

    try:
        result = await tcc.scrape(company)

        print("-" * 50)
        print(f"  Company         : {company}")
        print(f"  Revenue 2023    : {result['revenue_2023']}")
        print(f"  Net Profit 2023 : {result['net_profit_2023']}")
        print(f"  Employee Count  : {result['employee_count']}")
        print(f"  Source URL      : {result['tcc_source_url']}")
        print("-" * 50)

        if result['tcc_source_url'] == 'N/A':
            print("\n[FAIL] No usable TCC URL found — check Google results.")
        elif result['revenue_2023'] == 'N/A' and result['employee_count'] == 'N/A':
            print("\n[PARTIAL] Page loaded but metrics not extracted — check _extract_key_metrics regex.")
        else:
            print("\n[PASS] Metrics extracted successfully.")

        print("\nBrowser will close in 15 seconds — watch the window now...")
        await asyncio.sleep(15)

    except Exception as e:
        print(f"Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        await scraper.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else 'test'

    if mode == 'tcc':
        # python test_scraper.py tcc  — full end-to-end: Google → TCC page → metrics
        asyncio.run(test_tcc_full())
    elif mode == 'live':
        # python test_scraper.py live — headed browser, Google extraction only
        asyncio.run(test_live())
    elif mode == 'dump':
        # python test_scraper.py dump — refresh HTML dump from live Google
        asyncio.run(test_dump_html())
    else:
        # python test_scraper.py     — offline extraction test against saved HTML
        asyncio.run(test_extraction_from_file())
