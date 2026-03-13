"""Main Tofler scraper application with test CLI"""
import asyncio
import argparse
from pathlib import Path
from tqdm.asyncio import tqdm

import config
from utils.logger import ScraperLogger
from utils.progress import ProgressTracker
from utils.excel_handler import ExcelHandler
from scraper.browser_manager import BrowserPool
from scraper.tofler_scraper import ToflerScraper, CaptchaDetectedError
from scraper.data_processor import DataProcessor


class ToflerScraperApp:
    def __init__(self, resume=False):
        self.logger = ScraperLogger()
        self.progress = ProgressTracker()
        self.excel_handler = None
        self.browser_pool = None
        self.scraper = None
        self.resume = resume
        self.companies = []
        self.save_counter = 0
        self.captcha_count = 0
        self.captcha_threshold = 5  # Pause after 5 CAPTCHAs
        
    async def initialize(self):
        """Initialize all components"""
        self.logger.info("="*60)
        self.logger.info("Tofler Scraper Starting...")
        self.logger.info("="*60)
        
        # Load companies from Excel
        self.logger.info(f"Loading companies from {config.INPUT_EXCEL}...")
        self.excel_handler = ExcelHandler(config.INPUT_EXCEL)
        all_companies = self.excel_handler.read_companies()
        
        # Filter if resuming
        if self.resume and self.progress.data.get("processed", 0) > 0:
            self.logger.info(f"Resuming from previous session...")
            self.companies = [
                c for c in all_companies
                if not self.progress.is_processed(c["id"])
            ]
            self.logger.info(f"Already processed: {self.progress.data['processed']}")
            self.logger.info(f"Remaining: {len(self.companies)}")
        else:
            self.companies = all_companies
            self.progress.set_total(len(all_companies))
        
        self.logger.info(f"Total companies to scrape: {len(self.companies)}")
        
        # Initialize browser pool
        self.logger.info("Initializing browser pool...")
        self.browser_pool = BrowserPool(self.logger)
        await self.browser_pool.initialize()
        
        # Initialize scraper
        self.scraper = ToflerScraper(self.logger)
        
        self.logger.info("Initialization complete!")
    
    async def worker(self, worker_id, companies_queue, progress_bar):
        """Worker task to process companies"""
        context = self.browser_pool.get_context(worker_id)
        page = await context.new_page()
        
        self.logger.debug(f"[Worker-{worker_id}] Started")
        
        while not companies_queue.empty():
            try:
                company = await companies_queue.get()
                company_id = company["id"]
                company_name = company["name"]
                
                self.logger.debug(f"[Worker-{worker_id}] Processing: {company_id} - {company_name}")
                
                # Scrape company
                status, people_data, captcha_detected = await self.scraper.scrape_company(
                    page, company_id, company_name
                )
                
                # Handle CAPTCHA
                if captcha_detected:
                    self.captcha_count += 1
                    self.logger.log_failed(company_id, company_name, "CAPTCHA detected")
                    self.progress.update(company_id, 'failed')
                    
                    # Alert if too many CAPTCHAs
                    if self.captcha_count >= self.captcha_threshold:
                        self.logger.error(f"[ALERT] {self.captcha_count} CAPTCHAs detected! Consider pausing.")
                        # Reset count after alert
                        self.captcha_count = 0
                
                elif status == 'not_found':
                    self.logger.log_not_found(company_id, company_name)
                    self.progress.update(company_id, 'not_found')
                
                elif status == 'failed':
                    self.logger.log_failed(company_id, company_name, "Scraping failed after retries")
                    self.progress.update(company_id, 'failed')
                
                else:  # successful
                    # Process and save data
                    for person_data in people_data:
                        person = person_data["person"]
                        directorships = person_data["directorships"]
                        
                        directors_data, management_data = DataProcessor.process_person_data(
                            company_id, person, directorships, company_name
                        )
                        
                        # Add to buffers
                        for d in directors_data:
                            self.excel_handler.add_director_data(d)
                        for m in management_data:
                            self.excel_handler.add_management_data(m)
                    
                    # Incremental save
                    self.save_counter += 1
                    if self.save_counter >= config.SAVE_THRESHOLD:
                        self.logger.info(f"Saving progress... ({self.progress.data['processed']} companies)")
                        self.excel_handler.save_incremental()
                        self.save_counter = 0
                    
                    self.progress.update(company_id, 'successful')
                    self.logger.debug(f"[Worker-{worker_id}] Completed: {company_id}")
                
                # Increment browser usage for context recycling
                await self.browser_pool.increment_usage(worker_id)
                
                progress_bar.update(1)
                companies_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"[Worker-{worker_id}] Unexpected error: {e}")
                companies_queue.task_done()
        
        await page.close()
        self.logger.debug(f"[Worker-{worker_id}] Finished")
    
    async def run(self):
        """Main run loop"""
        await self.initialize()
        
        # Create queue
        queue = asyncio.Queue()
        for company in self.companies:
            await queue.put(company)
        
        # Create progress bar
        progress_bar = tqdm(
            total=len(self.companies),
            desc="Scraping Progress",
            unit="companies"
        )
        
        # Launch workers
        workers = [
            asyncio.create_task(self.worker(i, queue, progress_bar))
            for i in range(config.MAX_CONCURRENT_BROWSERS)
        ]
        
        # Wait for all workers to complete
        await asyncio.gather(*workers)
        
        progress_bar.close()
        
        # Final save
        self.logger.info("Saving final data...")
        self.excel_handler.save_incremental()
        self.excel_handler.close()
        
        # Close browser pool
        await self.browser_pool.close()
        
        # Print summary
        summary = self.progress.get_summary()
        self.logger.info("="*60)
        self.logger.info("SCRAPING COMPLETE!")
        self.logger.info(f"Total: {summary['total']}")
        self.logger.info(f"Successful: {summary['successful']}")
        self.logger.info(f"Not Found: {summary['not_found']}")
        self.logger.info(f"Failed: {summary['failed']}")
        self.logger.info("="*60)


async def test_single_company(company_name):
    """Test scraper with a single company name"""
    print("="*60)
    print(f"Testing single company: {company_name}")
    print("="*60)
    
    logger = ScraperLogger()
    browser_pool = BrowserPool(logger)
    
    try:
        # Initialize single browser
        await browser_pool.initialize()
        scraper = ToflerScraper(logger)
        
        # Get page
        context = browser_pool.get_context(0)
        page = await context.new_page()
        
        # Scrape
        print(f"\n[SEARCH] Searching for: {company_name}")
        status, people_data, captcha_detected = await scraper.scrape_company(
            page, "TEST", company_name
        )
        
        print(f"\n[RESULTS]")
        print(f"   Final Page URL: {page.url}")
        print(f"   Status: {status}")
        print(f"   CAPTCHA detected: {captcha_detected}")
        print(f"   People found: {len(people_data)}")
        
        if people_data:
            print(f"\n[PEOPLE]")
            for pd in people_data:
                person = pd["person"]
                directorships = pd["directorships"]
                print(f"\n   • {person['name']} ({person['designation']})")
                print(f"     DIN: {person['din']}")
                print(f"     Directorships: {len(directorships)}")
                
                for d in directorships[:3]:  # Show first 3
                    print(f"       - {d['name']} | {d['industry']} | {d['status']}")
                
                if len(directorships) > 3:
                    print(f"       ... and {len(directorships) - 3} more")
            
            # Save to Excel
            print(f"\n[EXCEL] Saving data to output/test_directors.xlsx...")
            try:
                # Initialize Excel handler specifically for test
                excel = ExcelHandler(config.INPUT_EXCEL)
                
                # Process data
                for person_data in people_data:
                    person = person_data["person"]
                    directorships = person_data["directorships"]
                    
                    directors_data, management_data = DataProcessor.process_person_data(
                        "TEST", person, directorships, company_name
                    )
                    
                    for d in directors_data:
                        excel.add_director_data(d)
                    for m in management_data:
                        excel.add_management_data(m)
                
                # Force save to a specific test file or default
                # But ExcelHandler methods use internal buffers and config settings?
                # excel.save_incremental() saves to default.
                # Let's save to default but specific filename if possible?
                # ExcelHandler doesn't support changing output filename easily after init?
                # Actually it saves to self.directors_file which is config.OUTPUT_DIR / ...
                # We'll just use the standard save mechanism.
                excel.save_incremental()
                excel.close()
                print(f"[EXCEL] Saved successfully to {config.OUTPUT_DIR}")
            except Exception as e:
                print(f"[EXCEL] Error saving: {e}")
                import traceback
                traceback.print_exc()

        await page.close()
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await browser_pool.close()
        print("\n" + "="*60)
        print("Test complete!")


async def main():
    parser = argparse.ArgumentParser(
        description="Tofler Scraper - Extract directorship data from Tofler.in",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                              # Run full scraper
  python main.py --resume                     # Resume from previous session
  python main.py --test "Company Name Pvt Ltd" # Test with single company
  python main.py --test "J S AUTO CAST"       # Test with company name
        """
    )
    parser.add_argument(
        '--resume', 
        action='store_true', 
        help='Resume from previous session'
    )
    parser.add_argument(
        '--test', 
        type=str, 
        metavar='COMPANY_NAME',
        help='Test mode: scrape a single company by name'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run in headless mode (default: True)'
    )
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run with visible browser (for debugging)'
    )
    
    args = parser.parse_args()
    
    # Override headless if visible flag is set
    if args.visible:
        config.HEADLESS = False
        print("Running with visible browser...")
    
    if args.test:
        # Test mode: single company
        await test_single_company(args.test)
    else:
        # Full scraper mode
        app = ToflerScraperApp(resume=args.resume)
        await app.run()


if __name__ == "__main__":
    asyncio.run(main())
