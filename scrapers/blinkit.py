import asyncio
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class BlinkitScraper:
    def __init__(self, job_id, jobs_dict):
        self.job_id = job_id
        self.jobs = jobs_dict 

    def update_status(self, status, progress=None, total=None, done=False, filename=None):
        self.jobs[self.job_id]['status'] = status
        if progress: self.jobs[self.job_id]['progress'] = progress
        if total: self.jobs[self.job_id]['total'] = total
        if done: self.jobs[self.job_id]['done'] = True
        if filename: self.jobs[self.job_id]['filename'] = filename

    async def run_search(self, search_url):
        try:
            self.update_status("Launching Browser...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(viewport={'width':1920,'height':1080})
                page = await context.new_page()
                
                if "blinkit.com" not in search_url:
                    search_url = f"https://blinkit.com/s/?q={urllib.parse.quote(search_url)}"

                self.update_status("Searching Blinkit...")
                await page.goto(search_url, wait_until="networkidle")
                await asyncio.sleep(3)

                # Scroll to load more
                for _ in range(3):
                    await page.mouse.wheel(0, 1000)
                    await asyncio.sleep(1)

                # Blinkit product cards often have specific classes or data attributes
                # We'll try a generic approach for their common structure
                # As of 2024/2025, structure might vary. Using text-based approximation or common classes.
                product_cards = await page.query_selector_all('div[data-test-id="available-product-item"]')
                if not product_cards:
                     product_cards = await page.query_selector_all('a[data-test-id="plp-product-item"]')
                
                self.update_status(f"Found {len(product_cards)} products. Extracting...")
                
                final = []
                for i, card in enumerate(product_cards):
                     try:
                        text = await card.inner_text()
                        lines = text.split('\n')
                        # Heuristic extraction
                        name = lines[0] if len(lines) > 0 else "N/A"
                        price_match = re.search(r"₹\s?(\d+)", text)
                        price = price_match.group(1) if price_match else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Blinkit",
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                     except: continue

                await browser.close()
                fname = f"blinkit_results_{self.job_id}.csv"
                pd.DataFrame(final).to_csv(fname, index=False, encoding='utf-8-sig')
                self.update_status("Done!", done=True, filename=fname)

        except Exception as e:
            self.update_status(f"Error: {e}", done=True)
    
    async def run_bulk(self, url_text):
        try:
            urls = [u.strip() for u in re.split(r'[,\n ]', url_text) if u.strip()]
            self.update_status("Launching Browser...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(viewport={'width':1920,'height':1080})
                
                final = []
                for i, url in enumerate(urls):
                    self.update_status(f"Processing {i+1}/{len(urls)}...", progress=i+1, total=len(urls))
                    if not url.startswith("http"): url = f"https://blinkit.com{url}" if url.startswith("/") else f"https://{url}"
                    
                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=60000)
                        
                        # Product Page Extraction
                        # Blinkit product detail pages usually have the name in an H1 or specific class
                        # We try multiple selectors for robustness
                        name_el = await page.query_selector('h1')
                        name = await name_el.inner_text() if name_el else "N/A"
                        
                        # Price is often in a specific container close to the add button
                        # Try finding the price symbol
                        body_text = await page.inner_text("body")
                        price_match = re.search(r"₹\s?(\d+)", body_text)
                        
                        # Refine price search if possible (e.g. look for class containing price)
                        # But body text regex is a reasonable fallback for these SPAs if classes change
                        price = price_match.group(1) if price_match else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Blinkit",
                            "URL": url,
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except Exception as e:
                        print(f"Error scraping {url}: {e}")
                    finally:
                        await page.close()
                    
                    await asyncio.sleep(2)

                await browser.close()
                fname = f"blinkit_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)

    async def run_reviews(self, product_url):
         self.update_status("Blinkit does not have traditional public reviews.", done=True)
