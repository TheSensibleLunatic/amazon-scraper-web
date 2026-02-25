import asyncio
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class SwiggyScraper:
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
                
                if "swiggy.com" not in search_url:
                    search_url = f"https://www.swiggy.com/instamart/search?custom_back=true&query={urllib.parse.quote(search_url)}"

                self.update_status("Searching Swiggy Instamart...")
                await page.goto(search_url, wait_until="networkidle")
                await asyncio.sleep(3)

                # Swiggy classes are often randomized like _12345 or styled components.
                # We often need to rely on data-testid or generic structure.
                product_cards = await page.query_selector_all('[data-testid="product_card"]')
                
                self.update_status(f"Found {len(product_cards)} products. Extracting...")
                
                final = []
                for i, card in enumerate(product_cards):
                     try:
                        # Attempt to find text content
                        text_content = await card.inner_text()
                        lines = text_content.split('\n')
                        
                        # Heuristic: Name is usually first or second line
                        name = lines[0] if lines else "N/A"
                        
                        # Price usually contains ₹
                        price_match = re.search(r"₹\s?(\d+)", text_content)
                        price = price_match.group(1) if price_match else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Swiggy Instamart",
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                     except: continue

                await browser.close()
                fname = f"swiggy_results_{self.job_id}.csv"
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
                    if not url.startswith("http"): url = f"https://www.swiggy.com{url}" if url.startswith("/") else f"https://{url}"
                    
                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=60000)
                        
                        # Swiggy Item Page
                        # Try to find H1 or typical product name classes
                        # Their classes are very randomized (e.g. _3wL...), so we might rely on test-ids if available or hierarchy
                        name_el = await page.query_selector('h1')
                        name = await name_el.inner_text() if name_el else "N/A"
                        
                        price = "N/A"
                        body_text = await page.inner_text("body")
                        price_match = re.search(r"₹\s?(\d+)", body_text)
                        if price_match: price = price_match.group(1)
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Swiggy Instamart",
                            "URL": url,
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except Exception as e:
                        print(f"Error scraping {url}: {e}")
                    finally:
                        await page.close()
                    
                    await asyncio.sleep(2)

                await browser.close()
                fname = f"swiggy_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)
        
    async def run_reviews(self, product_url):
        self.update_status("Swiggy Instamart does not have traditional reviews.", done=True)
