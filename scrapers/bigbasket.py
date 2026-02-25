import asyncio
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class BigBasketScraper:
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
                
                if "bigbasket.com" not in search_url:
                    search_url = f"https://www.bigbasket.com/ps/?q={urllib.parse.quote(search_url)}"
                
                self.update_status("Searching Big Basket...")
                await page.goto(search_url, wait_until="domcontentloaded")
                await asyncio.sleep(3)

                # Scroll a bit
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight/3)")
                await asyncio.sleep(1)

                # Big Basket usually has good QA tags or classes
                product_cards = await page.query_selector_all('div[ng-repeat^="prod in"]') # Old angular
                if not product_cards:
                     product_cards = await page.query_selector_all('div.sku-card') # Newer React/Vue
                if not product_cards:
                     product_cards = await page.query_selector_all('li[class*="PaginatedList"]') # Even newer?

                self.update_status(f"Found {len(product_cards)} products. Extracting...")
                
                final = []
                for i, card in enumerate(product_cards):
                     try:
                        text = await card.inner_text()
                        lines = text.split('\n')
                        name = lines[0] # Simplification
                        
                        price = "N/A"
                        for line in lines:
                             if "Rs" in line or "₹" in line:
                                  price = line.replace("MRP", "").strip()
                                  break
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Big Basket",
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                     except: continue

                await browser.close()
                fname = f"bigbasket_results_{self.job_id}.csv"
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
                    if not url.startswith("http"): url = f"https://www.bigbasket.com{url}" if url.startswith("/") else f"https://{url}"
                    
                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        
                        name_el = await page.query_selector("h1")
                        name = await name_el.inner_text() if name_el else "N/A"
                        
                        # BigBasket Price often in a table or DiscountedPrice class
                        price_el = await page.query_selector("td[data-qa='productPrice']")
                        if not price_el: price_el = await page.query_selector("div[data-qa='productPrice']")
                        price = (await price_el.inner_text()).replace("Rs", "").replace("₹", "").strip() if price_el else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Big Basket",
                            "URL": url,
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except Exception as e:
                        print(f"Error scraping {url}: {e}")
                    finally:
                        await page.close()
                    
                    await asyncio.sleep(2)

                await browser.close()
                fname = f"bigbasket_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)
        
    async def run_reviews(self, product_url):
        self.update_status("Review scraping not fully implemented for Big Basket yet.", done=True)
