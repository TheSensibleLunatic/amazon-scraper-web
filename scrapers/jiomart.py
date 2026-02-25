import asyncio
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class JiomartScraper:
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
                # Jiomart is sensitive to headless
                browser = await p.chromium.launch(headless=False) 
                context = await browser.new_context(viewport={'width':1920,'height':1080})
                page = await context.new_page()
                
                if "jiomart.com" not in search_url:
                    search_url = f"https://www.jiomart.com/search/{urllib.parse.quote(search_url)}"

                self.update_status("Searching Jiomart...")
                await page.goto(search_url, wait_until="domcontentloaded")
                await asyncio.sleep(3)

                # Selectors for Jiomart
                product_cards = await page.query_selector_all('.ais-InfiniteHits-item')
                if not product_cards:
                     product_cards = await page.query_selector_all('.plp-card-container')

                self.update_status(f"Found {len(product_cards)} products. Extracting...")
                
                final = []
                for i, card in enumerate(product_cards):
                     try:
                        name_el = await card.query_selector("div.plp-card-details-name")
                        name = await name_el.inner_text() if name_el else "N/A"
                        
                        price_el = await card.query_selector("span.plp-card-details-price-discounted")
                        if not price_el: price_el = await card.query_selector(".plp-card-details-price")
                        price = (await price_el.inner_text()).replace("₹", "") if price_el else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Jiomart",
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                     except: continue

                await browser.close()
                fname = f"jiomart_results_{self.job_id}.csv"
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
                    if not url.startswith("http"): url = f"https://www.jiomart.com{url}" if url.startswith("/") else f"https://{url}"
                    
                    # PID from URL
                    # URL usually: .../p/categoryId/productId
                    pid = "N/A"
                    try:
                        path_segments = [s for s in url.split("/") if s]
                        if path_segments: pid = path_segments[-1] # Heuristic
                    except: pass

                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        
                        # Initialize vars
                        name = "N/A"
                        price = "N/A"
                        rating = "N/A"
                        count = "N/A"

                        # Strategy 0: JSON-LD
                        try:
                            scripts = await page.query_selector_all('script[type="application/ld+json"]')
                            for script in scripts:
                                content = await script.inner_text()
                                try:
                                    data = json.loads(content)
                                    if isinstance(data, list):
                                        for x in data:
                                            if x.get('@type') == 'Product': data = x; break
                                    
                                    if data.get('@type') == 'Product':
                                        if 'name' in data: name = data['name']
                                        if 'offers' in data:
                                            if 'price' in data['offers']: price = str(data['offers']['price'])
                                        if 'aggregateRating' in data:
                                            if 'ratingValue' in data['aggregateRating']: rating = str(data['aggregateRating']['ratingValue'])
                                            if 'reviewCount' in data['aggregateRating']: count = str(data['aggregateRating']['reviewCount'])
                                except: continue
                        except: pass

                        # Strategy 1: CSS Fallbacks
                        if name == "N/A":
                            name_el = await page.query_selector('h1.product-title-name')
                            if not name_el: name_el = await page.query_selector("div.product-header-name h1")
                            if not name_el: name_el = await page.query_selector("h1") 
                            if name_el: name = await name_el.inner_text()
                        
                        if price == "N/A":
                            price_el = await page.query_selector('.product-price .price')
                            if not price_el: 
                                # Use regex on specific containers, not entire body
                                try:
                                    container = await page.query_selector("#price-section")
                                    if container: 
                                        txt = await container.inner_text()
                                        m = re.search(r"₹\s?([\d,]+)", txt)
                                        if m: price = m.group(1).replace(",", "")
                                except: pass
                            else:
                                price = (await price_el.inner_text()).replace("₹", "").strip()
                        
                        if count == "N/A":
                             count_el = await page.query_selector(".rating-count") 
                             if not count_el: count_el = await page.query_selector(".review-count") 
                             if count_el: count = await count_el.inner_text()

                        final.append({
                            "Product Name": name.strip(),
                            "Price": price,
                            "Rating": rating, 
                            "Number of Reviews": count,
                            "Product ID": pid,
                            "Platform": "Jiomart",
                            "URL": url,
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except Exception as e:
                        print(f"Error scraping {url}: {e}")
                    finally:
                        await page.close()
                    
                    await asyncio.sleep(2) # Jiomart can be sensitive

                await browser.close()
                fname = f"jiomart_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)
        
    async def run_reviews(self, product_url):
        self.update_status("Review scraping not fully implemented for Jiomart yet.", done=True)
