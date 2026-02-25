import asyncio
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class ZeptoScraper:
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
                
                if "zeptonow.com" not in search_url:
                    search_url = f"https://zeptonow.com/search?query={urllib.parse.quote(search_url)}"

                self.update_status("Searching Zepto...")
                await page.goto(search_url, wait_until="networkidle")
                await asyncio.sleep(3)

                product_cards = await page.query_selector_all('[data-testid="product-card"]')
                
                self.update_status(f"Found {len(product_cards)} products. Extracting...")
                
                final = []
                for i, card in enumerate(product_cards):
                     try:
                        name_el = await card.query_selector("h5")
                        if not name_el: name_el = await card.query_selector("h4") # fallback
                        name = await name_el.inner_text() if name_el else "N/A"
                        
                        price_el = await card.query_selector('[data-testid="product-price"]')
                        price = (await price_el.inner_text()).replace("₹", "") if price_el else "N/A"
                        
                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Platform": "Zepto",
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                     except: continue

                await browser.close()
                fname = f"zepto_results_{self.job_id}.csv"
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
                    if not url.startswith("http"): url = f"https://zeptonow.com{url}" if url.startswith("/") else f"https://{url}"
                    
                    # Extract PVID from URL (usually last segment or guid)
                    # e.g. /product-name/pvid/.... or simply ID at end
                    pvid = "N/A"
                    try:
                        # Heuristic: Take last non-empty segment
                        path_segments = [s for s in url.split("/") if s]
                        if path_segments: pvid = path_segments[-1]
                    except: pass

                    page = await context.new_page()
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=60000)
                        
                        # Initialize
                        name = "N/A"
                        price = "N/A"
                        rating = "N/A"
                        reviews_count = "N/A"
                        
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
                                            elif 'lowPrice' in data['offers']: price = str(data['offers']['lowPrice'])
                                        if 'aggregateRating' in data:
                                            if 'ratingValue' in data['aggregateRating']: rating = str(data['aggregateRating']['ratingValue'])
                                            if 'reviewCount' in data['aggregateRating']: reviews_count = str(data['aggregateRating']['reviewCount'])
                                except: continue
                        except: pass

                        # Fallbacks
                        if name == "N/A":
                            name_el = await page.query_selector('h1')
                            name = await name_el.inner_text() if name_el else "N/A"
                        
                        if price == "N/A":
                            try:
                                # Data Test ID
                                price_el = await page.query_selector('[data-testid="product-price"]')
                                if price_el: 
                                    price_text = await price_el.inner_text()
                                    match = re.search(r"₹\s?([\d,]+)", price_text)
                                    if match: price = match.group(1).replace(",", "")
                                # Fallback regex
                                if price == "N/A":
                                    elements = await page.query_selector_all("h4, h5, div")
                                    for el in elements:
                                        txt = await el.inner_text()
                                        if "₹" in txt and len(txt) < 20: 
                                            match = re.search(r"₹\s?([\d,]+)", txt)
                                            if match:
                                                price = match.group(1).replace(",", "")
                                                break
                            except: pass
                        
                        if rating == "N/A":
                             try:
                                body_text = await page.inner_text("body")
                                match = re.search(r"(\d\.\d)\s*\((\d+)\)", body_text)
                                if match:
                                    rating = match.group(1)
                                    reviews_count = match.group(2)
                             except: pass

                        final.append({
                            "Product Name": name,
                            "Price": price,
                            "Rating": rating,
                            "Number of Reviews": reviews_count,
                            "PVID": pvid,
                            "Platform": "Zepto",
                            "URL": url,
                            "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except Exception as e:
                        print(f"Error scraping {url}: {e}")
                    finally:
                        await page.close()
                    
                    await asyncio.sleep(2)

                await browser.close()
                fname = f"zepto_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)

    async def run_reviews(self, product_url):
         self.update_status("Zepto does not have traditional public reviews.", done=True)
