import asyncio
import random
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

class FlipkartScraper:
    def __init__(self, job_id, jobs_dict):
        self.job_id = job_id
        self.jobs = jobs_dict 

    def update_status(self, status, progress=None, total=None, done=False, filename=None):
        self.jobs[self.job_id]['status'] = status
        if progress: self.jobs[self.job_id]['progress'] = progress
        if total: self.jobs[self.job_id]['total'] = total
        if done: self.jobs[self.job_id]['done'] = True
        if filename: self.jobs[self.job_id]['filename'] = filename

    async def get_deep_details(self, context, item_data):
        url = item_data['URL']
        if not url.startswith('http'): url = f"https://www.flipkart.com{url}"
        
        # Extract PID from URL
        pid = "N/A"
        try:
            parsed = urllib.parse.urlparse(url)
            qs = urllib.parse.parse_qs(parsed.query)
            if 'pid' in qs: pid = qs['pid'][0]
            else:
                match = re.search(r"pid=([A-Z0-9]+)", url)
                if match: pid = match.group(1)
        except: pass

        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Initialize variables
            title = "N/A"
            price = "N/A"
            rating = "N/A"
            ratings_count = "N/A"
            
            # ---------------------------------------------------------
            # Layer 1: JSON-LD (Structured Data) - Good for Name/Rating/ID
            # ---------------------------------------------------------
            try:
                scripts = await page.query_selector_all('script[type="application/ld+json"]')
                for script in scripts:
                    content = await script.inner_text()
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            for item in data:
                                if item.get('@type') == 'Product': data = item; break
                        
                        if data.get('@type') == 'Product':
                            if 'name' in data and title == "N/A": title = data['name']
                            
                            # JSON-LD Price is often unreliable (shows base price or offer price not main display)
                            # We will only use it as a fallback later if Visual extraction fails.
                            json_price = None
                            if 'offers' in data:
                                offer = data['offers']
                                if isinstance(offer, list): offer = offer[0]
                                if 'price' in offer: json_price = str(offer['price'])
                            
                            if 'aggregateRating' in data:
                                agg = data['aggregateRating']
                                if 'ratingValue' in agg: rating = str(agg['ratingValue'])
                                if 'reviewCount' in agg: ratings_count = str(agg['reviewCount'])
                    except: continue
            except: pass

            # ---------------------------------------------------------
            # Layer 2: CSS Selectors (Visual Truth) - Specific Classes
            # ---------------------------------------------------------
            
            # TITLE
            if title == "N/A":
                for selector in ["span.B_NuCI", "h1.yhB1nd", "h1"]:
                    el = await page.query_selector(selector)
                    if el: 
                        title = await el.inner_text()
                        break

            # PRICE - VISUAL PRIORITY
            if price == "N/A":
                price_selectors = ["div.Nx9bqj.CxhGGd", "div.Nx9bqj", "div._30jeq3._16Jk6d", "div._30jeq3"]
                for selector in price_selectors:
                    el = await page.query_selector(selector)
                    if el:
                        txt = await el.inner_text()
                        cleaned = txt.replace("₹", "").replace(",", "").strip()
                        if cleaned.isdigit():
                            price = cleaned
                            break

            # ---------------------------------------------------------
            # Layer 3: Text content Search (Last Resort)
            # ---------------------------------------------------------
            
            # Price fallback: Strict Element match
            if price == "N/A":
                try:
                    elements = await page.query_selector_all("div, span, h1, h2, h3, h4")
                    candidates = []
                    for el in elements:
                        txt = (await el.inner_text()).strip()
                        if re.match(r"^₹\d{1,3}(?:,\d{3})*$", txt):
                            val = txt.replace("₹", "").replace(",", "")
                            if val.isdigit(): candidates.append(int(val))
                    
                    if candidates:
                        candidates = [c for c in candidates if c > 100]
                        if candidates: price = str(candidates[0])
                except: pass
            
            # JSON-LD Price Fallback (if Visual failed)
            if price == "N/A" and 'json_price' in locals() and json_price:
                price = json_price



            # RATING (Visual)
            if rating == "N/A":
                rating_selectors = ["div.XQDdHH", "div._3LWZlK"]
                for selector in rating_selectors:
                    el = await page.query_selector(selector)
                    if el:
                        rating = await el.inner_text()
                        break

            # RATINGS COUNT (Visual)
            if ratings_count == "N/A":
                count_selectors = ["span.Wphh3N", "span._2_R_DZ"]
                for selector in count_selectors:
                    el = await page.query_selector(selector)
                    if el:
                        txt = await el.inner_text()
                        # Use negative lookbehind (?<!\d) or stricter boundary
                        # Matches "47,384" inside "4.4 47,384 Ratings"
                        # We want the group adjacent to "Ratings"
                        match = re.search(r"(?<!\.)(\b[\d,]+)\s+Ratings", txt)
                        if match:
                            ratings_count = match.group(1)
                            break
            
            # ---------------------------------------------------------
            # Layer 3: Text content Search (Last Resort)
            # ---------------------------------------------------------
            
            # Price fallback: 
            # Problem: "Extra ₹1000 off" or "₹86 Fee" are mixed text.
            # Solution: Look for elements that contain *only* the price.
            if price == "N/A":
                try:
                    # Query all generic elements that might hold a price
                    elements = await page.query_selector_all("div, span, h1, h2, h3, h4")
                    candidates = []
                    for el in elements:
                        txt = (await el.inner_text()).strip()
                        # Check if text is EXACTLY "₹28,999" or "28,999" (with optional whitespace)
                        # Reject if it has extra chars like "off", "Fee", "+"
                        if re.match(r"^₹\d{1,3}(?:,\d{3})*$", txt):
                            val = txt.replace("₹", "").replace(",", "")
                            if val.isdigit(): candidates.append(int(val))
                    
                    if candidates:
                        # Heuristic: The selling price is usually the MAX candidate that isn't absurdly high?
                        # No, MRP might be higher. But MRP usually has a strikethrough class.
                        # However, strike-through text inner_text() is just "₹36,999".
                        # Wait, pure text elements?
                        # Let's trust the first few candidates.
                        # Usually the main price is first or second large number.
                        # Let's filter out very small numbers (fees)
                        candidates = [c for c in candidates if c > 100]
                        if candidates:
                             # If we have multiple, the 'Selling Price' is likely present.
                             # Often MRP is also present as a pure number.
                             # But MRP usually comes AFTER Selling Price in DOM order or CSS visual order?
                             # Let's pick the first one found in DOM order.
                             price = str(candidates[0])
                except: pass

            # Rating Fallback (Text)
            if rating == "N/A":
                try:
                    body_text = await page.inner_text("body")
                    # Strategy: Look for "4.3" that is immediately followed by "Ratings" or the count
                    # Pattern: 4.3 [star?] [space] 45,585 Ratings
                    match = re.search(r"(\d\.\d)\s*★?\s*?[\d,]+\s*Ratings", body_text)
                    if match:
                        rating = match.group(1)
                    else:
                        # Pattern 2: Just proximity to "Ratings"
                        for m in re.finditer(r"Ratings", body_text):
                            start = m.start()
                            preceding = body_text[max(0, start-30):start]
                            score_match = re.search(r"([3-5]\.\d)", preceding)
                            if score_match:
                                rating = score_match.group(1)
                                break
                except: pass

            # Ratings Count Fallback (Text)
            if ratings_count == "N/A":
                try:
                    body_text = await page.inner_text("body")
                    # Strict regex: Start of line or space, number, space, Ratings
                    match = re.search(r"(?:^|\s)([\d,]+)\s+Ratings", body_text)
                    if match: ratings_count = match.group(1)
                except: pass




            await page.close()
            
            return {
                "Product Name": title.strip(), 
                "Price (INR)": price, 
                "Rating": rating, 
                "Number of Ratings": ratings_count, 
                "Product ID": pid,
                "Result Type": item_data.get('Result Type', 'Direct'), 
                "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "URL": url
            }
        except:
            await page.close()
            return None

    async def run_search(self, search_url):
        try:
            self.update_status("Launching Browser (Visible)...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(viewport={'width':1920,'height':1080})
                page = await context.new_page()
                
                self.update_status("Visiting Flipkart Home...")
                await page.goto("https://www.flipkart.com/", wait_until="domcontentloaded")
                await asyncio.sleep(2)
                
                # Close login popup if it appears
                try:
                    close_btn = await page.query_selector("button._2KpZ6l._2doB4z")
                    if close_btn: await close_btn.click()
                except: pass

                self.update_status("Searching...")
                # If search_url is just a query, construct URL
                if "flipkart.com" not in search_url:
                    search_url = f"https://www.flipkart.com/search?q={urllib.parse.quote(search_url)}"
                
                await page.goto(search_url, wait_until="domcontentloaded")
                await asyncio.sleep(3)

                product_cards = await page.query_selector_all('div[data-id]')
                # Filter out garbage
                product_cards = [c for c in product_cards if await c.query_selector('a')]
                
                if not product_cards:
                     self.update_status("Error: No products found.", done=True)
                     await browser.close()
                     return
                
                self.update_status(f"Found {len(product_cards)} products. Deep Scrape...")
                initial_data = []
                for card in product_cards:
                    link_el = await card.query_selector('a')
                    href = await link_el.get_attribute("href")
                    
                    initial_data.append({
                        "URL": href,
                        "Result Type": "Organic" # Hard to detect sponsored reliably on FK easily
                    })
                
                final = []
                for i, item in enumerate(initial_data):
                    self.update_status(f"Processing {i+1}/{len(initial_data)}...", progress=i+1, total=len(initial_data))
                    d = await self.get_deep_details(context, item)
                    if d: final.append(d)
                    await asyncio.sleep(1) # FK is sensitive
                
                await browser.close()
                
                fname = f"flipkart_results_{self.job_id}.csv"
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
                context = await browser.new_context()
                
                final = []
                for i, url in enumerate(urls):
                    self.update_status(f"Processing {i+1}/{len(urls)}...", progress=i+1, total=len(urls))
                    d = await self.get_deep_details(context, {"URL": url})
                    if d: final.append(d)
                    await asyncio.sleep(1)

                await browser.close()
                fname = f"flipkart_bulk_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            self.update_status(f"Error: {e}", done=True)

    async def run_reviews(self, product_url):
        # Flipkart reviews are tricky.
        # This is a placeholder as full implementation requires complex pagination logic similar to Amazon
        self.update_status("Review scraping not fully implemented for Flipkart yet. returning partial data.", done=True)
