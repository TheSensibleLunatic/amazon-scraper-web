import asyncio
import random
import re
import urllib.parse
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

class AmazonScraper:
    def __init__(self, job_id, jobs_dict):
        self.job_id = job_id
        self.jobs = jobs_dict # Reference to global JOBS dict to update status

    def update_status(self, status, progress=None, total=None, done=False, filename=None):
        self.jobs[self.job_id]['status'] = status
        if progress: self.jobs[self.job_id]['progress'] = progress
        if total: self.jobs[self.job_id]['total'] = total
        if done: self.jobs[self.job_id]['done'] = True
        if filename: self.jobs[self.job_id]['filename'] = filename

    async def simulate_human_behavior(self, page):
        for _ in range(3):
            await page.mouse.move(random.randint(100, 1000), random.randint(100, 800), steps=10)
            await asyncio.sleep(0.2)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await asyncio.sleep(0.5)
        await page.evaluate("window.scrollTo(0, 0)")

    def extract_asin(self, url):
        match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", url)
        return match.group(1) if match else "N/A"

    async def get_deep_details(self, context, item_data):
        url = item_data['URL']
        asin = self.extract_asin(url)
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            title_el = await page.query_selector("#productTitle")
            title = await title_el.inner_text() if title_el else "N/A"
            
            price_el = await page.query_selector(".a-price-whole")
            price = (await price_el.inner_text()).replace(",", "").strip().rstrip('.') if price_el else "N/A"

            rating_el = await page.query_selector("span.a-icon-alt")
            rating = (await rating_el.inner_text()).split()[0] if rating_el else "N/A"
            
            reviews_el = await page.query_selector("#acrCustomerReviewText")
            reviews = "".join(filter(str.isdigit, await reviews_el.inner_text())) if reviews_el else "0"

            bought_el = await page.query_selector("#social-proofing-faceout-title-text span")
            if not bought_el: bought_el = await page.query_selector(".social-proofing-faceout-title-text span")
            bought_count = await bought_el.inner_text() if bought_el else "N/A"

            full_text = await page.inner_text("body")
            rank_matches = re.findall(r"#(\d+[\d,]*)\s+in\s+([A-Za-z\s&,\-]+)", full_text)
            
            def clean_cat(text): return text.split("Feedback")[0].split("Would you like")[0].strip()
            prim_rank_num, prim_rank_cat = "N/A", "N/A"
            sec_rank_num, sec_rank_cat = "N/A", "N/A"
            if len(rank_matches) > 0:
                prim_rank_num, prim_rank_cat = f"#{rank_matches[0][0]}", clean_cat(rank_matches[0][1])
            if len(rank_matches) > 1:
                sec_rank_num, sec_rank_cat = f"#{rank_matches[1][0]}", clean_cat(rank_matches[1][1])

            await page.close()
            return {
                "Product Name": title.strip(), "Price (INR)": price, "Rating": rating, 
                "Number of Ratings": reviews, "ASIN": asin,
                "Primary Rank Number": prim_rank_num, "Primary Rank Category": prim_rank_cat,
                "Secondary Rank Number": sec_rank_num, "Secondary Rank Category": sec_rank_cat,
                "Result Type": item_data['Result Type'], 
                "Bought in past month": bought_count,
                "Date Scraped": item_data.get('Date Scraped', 'N/A'),
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
                context = await browser.new_context(viewport={'width':1920,'height':1080}, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                page = await context.new_page()
                
                Stealth().apply_stealth_sync(page) 
                
                self.update_status("Visiting Home (Cookie Warmup)...")
                await page.goto("https://www.amazon.in/", wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3)

                self.update_status("Searching... (REFRESH IF BLOCKED!)")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await self.simulate_human_behavior(page)

                product_cards = []
                for attempt in range(40):
                    product_cards = await page.query_selector_all('div[data-component-type="s-search-result"]')
                    if len(product_cards) > 0: break
                    self.update_status(f"Waiting... ({40-attempt}). REFRESH PAGE manually if needed!")
                    await asyncio.sleep(5)
                
                if not product_cards:
                     self.update_status("Error: Timeout/No products.", done=True)
                     await browser.close()
                     return
                
                self.update_status(f"Found {len(product_cards)} products. Deep Scrape...")
                initial_data = []
                for card in product_cards:
                    link_el = await card.query_selector("h2 a")
                    if not link_el: link_el = await card.query_selector("a.a-link-normal.s-no-outline")
                    if not link_el: continue
                    
                    href = await link_el.get_attribute("href")
                    t_content = await card.inner_text()
                    r_type = "Organic"
                    if await card.query_selector('.puis-sponsored-label-text') or "Sponsored" in t_content[:50]: r_type = "Sponsored"
                    elif await card.query_selector('span[aria-label="Amazon\'s Choice"]') or "Amazon's Choice" in t_content: r_type = "Amazon's Choice"
                    
                    initial_data.append({
                        "URL": f"https://www.amazon.in{href}",
                        "Result Type": r_type,
                        "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                final = []
                for i, item in enumerate(initial_data):
                    self.update_status(f"Processing {i+1}/{len(initial_data)}...", progress=i+1, total=len(initial_data))
                    d = await self.get_deep_details(context, item)
                    if d: final.append(d)
                    await asyncio.sleep(random.uniform(2, 4))
                
                await browser.close()
                
                try:
                    parsed = urllib.parse.urlparse(search_url)
                    q = urllib.parse.parse_qs(parsed.query).get('k', ['search'])[0]
                    fname = f"amazon_scrapped_results_{re.sub(r'[^a-zA-Z0-9]', '_', q)}.csv"
                except: fname = f"amazon_scrapped_results_{self.job_id}.csv"
                
                pd.DataFrame(final).to_csv(fname, index=False, encoding='utf-8-sig')
                self.update_status("Done!", done=True, filename=fname)
        except Exception as e:
            print(f"Error: {e}")
            self.update_status(f"Error: {e}", done=True)

    async def run_bulk(self, url_text):
        try:
            urls = [u.strip() for u in re.split(r'[,\n ]', url_text) if u.strip()]
            if not urls:
                self.update_status("Error: No Valid URLs found.", done=True)
                return

            self.update_status("Launching Browser (Visible)...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(viewport={'width':1920,'height':1080}, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                
                final = []
                for i, url in enumerate(urls):
                    self.update_status(f"Scraping Product {i+1}/{len(urls)}...", progress=i+1, total=len(urls))
                    
                    if not url.startswith("http"): url = f"https://www.amazon.in{url}" if url.startswith("/") else f"https://{url}"

                    item = {
                        "URL": url,
                        "Result Type": "Direct URL",
                        "Date Scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    d = await self.get_deep_details(context, item)
                    if d: final.append(d)
                    
                    await asyncio.sleep(random.uniform(2, 4))

                await browser.close()
                
                fname = f"amazon_bulk_results_{self.job_id}.xlsx"
                pd.DataFrame(final).to_excel(fname, index=False)
                self.update_status("Done!", done=True, filename=fname)

        except Exception as e:
            print(f"Bulk Error: {e}")
            self.update_status(f"Error: {e}", done=True)

    async def run_reviews(self, product_url):
        try:
            self.update_status("Launching Browser (Visible)...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(viewport={'width':1920,'height':1080})
                page = await context.new_page()
                Stealth().apply_stealth_sync(page)
                
                self.update_status("Visiting Home...")
                await page.goto("https://www.amazon.in/", wait_until="domcontentloaded")
                await asyncio.sleep(2)
                
                asin = self.extract_asin(product_url)
                review_url = f"https://www.amazon.in/product-reviews/{asin}/?reviewerType=all_reviews"
                self.update_status("Navigating directly to Review Page...")
                await page.goto(review_url, wait_until="domcontentloaded")

                # Handle login redirects check (simplified from original for brevity, but retaining core logic)
                while "/ap/signin" in page.url:
                    self.update_status("Amazon asks for Login. PLEASE FINISH MANUALLY!")
                    await asyncio.sleep(5)
                
                try:
                    await page.wait_for_selector("div[data-hook='review']", timeout=10000)
                except:
                    pass

                reviews_data = []
                page_num = 1
                MAX_PAGES = 50 # Cap for now
                
                while page_num <= MAX_PAGES:
                    self.update_status(f"Scraping Reviews Page {page_num}...")
                    
                    await asyncio.sleep(2)
                    cards = await page.query_selector_all("div[data-hook='review']")
                    if not cards: break
                    
                    for card in cards:
                        try:
                            name = await (await card.query_selector(".a-profile-name")).inner_text()
                            rating = (await (await card.query_selector("i[data-hook='review-star-rating'] span.a-icon-alt")).inner_text()).split()[0]
                            date = await (await card.query_selector("span[data-hook='review-date']")).inner_text()
                            body = await (await card.query_selector("span[data-hook='review-body']")).inner_text()
                            
                            reviews_data.append({
                                "Reviewer Name": name, "Rating": rating, "Review Date": date, "Review Text": body.strip()
                            })
                        except: continue
                    
                    next_btn = await page.query_selector("li.a-last a")
                    if next_btn:
                        await next_btn.click()
                        await page.wait_for_load_state("domcontentloaded")
                        page_num += 1
                    else:
                        break

                await browser.close()
                
                fname = f"amazon_reviews_{asin}.csv"
                pd.DataFrame(reviews_data).to_csv(fname, index=False, encoding='utf-8-sig')
                self.update_status("Done!", done=True, filename=fname)

        except Exception as e:
            print(f"Review Error: {e}")
            self.update_status(f"Error: {e}", done=True)
