import asyncio
import pandas as pd
import re
from playwright.async_api import async_playwright

# --- SETTINGS ---
SEARCH_QUERY = "sandwich maker"
SEARCH_URL = f"https://www.amazon.in/s?k={SEARCH_QUERY.replace(' ', '+')}"
LIMIT_PRODUCTS = 10 

async def apply_stealth(page):
    await page.add_init_script("delete Object.getPrototypeOf(navigator).webdriver")
    await page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

def extract_asin(url):
    """Extracts the 10-character ASIN from an Amazon URL."""
    match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", url)
    return match.group(1) if match else "N/A"

async def get_deep_details(context, url):
    """Visits the product page to find ASIN and all Rank data."""
    page = await context.new_page()
    await apply_stealth(page)
    asin = extract_asin(url) # Pull ASIN from the URL immediately
    
    try:
        print(f"Deep scraping ASIN {asin}...")
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # 0. Product Title (Extract from detail page for full accuracy)
        title_el = await page.query_selector("#productTitle")
        title = await title_el.inner_text()
        title = title.strip() if title_el else "N/A"
        
        # Extract all ranks found on the page
        full_text = await page.inner_text("body")
        # Look for the # followed by numbers and 'in [Category]'
        rank_matches = re.findall(r"#(\d+[\d,]*)\s+in\s+([A-Za-z\s&,\-]+)", full_text)
        
        # Helper to clean category name (remove "Feedback" garbage)
        def clean_cat(text):
            return text.split("Feedback")[0].split("Would you like")[0].strip()

        prim_rank_num, prim_rank_cat = "N/A", "N/A"
        sec_rank_num, sec_rank_cat = "N/A", "N/A"

        if len(rank_matches) > 0:
            prim_rank_num = f"#{rank_matches[0][0]}"
            prim_rank_cat = clean_cat(rank_matches[0][1])
        
        if len(rank_matches) > 1:
            sec_rank_num = f"#{rank_matches[1][0]}"
            sec_rank_cat = clean_cat(rank_matches[1][1])

        await page.close()
        return {
            "Product Name": title,
            "ASIN": asin,
            "Primary Rank Number": prim_rank_num,
            "Primary Rank Category": prim_rank_cat,
            "Secondary Rank Number": sec_rank_num,
            "Secondary Rank Category": sec_rank_cat
        }
    except:
        await page.close()
        return {"ASIN": asin, "Primary Rank": "N/A", "Secondary Rank": "N/A"}

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False) 
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await apply_stealth(page)

        print("Warming up (Visiting Home)...")
        await page.goto("https://www.amazon.in/", wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        print(f"Searching Amazon for: {SEARCH_QUERY}...")
        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=45000)

        # Find all product containers
        products = await page.query_selector_all('div[data-component-type="s-search-result"]')
        print(f"Found {len(products)} products on page. Scraping all...")
        
        temp_results = []
        for product in products:
            try:
                # Extract Basic Info
                title_el = await product.query_selector('h2 a span')
                price_el = await product.query_selector('.a-price-whole')
                rating_el = await product.query_selector('i.a-icon-star-small span.a-icon-alt')
                reviews_el = await product.query_selector('span.a-size-base.s-underline-text')
                link_el = await product.query_selector('h2 a')

                if link_el:
                    link = f"https://www.amazon.in{await link_el.get_attribute('href')}"
                    
                    # Detect Result Type
                    result_type = "Organic"
                    text_content = await product.inner_text()
                    
                    # Check Sponsored
                    sp_label = await product.query_selector('.puis-sponsored-label-text')
                    if sp_label or "Sponsored" in text_content[:50]:
                        result_type = "Sponsored"
                    
                    # Check Amazon's Choice
                    choice_label = await product.query_selector('span[aria-label="Amazon\'s Choice"]')
                    if choice_label or "Amazon's Choice" in text_content:
                        if result_type != "Sponsored":
                            result_type = "Amazon's Choice"
                            
                    temp_results.append({
                        "Product Name": (await title_el.inner_text()).strip() if title_el else "N/A",
                        "Price (INR)": (await price_el.inner_text()).replace(",", "").strip() if price_el else "N/A",
                        "Rating": (await rating_el.inner_text()).split()[0] if rating_el else "N/A",
                        "Number of Ratings": (await reviews_el.inner_text()).replace("(", "").replace(")", "").strip() if reviews_el else "0",
                        "Result Type": result_type,
                        "URL": link
                    })
            except: continue

        # Now Deep Scrape for ASIN and Ranks
        final_results = []
        for i, item in enumerate(temp_results):
            print(f"Processing {i+1}/{len(temp_results)}: {item['Product Name'][:30]}...")
            details = await get_deep_details(context, item["URL"])
            if details:
                item.update(details)
                # Ensure Result Type persists if not in details (it isn't returned by deep details logic in this script yet, so we keep it from item)
            final_results.append(item)
            await asyncio.sleep(1.5) # Anti-ban delay

        if final_results:
            df = pd.DataFrame(final_results)
            output_file = "amazon_full_pro_data.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n✅ Success! CSV with ASIN and Secondary Ranks saved to {output_file}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())