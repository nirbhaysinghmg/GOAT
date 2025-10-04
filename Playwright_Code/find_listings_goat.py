import asyncio
import json
import time
import csv
from playwright.async_api import async_playwright
from urllib.parse import urlencode

# CONFIGURATION
MAX_PAGES = 15  # Set to a number (e.g., 5) to limit pages, or None for unlimited scraping

async def make_goat_request(page_number=1):
    """
    Use Playwright to navigate to GOAT search page and extract API response from network tab
    """
    
    # Search URL
    search_url = f"https://www.goat.com/sneakers/brand/salomon?pageNumber={page_number}&inStock=true&genders=women"
    
    try:
        async with async_playwright() as p:
            # Launch a fresh Chromium instance (no CDP)
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
            # Create a new page in the new context
            page = await context.new_page()
            
            # Enable network tracking
            await page.route("**/*", lambda route: route.continue_())
            
            # Store API responses
            api_responses = []
            
            def handle_response(response):
                if "get-product-search-results" in response.url:
                    api_responses.append({
                        'url': response.url,
                        'status': response.status,
                        'response': response
                    })
            
            page.on("response", handle_response)
            
            print(f"Navigating to: {search_url}")
            await page.goto(search_url, wait_until="networkidle")
            
            # Wait a bit for any additional API calls
            await page.wait_for_timeout(3000)
            
            # Look for the API response
            api_data = None
            for api_response in api_responses:
                if api_response['status'] == 200:
                    try:
                        response_body = await api_response['response'].body()
                        api_data = json.loads(response_body.decode('utf-8'))
                        print(f"Found API response for page {page_number}")
                        break
                    except Exception as e:
                        print(f"Error parsing API response: {e}")
                        continue
            
            await page.close()
            await context.close()
            await browser.close()
            
            if api_data:
                return api_data
            else:
                print(f"No API response found for page {page_number}")
                return None
                
    except Exception as e:
        print(f"Playwright error: {e}")
        return None

async def scrape_all_pages():
    """
    Scrape pages based on MAX_PAGES configuration
    If MAX_PAGES is None: scrape until no more results are returned
    If MAX_PAGES is a number: scrape up to that many pages
    """
    all_data = []
    page_number = 1
    
    if MAX_PAGES is None:
        print("Starting to scrape GOAT listings (unlimited pages)...")
    else:
        print(f"Starting to scrape GOAT listings (max {MAX_PAGES} pages)...")
    
    while True:
        # Check if we've reached the maximum page limit
        if MAX_PAGES is not None and page_number > MAX_PAGES:
            print(f"Reached maximum page limit ({MAX_PAGES}). Stopping.")
            break
            
        print(f"Scraping page {page_number}...")
        
        # Make request for current page
        data = await make_goat_request(page_number)
        
        if data is None:
            print(f"No data returned for page {page_number}. Stopping.")
            break
        
        # Check if there are products in the response
        if 'data' not in data or 'productsList' not in data['data'] or len(data['data']['productsList']) == 0:
            print(f"No products found on page {page_number}. Stopping.")
            break
        
        # Add products to our collection
        all_data.extend(data['data']['productsList'])
        print(f"Found {len(data['data']['productsList'])} products on page {page_number}")
        
        # Save data after each page
        if page_number == 1:
            # First page - create new CSV file
            save_to_csv(data['data']['productsList'], "goat_listings.csv", mode='w')
        else:
            # Subsequent pages - append to existing CSV
            append_to_csv(data['data']['productsList'], "goat_listings.csv")
        
        # Check if this is the last page (only if MAX_PAGES is None - unlimited mode)
        if MAX_PAGES is None:
            total_count = data['data'].get('totalResults', 0)
            page_limit = int(data.get('pageLimit', 12))
            
            if len(all_data) >= total_count or len(data['data']['productsList']) < page_limit:
                print(f"Reached end of results. Total products: {len(all_data)}")
                break
        
        page_number += 1
        
        # Add a small delay to be respectful to the server
        await asyncio.sleep(1)
    
    print(f"Scraping completed. Total products collected: {len(all_data)}")
    return all_data

def save_to_json(data, filename="goat_listings.json"):
    """
    Save the scraped data to a JSON file
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to {filename}")

def save_to_csv(data, filename="goat_listings.csv", mode='w'):
    """
    Save the scraped data to a comprehensive CSV file with all important fields
    mode='w' for new file, mode='a' for append
    """
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header only for new files
        if mode == 'w':
            writer.writerow([
                'Product Name', 'Product ID', 'Slug', 'Product URL', 'API URL',
                'Brand', 'Silhouette', 'Category', 'Product Type', 'Gender',
                'Status', 'In Stock', 'Under Retail', 'Retail Price (USD)',
                'Retail Price (Cents)', 'Retail Price Currency',
                'Picture URL', 'Season Year', 'Season Type', 'Activity',
                'Release Date', 'Total Variants'
            ])
        
        # Write data rows
        for product in data:
            # Basic product info
            product_name = product.get('title', 'N/A')
            product_id = product.get('id', 'N/A')
            slug = product.get('slug', 'N/A')
            
            # URLs
            product_url = f"https://www.goat.com/sneakers/{slug}" if slug != 'N/A' else 'N/A'
            api_url = f"https://www.goat.com/web-api/v1/product_variants/buy_bar_data?productTemplateId={product_id}&countryCode=HK"
            
            # Brand and category info
            brand = product.get('brandName', 'N/A')
            silhouette = product.get('silhouette', 'N/A')
            category = product.get('category', 'N/A')
            product_type = product.get('productType', 'N/A')
            gender = product.get('gender', 'N/A')
            
            # Status and availability
            status = product.get('status', 'N/A')
            in_stock = product.get('inStock', 'N/A')
            under_retail = product.get('underRetail', 'N/A')
            
            # Retail pricing
            retail_price_data = product.get('localizedRetailPriceCents', {})
            retail_price_cents = retail_price_data.get('amountCents', 0)
            retail_price_usd = f"${retail_price_cents/100:.2f}" if retail_price_cents else "N/A"
            retail_price_currency = retail_price_data.get('currency', 'N/A')
            
            # Media and metadata
            picture_url = product.get('pictureUrl', 'N/A')
            season_year = product.get('seasonYear', 'N/A')
            season_type = product.get('seasonType', 'N/A')
            activity = ', '.join(product.get('activitiesList', [])) if product.get('activitiesList') else 'N/A'
            
            # Release date (convert from Unix timestamp)
            release_date = 'N/A'
            release_timestamp = product.get('releaseDate', {}).get('seconds', 0)
            if release_timestamp:
                try:
                    release_date = time.strftime('%Y-%m-%d', time.localtime(release_timestamp))
                except:
                    release_date = 'N/A'
            
            # Variants count
            variants_list = product.get('variantsList', [])
            total_variants = len(variants_list)
            
            # Write comprehensive row
            writer.writerow([
                product_name, product_id, slug, product_url, api_url,
                brand, silhouette, category, product_type, gender,
                status, in_stock, under_retail, retail_price_usd,
                retail_price_cents, retail_price_currency,
                picture_url, season_year, season_type, activity,
                release_date, total_variants
            ])
    
    if mode == 'w':
        print(f"Comprehensive data saved to {filename}")
    else:
        print(f"Additional comprehensive data appended to {filename}")

def append_to_csv(data, filename="goat_listings.csv"):
    """
    Append new data to existing CSV file
    """
    save_to_csv(data, filename, mode='a')

async def main():
    # Test with a single request first
    print("Testing single request...")
    test_data = await make_goat_request(1)
    
    if test_data:
        print("✓ Single request successful!")
        print(f"Sample response keys: {list(test_data.keys())}")
        
        if 'data' in test_data and 'productsList' in test_data['data']:
            print(f"Found {len(test_data['data']['productsList'])} products on page 1")
            
            # Uncomment the line below to scrape all pages
            all_products = await scrape_all_pages()
            save_to_json(all_products)
            # CSV is now saved after each page automatically
            
        else:
            print("No 'data' or 'productsList' key found in response")
            print("Full response:", json.dumps(test_data, indent=2))
    else:
        print("✗ Request failed")

if __name__ == "__main__":
    asyncio.run(main())
