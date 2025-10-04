import csv
import json
import asyncio
import time
from playwright.async_api import async_playwright
import os

class GoatPDPScraper:
    def __init__(self):
        # Configuration - tune these for performance
        self.input_csv = "goat_listings.csv"
        self.output_csv = "goat_variants_complete.csv"
        self.browser = None
        self.page = None
        
        # Performance settings
        self.batch_size = 10  # Save progress every N products
        self.delay_between_requests = 0.5  # Seconds between requests
        self.page_timeout = 15000  # Milliseconds to wait for page load
        self.navigation_wait = "domcontentloaded"  # Faster than "networkidle"
        
    async def setup_browser(self):
        """Setup Playwright browser with CDP connection - optimized for speed"""
        try:
            playwright = await async_playwright().start()
            # Connect to existing Chrome instance
            self.browser = await playwright.chromium.connect_over_cdp("http://localhost:9223")
            context = self.browser.contexts[0]
            self.page = await context.new_page()
            
            # Performance optimizations
            await self.page.set_viewport_size({"width": 800, "height": 600})  # Smaller viewport
            await self.page.add_init_script("""
                // Disable images, CSS, fonts for faster loading
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            """)
            
            print("✓ Browser setup successful (optimized)")
            return True
        except Exception as e:
            print(f"✗ Browser setup failed: {e}")
            return False
    
    async def close_browser(self):
        """Close browser and cleanup"""
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
    
    def read_product_listings(self):
        """Read the product listings from CSV with comprehensive fields"""
        products = []
        try:
            with open(self.input_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    products.append({
                        'product_name': row['Product Name'],
                        'product_id': row['Product ID'],
                        'slug': row.get('Slug', 'N/A'),
                        'product_url': row.get('Product URL', 'N/A'),
                        'api_url': row.get('API URL', 'N/A'),
                        'brand': row.get('Brand', 'N/A'),
                        'silhouette': row.get('Silhouette', 'N/A'),
                        'category': row.get('Category', 'N/A'),
                        'product_type': row.get('Product Type', 'N/A'),
                        'gender': row.get('Gender', 'N/A'),
                        'status': row.get('Status', 'N/A'),
                        'in_stock': row.get('In Stock', 'N/A'),
                        'under_retail': row.get('Under Retail', 'N/A'),
                        'retail_price_usd': row.get('Retail Price (USD)', 'N/A'),
                        'retail_price_cents': row.get('Retail Price (Cents)', 'N/A'),
                        'retail_price_currency': row.get('Retail Price Currency', 'N/A'),
                        'picture_url': row.get('Picture URL', 'N/A'),
                        'season_year': row.get('Season Year', 'N/A'),
                        'season_type': row.get('Season Type', 'N/A'),
                        'activity': row.get('Activity', 'N/A'),
                        'release_date': row.get('Release Date', 'N/A'),
                        'total_variants': row.get('Total Variants', 'N/A'),
                        'url': row.get('API URL', 'N/A')  # Use API URL for variant scraping
                    })
            print(f"✓ Loaded {len(products)} products from {self.input_csv}")
            return products
        except Exception as e:
            print(f"✗ Error reading CSV: {e}")
            return []
    
    async def extract_json_from_page(self, url):
        """Extract JSON data from the page - optimized for speed"""
        try:
            print(f"  Navigating to: {url}")
            # Use faster navigation with minimal wait
            await self.page.goto(url, wait_until=self.navigation_wait, timeout=self.page_timeout)
            
            # Quick check for <pre> tags first (most common case)
            json_data = await self.page.evaluate("""
                () => {
                    const pre = document.querySelector('pre');
                    if (pre && pre.textContent.includes('"sizeOption"')) {
                        try {
                            return JSON.parse(pre.textContent);
                        } catch (e) {
                            return null;
                        }
                    }
                    return null;
                }
            """)
            
            if json_data:
                print(f"    ✓ Found JSON data in <pre> tags")
                return json_data
            
            # Fallback: check if page is pure JSON
            try:
                page_content = await self.page.content()
                if page_content.strip().startswith('['):
                    json_data = json.loads(page_content)
                    if 'sizeOption' in str(json_data):
                        print(f"    ✓ Page is JSON data")
                        return json_data
            except Exception:
                pass
            
            print(f"    ⚠ No JSON data found on page")
            return None
            
        except Exception as e:
            print(f"    ✗ Error extracting data: {e}")
            return None
    
    def parse_variants_data(self, variants_data, product_info):
        """Parse variants data and return CSV rows with comprehensive product + variant info"""
        rows = []
        
        if not variants_data or not isinstance(variants_data, list):
            print(f"    ⚠ No valid variants data found for {product_info['product_name']}")
            return rows
        
        for variant in variants_data:
            try:
                # Extract size information
                size_option = variant.get('sizeOption', {})
                size_presentation = size_option.get('presentation', 'N/A')
                size_value = size_option.get('value', 'N/A')
                
                # Calculate US Size (subtract 1 from the size value)
                us_size = 'N/A'
                if size_value != 'N/A' and size_value is not None:
                    try:
                        us_size = str(float(size_value) - 1)
                    except (ValueError, TypeError):
                        us_size = 'N/A'
                
                # Extract condition information
                shoe_condition = variant.get('shoeCondition', 'N/A')
                box_condition = variant.get('boxCondition', 'N/A')
                
                # Extract stock status
                stock_status = variant.get('stockStatus', 'N/A')
                
                # Extract pricing information (convert cents to dollars)
                lowest_price_data = variant.get('lowestPriceCents', {})
                lowest_price_cents = lowest_price_data.get('amount', 0)
                lowest_price_usd = f"${lowest_price_cents/100:.2f}" if lowest_price_cents else "N/A"
                lowest_price_currency = lowest_price_data.get('currency', 'N/A')
                
                instant_ship_data = variant.get('instantShipLowestPriceCents', {})
                instant_ship_cents = instant_ship_data.get('amount', 0)
                instant_ship_usd = f"${instant_ship_cents/100:.2f}" if instant_ship_cents else "N/A"
                instant_ship_currency = instant_ship_data.get('currency', 'N/A')
                
                last_sold_data = variant.get('lastSoldPriceCents', {})
                last_sold_cents = last_sold_data.get('amount', 0)
                last_sold_usd = f"${last_sold_cents/100:.2f}" if last_sold_cents else "N/A"
                last_sold_currency = last_sold_data.get('currency', 'N/A')
                
                # Extract additional fields from sample.json
                lowest_price_amount_cents = lowest_price_cents if lowest_price_cents else 'N/A'
                instant_ship_amount_cents = instant_ship_cents if instant_ship_cents else 'N/A'
                last_sold_amount_cents = last_sold_cents if last_sold_cents else 'N/A'
                
                # Create comprehensive row with ALL product + variant fields
                row = [
                    # Product Information (from find_listings.py)
                    product_info['product_name'],           # Product Name
                    product_info['product_id'],             # Product ID
                    product_info['slug'],                   # Slug
                    product_info['product_url'],            # Product URL
                    product_info['api_url'],                # API URL
                    product_info['brand'],                  # Brand
                    product_info['silhouette'],             # Silhouette
                    product_info['category'],               # Category
                    product_info['product_type'],           # Product Type
                    product_info['gender'],                 # Gender
                    product_info['status'],                 # Status
                    product_info['in_stock'],               # In Stock
                    product_info['under_retail'],           # Under Retail
                    product_info['retail_price_usd'],       # Retail Price (USD)
                    product_info['retail_price_cents'],     # Retail Price (Cents)
                    product_info['retail_price_currency'],  # Retail Price Currency
                    product_info['picture_url'],            # Picture URL
                    product_info['season_year'],            # Season Year
                    product_info['season_type'],            # Season Type
                    product_info['activity'],               # Activity
                    product_info['release_date'],           # Release Date
                    product_info['total_variants'],         # Total Variants
                    # Variant Information (from API response)
                    size_presentation,                      # Size (Original)
                    us_size,                                # US Size (Size - 1)
                    size_value,                             # Size Value (Numeric)
                    shoe_condition,                         # Shoe Condition
                    box_condition,                          # Box Condition
                    stock_status,                           # Stock Status
                    lowest_price_usd,                       # Lowest Price (USD)
                    lowest_price_amount_cents,              # Lowest Price (Cents)
                    lowest_price_currency,                  # Lowest Price Currency
                    instant_ship_usd,                       # Instant Ship Price (USD)
                    instant_ship_amount_cents,              # Instant Ship Price (Cents)
                    instant_ship_currency,                  # Instant Ship Currency
                    last_sold_usd,                          # Last Sold Price (USD)
                    last_sold_amount_cents,                 # Last Sold Price (Cents)
                    last_sold_currency                      # Last Sold Currency
                ]
                
                rows.append(row)
                
            except Exception as e:
                print(f"    ⚠ Error parsing variant: {e}")
                continue
        
        return rows
    
    def save_progress_csv(self, rows, filename):
        """Save progress to a temporary CSV file"""
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            print(f"    💾 Progress saved to {filename}")
        except Exception as e:
            print(f"    ⚠ Could not save progress: {e}")
    
    async def scrape_all_products(self):
        """Main scraping function"""
        if not await self.setup_browser():
            return
        
        try:
            # Read product listings
            products = self.read_product_listings()
            if not products:
                return
            
            # Prepare CSV data
            all_rows = []
            
            # Add header with comprehensive product info + variant details
            header = [
                # Product Information (from find_listings.py)
                'Product Name', 'Product ID', 'Slug', 'Product URL', 'API URL',
                'Brand', 'Silhouette', 'Category', 'Product Type', 'Gender',
                'Status', 'In Stock', 'Under Retail', 'Retail Price (USD)',
                'Retail Price (Cents)', 'Retail Price Currency',
                'Picture URL', 'Season Year', 'Season Type', 'Activity',
                'Release Date', 'Total Variants',
                # Variant Information (from API response)
                'Size (Original)', 'US Size (Size-1)', 'Size Value (Numeric)',
                'Shoe Condition', 'Box Condition', 'Stock Status',
                'Lowest Price (USD)', 'Lowest Price (Cents)', 'Lowest Price Currency',
                'Instant Ship Price (USD)', 'Instant Ship Price (Cents)', 'Instant Ship Currency',
                'Last Sold Price (USD)', 'Last Sold Price (Cents)', 'Last Sold Currency'
            ]
            all_rows.append(header)
            
            # Process each product with optimized batching
            total_products = len(products)
            successful_products = 0
            
            for i, product in enumerate(products, 1):
                print(f"\n📦 Processing product {i}/{total_products}: {product['product_name']}")
                
                try:
                    # Extract JSON from page
                    json_data = await self.extract_json_from_page(product['url'])
                    
                    if json_data:
                        # Parse variants data
                        variant_rows = self.parse_variants_data(
                            json_data, 
                            product
                        )
                        
                        if variant_rows:
                            all_rows.extend(variant_rows)
                            print(f"    ✓ Added {len(variant_rows)} variant rows")
                            successful_products += 1
                        else:
                            print(f"    ⚠ No variants found")
                    else:
                        print(f"    ⚠ No data extracted")
                    
                    # Reduced delay between requests for speed
                    if i % self.batch_size == 0:
                        print(f"    ⏱️  Batch {i//self.batch_size} completed, saving progress...")
                        # Save progress every batch
                        self.save_progress_csv(all_rows, f"goat_variants_batch_{i//self.batch_size}.csv")
                    
                    await asyncio.sleep(self.delay_between_requests)
                    
                except Exception as e:
                    print(f"    ✗ Error processing product: {e}")
                    continue
            
            # Save to CSV
            if len(all_rows) > 1:  # More than just header
                try:
                    with open(self.output_csv, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerows(all_rows)
                    
                    print(f"\n🎉 Scraping completed successfully!")
                    print(f"📊 Summary:")
                    print(f"   Total products processed: {total_products}")
                    print(f"   Successful products: {successful_products}")
                    print(f"   Total variant rows: {len(all_rows)-1}")
                    print(f"   Output file: {self.output_csv}")
                    
                except Exception as e:
                    print(f"✗ Error saving CSV: {e}")
            else:
                print(f"\n❌ No data to save")
                
        except Exception as e:
            print(f"✗ Scraping error: {e}")
        
        finally:
            await self.close_browser()

async def main():
    """Main function"""
    print("🚀 GOAT Product Detail Page Scraper")
    print("=" * 50)
    
    scraper = GoatPDPScraper()
    
    # Check if input CSV exists
    if not os.path.exists(scraper.input_csv):
        print(f"❌ {scraper.input_csv} not found!")
        print("💡 Please ensure the file exists and contains Product Name, Product ID, and URL columns")
        return
    
    print(f"📁 Input CSV: {scraper.input_csv}")
    print(f"📄 Output CSV: {scraper.output_csv}")
    print(f"🌐 Will scrape {len(scraper.read_product_listings())} product URLs")
    print("-" * 50)
    
    # Start scraping
    await scraper.scrape_all_products()

if __name__ == "__main__":
    asyncio.run(main())
