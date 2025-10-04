import os
import requests
import time
import csv
import json
import html as html_lib
from typing import Optional, Dict, Any, Tuple
from bs4 import BeautifulSoup
import concurrent.futures

# optional high-fidelity curl_cffi
try:
    from curl_cffi import requests as curl_requests
    HAVE_CURL_CFFI = True
except Exception:
    HAVE_CURL_CFFI = False

import cloudscraper

# ---------- Config ----------
MAX_RETRIES = int(os.getenv("GOAT_MAX_RETRIES", "5"))
TIMEOUT = int(os.getenv("GOAT_TIMEOUT", "30"))

# File paths
INPUT_CSV = "goat_listings.csv"
OUTPUT_CSV = "goat_variants_complete.csv"
BATCH_SIZE = 10  # Save progress every N products
DELAY_BETWEEN_REQUESTS = 0.5  # Seconds between requests
MAX_WORKERS = 10  # Number of parallel workers for fetching product pages

# Proxy configuration (optional)
PROXY_HTTP = os.getenv("PROXY_HTTP") or os.getenv("HTTP_PROXY")
PROXY_HTTPS = os.getenv("PROXY_HTTPS") or os.getenv("HTTPS_PROXY")
PREFERRED_PROXY = os.getenv("GOAT_PROXY")
PROXY_CONFIG: Dict[str, str] = {}
if PREFERRED_PROXY:
    PROXY_CONFIG = {"http": PREFERRED_PROXY, "https": PREFERRED_PROXY}
elif PROXY_HTTP or PROXY_HTTPS:
    if PROXY_HTTP:
        PROXY_CONFIG["http"] = PROXY_HTTP
    if PROXY_HTTPS:
        PROXY_CONFIG["https"] = PROXY_HTTPS

# Cookie/header configuration
# IMPORTANT: Set GOAT_CURRENCY to match the prices you want to see!
# The API returns prices based on the "currency" cookie, NOT just the countryCode parameter
# Common configurations:
#   - Hong Kong location with USD prices: GOAT_COUNTRY="HK", GOAT_CURRENCY="USD"
#   - India location with USD prices: GOAT_COUNTRY="IN", GOAT_CURRENCY="USD"
#   - USA location with USD prices: GOAT_COUNTRY="US", GOAT_CURRENCY="USD"

GOAT_COOKIE = ""  # Paste your full cookie string here if needed
GOAT_COUNTRY = "HK"  # Country code (HK for Hong Kong, IN for India, US for USA, etc.)
GOAT_CURRENCY = "USD"  # Currency code (USD for US Dollar, HKD for HK Dollar, INR for Indian Rupee, etc.)
GOAT_ACCEPT_LANGUAGE = "en-GB,en;q=0.9"
GOAT_CSRF = ""  # Paste your CSRF token here if needed

def build_headers_api(referer: str = "https://www.goat.com") -> dict:
    """Headers for API requests with proper currency and country cookies"""
    headers = {
        "authority": "www.goat.com",
        "accept": "application/json",
        "origin": "https://www.goat.com",
        "referer": referer,
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "accept-language": GOAT_ACCEPT_LANGUAGE or "en-GB,en;q=0.7",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "accept-encoding": "gzip, deflate, br",
        "priority": "u=1, i",
        "sec-gpc": "1",
    }

    # Build cookie string with CRITICAL currency and country settings
    cookie_parts = []

    # Add base cookies from GOAT_COOKIE if provided
    if GOAT_COOKIE:
        cookie_parts.append(GOAT_COOKIE.strip())

    # IMPORTANT: Set currency cookie to get correct prices
    if GOAT_CURRENCY:
        cookie_parts.append(f"currency={GOAT_CURRENCY}")

    # Set locale
    cookie_parts.append("locale=en")
    cookie_parts.append("locale_next=en-us")

    # Set country and locale override
    if GOAT_COUNTRY:
        cookie_parts.append(f"country={GOAT_COUNTRY}")
        cookie_parts.append("localeOverride=true")

    # Add CSRF token to cookies
    if GOAT_CSRF:
        cookie_parts.append(f"csrf={GOAT_CSRF}")

    if cookie_parts:
        headers["cookie"] = "; ".join([p for p in cookie_parts if p])

    # Add csrf header if provided
    if GOAT_CSRF:
        headers["x-csrf-token"] = GOAT_CSRF

    return headers


def build_headers_html() -> dict:
    """Headers for HTML page requests (for GoatFacts extraction)"""
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-GB,en;q=0.7",
        "cache-control": "max-age=0",
        "priority": "u=0, i",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "sec-gpc": "1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "referer": "https://www.goat.com/",
        "accept-encoding": "gzip, deflate, br"
    }
    if GOAT_COOKIE:
        headers["cookie"] = GOAT_COOKIE
    return headers


# ---------- GoatFacts extraction functions ----------

def decode_body(resp) -> str:
    try:
        enc = resp.headers.get("content-encoding", "")
        if "br" in enc:
            import brotli
            return brotli.decompress(resp.content).decode(errors="ignore")
        return resp.text
    except Exception:
        try:
            return resp.text
        except Exception:
            return ""


def extract_story_html(document: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        soup = BeautifulSoup(document, "lxml")
        script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
        if not script_tag or not script_tag.string:
            return None, None
        data = json.loads(script_tag.string)
        pt = data.get("props", {}).get("pageProps", {}).get("productTemplate", {})
        story_html = pt.get("storyHtml")
        if not story_html:
            return None, None
        cleaned_html = html_lib.unescape(story_html)
        plain_text = BeautifulSoup(cleaned_html, "lxml").get_text(separator=" ", strip=True)
        return cleaned_html, plain_text
    except Exception:
        return None, None


def extract_product_meta(document: str) -> Dict[str, Any]:
    try:
        soup = BeautifulSoup(document, "lxml")
        script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
        if not script_tag or not script_tag.string:
            return {}
        data = json.loads(script_tag.string)
        pt = data.get("props", {}).get("pageProps", {}).get("productTemplate", {})
        return {
            "brandName": pt.get("brandName"),
            "color": pt.get("color"),
            "sku": pt.get("sku"),
            "designer": pt.get("designer")
        }
    except Exception:
        return {}


def try_curl_cffi(url: str, headers: dict, proxy: Optional[str] = None):
    if not HAVE_CURL_CFFI:
        return None
    try:
        for persona in ("chrome124", "chrome123", "chrome120"):
            kwargs = {
                "headers": headers,
                "allow_redirects": True,
                "timeout": TIMEOUT,
                "impersonate": persona,
                "verify": True,
            }
            if proxy:
                kwargs["proxies"] = {"http": proxy, "https": proxy}
            try:
                resp = curl_requests.get(url, **kwargs)
                return resp
            except Exception:
                continue
        return None
    except Exception:
        return None


def try_cloudscraper(url: str, headers: dict, proxies: Optional[Dict[str, str]] = None):
    try:
        sess = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "darwin", "desktop": True})
        if proxies:
            sess.proxies.update(proxies)
        resp = sess.get(url, headers=headers, allow_redirects=True, timeout=TIMEOUT)
        return resp
    except Exception:
        return None


def fetch_product_data(product_id: str, country_code: str = "IN") -> Optional[Dict[str, Any]]:
    """
    Resilient fetch with retries/backoff and multiple transport fallbacks.
    Returns parsed JSON dict or None.
    """
    url = f"https://www.goat.com/web-api/v1/product_variants/buy_bar_data?productTemplateId={product_id}&countryCode={country_code}"
    headers = build_headers_api(referer=f"https://www.goat.com/sneakers/{product_id}")

    # proxies for curl (string) or cloudscraper (dict)
    proxy_str = PREFERRED_PROXY or (PROXY_CONFIG.get("http") or PROXY_CONFIG.get("https"))
    proxies_dict = ({"http": proxy_str, "https": proxy_str} if proxy_str else (PROXY_CONFIG or None))

    last_status = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = None
            if HAVE_CURL_CFFI:
                resp = try_curl_cffi(url, headers, proxy_str)
            if resp is None:
                resp = try_cloudscraper(url, headers, proxies_dict)
            if resp is None:
                # final fallback to requests with optional proxies
                sess = requests.Session()
                resp = sess.get(url, headers=headers, timeout=TIMEOUT, proxies=PROXY_CONFIG or None)

            last_status = getattr(resp, "status_code", None) or getattr(resp, "status", None)

            if last_status == 429:
                wait = 2 + attempt
                print(f"Rate limited (429). Backing off {wait}s...")
                time.sleep(wait)
                continue

            if last_status != 200:
                wait = 1 + attempt
                print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] HTTP {last_status} for {url}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            # Parse JSON safely
            try:
                data = resp.json()
            except Exception as e:
                wait = 1 + attempt
                print(f"⚠️ JSON parse error: {e}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            return data
        except Exception as e:
            wait = 1 + attempt
            print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] error fetching {url}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

    print(f"❌ Failed to fetch {url} after {MAX_RETRIES} attempts. last_status={last_status}")
    return None


def fetch_product_page_html(product_url: str) -> Dict[str, Any]:
    """
    Fetch product page HTML and extract GoatFacts data (story_html, story_text, brandName, color, sku, designer).
    This function is designed to be run in parallel via ThreadPoolExecutor.
    Returns a dict with extracted fields and http_status / error.
    """
    hdrs = build_headers_html()
    last_status = None

    # proxies for curl (string) or cloudscraper (dict)
    proxy_str = PREFERRED_PROXY or (PROXY_CONFIG.get("http") or PROXY_CONFIG.get("https"))
    proxies_dict = ({"http": proxy_str, "https": proxy_str} if proxy_str else (PROXY_CONFIG or None))

    for attempt in range(MAX_RETRIES):
        try:
            resp = None
            if HAVE_CURL_CFFI:
                resp = try_curl_cffi(product_url, hdrs, proxy_str)
            if resp is None:
                resp = try_cloudscraper(product_url, hdrs, proxies_dict)

            if resp is None:
                raise RuntimeError("No response (request failed)")

            last_status = getattr(resp, "status_code", None) or getattr(resp, "status", None)

            if last_status == 200:
                body = decode_body(resp)
                cleaned_html, plain_text = extract_story_html(body)
                meta = extract_product_meta(body)

                return {
                    "story_html": cleaned_html or "",
                    "story_text": plain_text or "",
                    "brandName": meta.get("brandName", "") if meta else "",
                    "color": meta.get("color", "") if meta else "",
                    "sku": meta.get("sku", "") if meta else "",
                    "designer": meta.get("designer", "") if meta else "",
                    "http_status": int(last_status),
                    "error": ""
                }
            else:
                wait = 1 + attempt
                time.sleep(wait)
                continue
        except Exception as e:
            wait = 1 + attempt
            time.sleep(wait)
            continue

    # all attempts failed
    return {
        "story_html": "",
        "story_text": "",
        "brandName": "",
        "color": "",
        "sku": "",
        "designer": "",
        "http_status": int(last_status) if last_status else None,
        "error": f"Failed after {MAX_RETRIES} attempts (last_status={last_status})"
    }


def read_product_listings():
    """Read the product listings from CSV with comprehensive fields"""
    products = []
    try:
        # Use utf-8-sig to handle BOM (Byte Order Mark) if present
        with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
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
                })
        print(f"✓ Loaded {len(products)} products from {INPUT_CSV}")
        return products
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return []


def parse_variants_data(variants_data, product_info, html_data):
    """Parse variants data and return CSV rows with comprehensive product + variant + GoatFacts info"""
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

            # Extract pricing information - return exactly as from JSON
            # Lowest Price
            lowest_price_data = variant.get('lowestPriceCents', {})
            lowest_price_amount_cents = lowest_price_data.get('amount', 'N/A')
            lowest_price_currency = lowest_price_data.get('currency', 'N/A')
            # Convert to dollars for display
            if lowest_price_amount_cents != 'N/A':
                lowest_price_usd = f"${lowest_price_amount_cents/100:.2f}"
            else:
                lowest_price_usd = "N/A"

            # Instant Ship Price
            instant_ship_data = variant.get('instantShipLowestPriceCents', {})
            instant_ship_amount_cents = instant_ship_data.get('amount', 'N/A')
            instant_ship_currency = instant_ship_data.get('currency', 'N/A')
            if instant_ship_amount_cents != 'N/A':
                instant_ship_usd = f"${instant_ship_amount_cents/100:.2f}"
            else:
                instant_ship_usd = "N/A"

            # Last Sold Price
            last_sold_data = variant.get('lastSoldPriceCents', {})
            last_sold_amount_cents = last_sold_data.get('amount', 'N/A')
            last_sold_currency = last_sold_data.get('currency', 'N/A')
            if last_sold_amount_cents != 'N/A':
                last_sold_usd = f"${last_sold_amount_cents/100:.2f}"
            else:
                last_sold_usd = "N/A"

            # Create comprehensive row with ALL product + variant + GoatFacts fields
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
                last_sold_currency,                     # Last Sold Currency
                # GoatFacts fields (from HTML page)
                html_data.get('story_html', ''),        # story_html
                html_data.get('story_text', ''),        # story_text
                html_data.get('brandName', ''),         # brandName
                html_data.get('color', ''),             # color
                html_data.get('sku', ''),               # sku
                html_data.get('designer', ''),          # designer
                html_data.get('http_status', ''),       # http_status
                html_data.get('error', '')              # error
            ]

            rows.append(row)

        except Exception as e:
            print(f"    ⚠ Error parsing variant: {e}")
            continue

    return rows


def save_progress_csv(rows, filename):
    """Save progress to a temporary CSV file"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        print(f"    💾 Progress saved to {filename}")
    except Exception as e:
        print(f"    ⚠ Could not save progress: {e}")


def append_rows_to_csv(rows, filename, write_header=False):
    """Append rows to CSV file"""
    try:
        mode = 'w' if write_header else 'a'
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        if write_header:
            print(f"    💾 Created CSV with header: {filename}")
        else:
            print(f"    💾 Appended {len(rows)} rows to: {filename}")
    except Exception as e:
        print(f"    ⚠ Could not append to CSV: {e}")


def fetch_product_complete(product_info):
    """
    Fetch both HTML page data and API variant data for a product in parallel.
    This function is designed to be run via ThreadPoolExecutor.
    Returns tuple: (product_info, html_data, api_data)

    NOTE: Uses GOAT_COUNTRY and GOAT_CURRENCY settings to fetch prices in the correct currency
    """
    product_id = product_info['product_id']
    product_url = product_info['product_url']
    country_code = GOAT_COUNTRY  # Use the configured country (e.g., "HK" for Hong Kong)

    # Use ThreadPoolExecutor to fetch HTML and API data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks
        html_future = executor.submit(fetch_product_page_html, product_url)
        api_future = executor.submit(fetch_product_data, product_id, country_code)

        # Wait for both to complete
        html_data = html_future.result()
        api_data = api_future.result()

    return (product_info, html_data, api_data)


def scrape_all_products():
    """Main scraping function with parallel HTML + API fetching and batch CSV appending"""
    # Read product listings
    products = read_product_listings()
    if not products:
        return

    # Define header with comprehensive product info + variant details + GoatFacts fields
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
        'Last Sold Price (USD)', 'Last Sold Price (Cents)', 'Last Sold Currency',
        # GoatFacts fields (from HTML page)
        'story_html', 'story_text', 'brandName', 'color', 'sku', 'designer', 'http_status', 'error'
    ]

    # Write header to CSV first
    append_rows_to_csv([header], OUTPUT_CSV, write_header=True)

    # Process products in parallel batches
    total_products = len(products)
    successful_products = 0
    total_variant_rows = 0

    # Process products in batches
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for batch_start in range(0, total_products, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_products)
            batch_products = products[batch_start:batch_end]

            print(f"\n🔄 Processing batch {batch_start//BATCH_SIZE + 1} (products {batch_start+1}-{batch_end} of {total_products}) with {MAX_WORKERS} parallel workers")
            print(f"   Each product fetches HTML + API data in parallel")

            # Submit all products in this batch for parallel processing
            future_to_product = {
                executor.submit(fetch_product_complete, product): product
                for product in batch_products
            }

            # Collect results as they complete
            batch_rows = []
            for fut in concurrent.futures.as_completed(future_to_product):
                product = future_to_product[fut]
                try:
                    product_info, html_data, api_data = fut.result()

                    print(f"  ✓ Completed: {product_info['product_name']}")

                    if api_data:
                        # Parse variants data
                        variant_rows = parse_variants_data(api_data, product_info, html_data)

                        if variant_rows:
                            batch_rows.extend(variant_rows)
                            total_variant_rows += len(variant_rows)
                            print(f"    ✓ Added {len(variant_rows)} variant rows with GoatFacts data")
                            successful_products += 1
                        else:
                            print(f"    ⚠ No variants found")
                    else:
                        print(f"    ⚠ No API data extracted")

                except Exception as exc:
                    print(f"  ✗ Exception processing {product['product_name']}: {exc}")
                    continue

            # Append this batch to CSV
            if batch_rows:
                print(f"\n  ⏱️  Batch {batch_start//BATCH_SIZE + 1} completed, appending {len(batch_rows)} rows to CSV...")
                append_rows_to_csv(batch_rows, OUTPUT_CSV, write_header=False)

            # Small pause between batches
            if batch_end < total_products:
                time.sleep(1.0)

    # Final summary
    print(f"\n🎉 Scraping completed successfully!")
    print(f"📊 Summary:")
    print(f"   Total products processed: {total_products}")
    print(f"   Successful products: {successful_products}")
    print(f"   Total variant rows: {total_variant_rows}")
    print(f"   Output file: {OUTPUT_CSV}")


if __name__ == "__main__":
    print("🚀 GOAT Product Detail Page API Scraper")
    print("=" * 50)

    # Check if input CSV exists
    if not os.path.exists(INPUT_CSV):
        print(f"❌ {INPUT_CSV} not found!")
        print("💡 Please ensure the file exists and contains Product Name, Product ID, and URL columns")
    else:
        print(f"📁 Input CSV: {INPUT_CSV}")
        print(f"📄 Output CSV: {OUTPUT_CSV}")
        print(f"🌐 Will scrape {len(read_product_listings())} product URLs")
        print("-" * 50)

        # Start scraping
        scrape_all_products()
