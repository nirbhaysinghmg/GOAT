import os
import requests
import urllib.parse
import json
import time
from typing import Optional, Dict, Any

# optional high-fidelity curl_cffi
try:
    from curl_cffi import requests as curl_requests
    HAVE_CURL_CFFI = True
except Exception:
    HAVE_CURL_CFFI = False

import cloudscraper

SEARCH_URLS = [
    "https://www.goat.com/sneakers/brand/salomon?pageSlug=sneakers&brandName=salomon&pageNumber=1&inStock=true&genders=women",
    "https://www.goat.com/sneakers/brand/salomon?pageNumber=1&inStock=true&genders=men"
]


BASE_API = "https://www.goat.com/web-api/consumer-search/get-product-search-results"

# ---------- Config (env-driven, similar to GoatFacts.py) ----------
MAX_RETRIES = int(os.getenv("GOAT_MAX_RETRIES", "5"))
TIMEOUT = int(os.getenv("GOAT_TIMEOUT", "30"))

# Proxy/env configuration (optional)
PROXY_HTTP = os.getenv("PROXY_HTTP") or os.getenv("HTTP_PROXY")
PROXY_HTTPS = os.getenv("PROXY_HTTPS") or os.getenv("HTTPS_PROXY")
PREFERRED_PROXY = os.getenv("GOAT_PROXY")  # single proxy override
PROXY_CONFIG: Dict[str, str] = {}
if PREFERRED_PROXY:
    PROXY_CONFIG = {"http": PREFERRED_PROXY, "https": PREFERRED_PROXY}
elif PROXY_HTTP or PROXY_HTTPS:
    if PROXY_HTTP:
        PROXY_CONFIG["http"] = PROXY_HTTP
    if PROXY_HTTPS:
        PROXY_CONFIG["https"] = PROXY_HTTPS

# Optional cookie/header overrides (hardcoded)
# Paste your real cookie/CSRF if needed. Region defaults to HK.
GOAT_COOKIE = ""  # e.g., '__cf_bm=...; _goat_session=...; cf_clearance=...'
GOAT_COUNTRY = "HK"
GOAT_ACCEPT_LANGUAGE = "en-GB,en;q=0.9"
GOAT_CSRF = ""  # e.g., 'EXNo10Eh-...'

def build_api_url(page_url: str, page_number: int) -> str:
    parsed = urllib.parse.urlparse(page_url)
    params = urllib.parse.parse_qs(parsed.query)

    # Extract gender from query params (e.g., genders=women)
    gender = (params.get("genders", [""])[0] or "").strip()

    # Extract brand from path segment if URL looks like /sneakers/brand/<brand>
    # Example: https://www.goat.com/sneakers/brand/salomon?... → brand = "salomon"
    brand = ""
    try:
        path_parts = [p for p in (parsed.path or "").split("/") if p]
        if "brand" in path_parts:
            idx = path_parts.index("brand")
            if idx + 1 < len(path_parts):
                brand = path_parts[idx + 1]
    except Exception:
        brand = ""

    # Optional silhouette param (rare on these URLs, but keep parity)
    silhouette = params.get("silhouette", [""])[0]

    product_filter = {
        "conditions": [],
        "sizes": [],
        "categories": ["footwear"],
        "releaseYears": [],
        "activities": [],
        "brands": [brand] if brand else [],
        "colors": [],
        "genders": [gender] if gender else [],
        "productTypes": [],
        "silhouettes": [silhouette] if silhouette else []
    }

    raw_params = {
        "inStock": "true",
        "salesChannelId": "1",
        "queryString": "",
        "sortType": "1",
        "pageLimit": "12",
        "pageNumber": str(page_number),
        # Match browser request more closely
        "includeAggregations": "true",
        "collectionSlug": "",
        "priceCentsMin": "",
        "priceCentsMax": "",
        "productFilter": json.dumps(product_filter),
        # We intentionally omit undefined flags; server does not require them
        # "sale": "undefined",
        # "instantShip": "undefined",
        # "underRetail": "undefined",
        # "pageCount": "3",
    }

    # Remove empty or undefined values
    query_params = {k: v for k, v in raw_params.items() if v not in (None, "", "undefined")}

    return BASE_API + "?" + urllib.parse.urlencode(query_params)

def build_headers(page_url: str) -> dict:
    headers = {
        "authority": "www.goat.com",
        "accept": "application/json, text/plain, */*",
        "origin": "https://www.goat.com",
        "referer": page_url,
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "accept-language": GOAT_ACCEPT_LANGUAGE or "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "accept-encoding": "gzip, deflate, br",
        "connection": "keep-alive",
        "sec-gpc": "1",
    }
    # Build cookie string with optional country and csrf
    cookie_parts = []
    if GOAT_COOKIE:
        cookie_parts.append(GOAT_COOKIE.strip())
    if GOAT_COUNTRY:
        cookie_parts.append(f"country={GOAT_COUNTRY}")
        cookie_parts.append("localeOverride=true")
    if GOAT_CSRF:
        cookie_parts.append(f"csrf={GOAT_CSRF}")
    if cookie_parts:
        headers["cookie"] = "; ".join([p for p in cookie_parts if p])
    # Add csrf header if provided
    if GOAT_CSRF:
        headers["x-csrf-token"] = GOAT_CSRF
    return headers


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
        # if no cookie in headers but GOAT_COOKIE provided, add it
        if "cookie" not in headers and GOAT_COOKIE:
            headers = dict(headers)
            headers["cookie"] = GOAT_COOKIE
        resp = sess.get(url, headers=headers, allow_redirects=True, timeout=TIMEOUT)
        return resp
    except Exception:
        return None


def fetch_json_page(page_url: str, page_number: int) -> Optional[Dict[str, Any]]:
    """
    Resilient JSON fetch with retries/backoff and multiple transport fallbacks.
    Returns parsed JSON dict or None.
    """
    api_url = build_api_url(page_url, page_number)
    headers = build_headers(page_url)

    # proxies for curl (string) or cloudscraper (dict)
    proxy_str = PREFERRED_PROXY or (PROXY_CONFIG.get("http") or PROXY_CONFIG.get("https"))
    proxies_dict = ({"http": proxy_str, "https": proxy_str} if proxy_str else (PROXY_CONFIG or None))

    last_status = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = None
            if HAVE_CURL_CFFI:
                resp = try_curl_cffi(api_url, headers, proxy_str)
            if resp is None:
                resp = try_cloudscraper(api_url, headers, proxies_dict)
            if resp is None:
                # final fallback to requests with optional proxies
                sess = requests.Session()
                resp = sess.get(api_url, headers=headers, timeout=TIMEOUT, proxies=PROXY_CONFIG or None)

            last_status = getattr(resp, "status_code", None) or getattr(resp, "status", None)

            if last_status == 429:
                wait = 2 + attempt
                print(f"Rate limited (429). Backing off {wait}s...")
                time.sleep(wait)
                continue

            if last_status != 200:
                wait = 1 + attempt
                print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] HTTP {last_status} for {api_url}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            # Parse JSON safely
            try:
                data = resp.json()
            except Exception:
                try:
                    data = json.loads(resp.text)
                except Exception as e:
                    wait = 1 + attempt
                    print(f"⚠️ JSON parse error: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

            return data
        except Exception as e:
            wait = 1 + attempt
            print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] error fetching {api_url}: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

    print(f"❌ Failed to fetch {api_url} after {MAX_RETRIES} attempts. last_status={last_status}")
    return None

def fetch_all_products(page_url: str):
    page = 1
    all_products = []
    page_limit = 12  # keep in sync with query_params

    while True:
        print(f"Fetching page {page} → {build_api_url(page_url, page)}")
        data = fetch_json_page(page_url, page)
        if data is None:
            print("No data returned (giving up this URL).")
            break
        products = data.get("data", {}).get("productsList", [])

        if not products:
            print("No more products.")
            break

        all_products.extend(products)
        # Stop if last page (fewer than pageLimit results)
        if len(products) < page_limit:
            break

        page += 1
        time.sleep(0.5)

    return all_products

def save_to_json(data, filename="goat_listings.json"):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Data saved to {filename}")

def save_to_csv(data, filename="goat_listings.csv", mode='w'):
    import csv as _csv
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = _csv.writer(f)
        if mode == 'w':
            writer.writerow([
                'Product Name', 'Product ID', 'Slug', 'Product URL', 'API URL',
                'Brand', 'Silhouette', 'Category', 'Product Type', 'Gender',
                'Status', 'In Stock', 'Under Retail', 'Retail Price (USD)',
                'Retail Price (Cents)', 'Retail Price Currency',
                'Picture URL', 'Season Year', 'Season Type', 'Activity',
                'Release Date', 'Total Variants'
            ])
        for product in data:
            product_name = product.get('title', 'N/A')
            product_id = product.get('id', 'N/A')
            slug = product.get('slug', 'N/A')
            product_url = f"https://www.goat.com/sneakers/{slug}" if slug != 'N/A' else 'N/A'
            api_url = f"https://www.goat.com/web-api/v1/product_variants/buy_bar_data?productTemplateId={product_id}&countryCode=HK"
            brand = product.get('brandName', 'N/A')
            silhouette = product.get('silhouette', 'N/A')
            category = product.get('category', 'N/A')
            product_type = product.get('productType', 'N/A')
            gender = product.get('gender', 'N/A')
            status = product.get('status', 'N/A')
            in_stock = product.get('inStock', 'N/A')
            under_retail = product.get('underRetail', 'N/A')
            retail_price_data = product.get('localizedRetailPriceCents', {})
            retail_price_cents = retail_price_data.get('amountCents', 0)
            retail_price_usd = f"${retail_price_cents/100:.2f}" if retail_price_cents else "N/A"
            retail_price_currency = retail_price_data.get('currency', 'N/A')
            picture_url = product.get('pictureUrl', 'N/A')
            season_year = product.get('seasonYear', 'N/A')
            season_type = product.get('seasonType', 'N/A')
            activity = ', '.join(product.get('activitiesList', [])) if product.get('activitiesList') else 'N/A'
            release_date = 'N/A'
            release_timestamp = product.get('releaseDate', {}).get('seconds', 0)
            if release_timestamp:
                try:
                    release_date = time.strftime('%Y-%m-%d', time.localtime(release_timestamp))
                except:
                    release_date = 'N/A'
            variants_list = product.get('variantsList', [])
            total_variants = len(variants_list)
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
    save_to_csv(data, filename, mode='a')

def scrape_all_pages():
    all_data = []
    wrote_header = False
    written_product_ids = set()
    for base_url in SEARCH_URLS:
        print(f"\n=== Starting silhouette: {base_url} ===")
        page_number = 1
        while True:
            print(f"Scraping page {page_number}...")
            data = fetch_json_page(base_url, page_number)
            if data is None:
                print(f"No data returned for page {page_number}. Moving to next silhouette.")
                break
            if 'data' not in data or 'productsList' not in data['data'] or len(data['data']['productsList']) == 0:
                print(f"No products found on page {page_number}. Moving to next silhouette.")
                break
            page_products = data['data']['productsList']
            new_products = []
            for product in page_products:
                product_id = product.get('id')
                if not product_id:
                    continue
                if product_id in written_product_ids:
                    continue
                new_products.append(product)
            print(f"Found {len(page_products)} products on page {page_number}; new unique: {len(new_products)}")
            if len(new_products) == 0:
                print("No new results to save on this page; ending this silhouette.")
                break
            all_data.extend(new_products)
            if not wrote_header:
                save_to_csv(new_products, "goat_listings.csv", mode='w')
                wrote_header = True
            else:
                append_to_csv(new_products, "goat_listings.csv")
            for product in new_products:
                pid = product.get('id')
                if pid is not None:
                    written_product_ids.add(pid)
            page_limit = int(data.get('pageLimit', 12))
            if len(data['data']['productsList']) < page_limit:
                print("Reached end of results for this silhouette.")
                break
            page_number += 1
            time.sleep(1)
    print(f"\nScraping completed. Total products collected: {len(all_data)}")
    return all_data

if __name__ == "__main__":
    print("Testing single request on first URL...")
    first = fetch_json_page(SEARCH_URLS[0], 1)
    if first and 'data' in first and 'productsList' in first['data']:
        print(f"✓ Single request successful: {len(first['data']['productsList'])} products on page 1")
        all_products = scrape_all_pages()
        save_to_json(all_products)
    else:
        print("✗ Request failed or no products.")