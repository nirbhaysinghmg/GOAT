
import os
import sys
import time
import json
import html as html_lib
import pandas as pd
from typing import Optional, Dict, Any, Tuple, List
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

# optional high-fidelity curl_cffi
try:
    from curl_cffi import requests as curl_requests
    HAVE_CURL_CFFI = True
except Exception:
    HAVE_CURL_CFFI = False

import cloudscraper
import concurrent.futures
import requests.exceptions
import csv as _csv

# ---------- Config ----------
INPUT_CSV = "goat_listings.csv"
OUTPUT_CSV = "goat_listings_salomon_facts.csv"
MAX_RETRIES = 5
TIMEOUT = 30
BATCH_SAVE_SIZE = 10
MAX_WORKERS = 10

# Proxy/env configuration (optional)
PROXY_HTTP = os.getenv("PROXY_HTTP") or os.getenv("HTTP_PROXY")
PROXY_HTTPS = os.getenv("PROXY_HTTPS") or os.getenv("HTTPS_PROXY")
PREFERRED_PROXY = os.getenv("GOAT_PROXY")  # single proxy override
PROXY_CONFIG = {}
if PREFERRED_PROXY:
    PROXY_CONFIG = {"http": PREFERRED_PROXY, "https": PREFERRED_PROXY}
elif PROXY_HTTP or PROXY_HTTPS:
    if PROXY_HTTP:
        PROXY_CONFIG["http"] = PROXY_HTTP
    if PROXY_HTTPS:
        PROXY_CONFIG["https"] = PROXY_HTTPS

# Optional cookie header from env
GOAT_COOKIE = os.getenv("GOAT_COOKIE")

# ---------- Helper functions (extraction) ----------

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

def build_headers() -> dict:
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

def try_curl_cffi(target_url: str, hdrs: dict, proxy: Optional[str] = None):
    if not HAVE_CURL_CFFI:
        return None
    try:
        for persona in ("chrome124", "chrome123", "chrome120"):
            kwargs = {
                "headers": hdrs,
                "allow_redirects": True,
                "timeout": TIMEOUT,
                "impersonate": persona,
                "verify": True,
            }
            if proxy:
                kwargs["proxies"] = {"http": proxy, "https": proxy}
            try:
                resp = curl_requests.get(target_url, **kwargs)
                return resp
            except Exception:
                continue
        return None
    except Exception:
        return None

def try_cloudscraper(target_url: str, hdrs: dict, proxies: Optional[Dict[str,str]] = None):
    try:
        sess = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"darwin","desktop":True})
        if proxies:
            sess.proxies.update(proxies)
        if GOAT_COOKIE:
            hdrs = dict(hdrs)
            hdrs["cookie"] = GOAT_COOKIE
        resp = sess.get(target_url, headers=hdrs, allow_redirects=True, timeout=TIMEOUT)
        return resp
    except Exception:
        return None

def fetch_once(target_url: str) -> Dict[str, Any]:
    """
    Blocking fetch function that will be executed in threadpool.
    Returns a dict with extracted fields and http_status / error
    """
    hdrs = build_headers()
    last_status = None
    # proxies for curl (string) or cloudscraper (dict)
    proxy_str = PREFERRED_PROXY or (PROXY_CONFIG.get("http") or PROXY_CONFIG.get("https"))
    proxies_dict = ({"http": proxy_str, "https": proxy_str} if proxy_str else (PROXY_CONFIG or None))

    for attempt in range(MAX_RETRIES):
        try:
            resp = None
            if HAVE_CURL_CFFI:
                resp = try_curl_cffi(target_url, hdrs, proxy_str)
            if resp is None:
                resp = try_cloudscraper(target_url, hdrs, proxies_dict)

            if resp is None:
                raise RuntimeError("No response (request failed)")

            last_status = getattr(resp, "status_code", None) or getattr(resp, "status", None)

            if last_status == 200:
                body = decode_body(resp)
                cleaned_html, plain_text = extract_story_html(body)
                meta = extract_product_meta(body)
                # Print extracted fields for visibility
                print(f"\n🔍 Extracted for {target_url}: "
                      f"brandName={meta.get('brandName')}, color={meta.get('color')}, "
                      f"sku={meta.get('sku')}, designer={meta.get('designer')}, "
                      f"http_status={last_status}, story_html_len={len(cleaned_html) if cleaned_html else 0}, "
                      f"story_text_snippet={ (plain_text[:80] + '...') if plain_text else '' }")
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
                print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] {target_url} HTTP {last_status}. Backing off {wait}s...")
                time.sleep(wait)
                continue
        except Exception as e:
            wait = 1 + attempt
            print(f"⚠️ [{attempt+1}/{MAX_RETRIES}] {target_url} error: {e}. Backing off {wait}s...")
            time.sleep(wait)
            continue

    # all attempts failed
    print(f"❌ Failed to fetch {target_url} after {MAX_RETRIES} attempts. last_status={last_status}")
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

# ---------- Helpers to find URL column ----------
def find_url_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["Product URL"]
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    # fallback: pick column with 'http' in first few values
    for c in df.columns:
        sample = df[c].dropna().astype(str).head(10).tolist()
        if any("http" in s for s in sample):
            return c
    return None

# ---------- Main logic ----------
def main():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Input CSV not found: {INPUT_CSV}")
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, dtype=str)
    url_col = find_url_column(df)
    if not url_col:
        print("❌ Could not find a URL column in input CSV. Please ensure a column contains product URLs.")
        print("Columns:", list(df.columns))
        sys.exit(1)
    print(f"Using URL column: {url_col}")

    # Preserve input headers
    input_headers = list(df.columns)
    new_columns = ["story_html", "story_text", "brandName", "color", "sku", "designer", "http_status", "error"]
    output_headers = input_headers + new_columns

    # Prepare unique urls in order of first occurrence
    unique_urls = []
    seen = set()
    for val in df[url_col].fillna("").astype(str).tolist():
        v = val.strip()
        if not v:
            continue
        if v not in seen:
            seen.add(v)
            unique_urls.append(v)
    total_unique = len(unique_urls)
    print(f"Found {total_unique} unique URLs (from {len(df)} input rows).")

    # Prepare results mapping and written flag for rows
    results_map: Dict[str, Dict[str, Any]] = {}
    written_flags = [False] * len(df)

    # Prepare output file (remove if exists)
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    header_written = False

    def append_resolved_rows_to_output():
        """
        Walk input df in order; append rows whose url is in results_map and not yet written.
        Mark written_flags accordingly. Open file in append mode and write rows (no header).
        """
        nonlocal header_written
        rows_to_write = []
        for idx, row in df.iterrows():
            if written_flags[idx]:
                continue
            url_val = str(row.get(url_col, "")).strip()
            if not url_val:
                # write row with empty extracted fields immediately
                mapped = {
                    "story_html":"", "story_text":"", "brandName":"", "color":"", "sku":"", "designer":"",
                    "http_status":"", "error":"No URL"
                }
                rowdict = row.fillna("").to_dict()
                rowdict.update(mapped)
                rows_to_write.append(rowdict)
                written_flags[idx] = True
                continue
            if url_val in results_map:
                mapped = results_map[url_val]
                # ensure every new_column exists
                for k in new_columns:
                    if k not in mapped or mapped[k] is None:
                        mapped[k] = ""
                rowdict = row.fillna("").to_dict()
                rowdict.update({
                    "story_html": mapped["story_html"],
                    "story_text": mapped["story_text"],
                    "brandName": mapped["brandName"],
                    "color": mapped["color"],
                    "sku": mapped["sku"],
                    "designer": mapped["designer"],
                    "http_status": mapped["http_status"],
                    "error": mapped["error"]
                })
                rows_to_write.append(rowdict)
                written_flags[idx] = True

        if rows_to_write:
            write_mode = "a" if header_written else "w"
            with open(OUTPUT_CSV, write_mode, newline="", encoding="utf-8") as outf:
                writer = _csv.DictWriter(outf, fieldnames=output_headers)
                if not header_written:
                    writer.writeheader()
                    header_written = True
                writer.writerows(rows_to_write)
            print(f"💾 Appended {len(rows_to_write)} rows to {OUTPUT_CSV}")

    # Thread pool executor for parallel fetches
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # process in batches of BATCH_SAVE_SIZE unique urls
        for batch_start in range(0, total_unique, BATCH_SAVE_SIZE):
            batch_end = min(batch_start + BATCH_SAVE_SIZE, total_unique)
            batch_urls = unique_urls[batch_start:batch_end]
            print(f"\n🔁 Processing unique-URL batch {batch_start//BATCH_SAVE_SIZE + 1} "
                  f"({batch_start+1}-{batch_end} of {total_unique}) with up to {MAX_WORKERS} workers")

            # submit futures
            future_to_url = {executor.submit(fetch_once, u): u for u in batch_urls}

            # collect results as they complete
            for fut in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[fut]
                try:
                    res = fut.result()
                except Exception as exc:
                    print(f"❌ Exception fetching {url}: {exc}")
                    res = {
                        "story_html": "", "story_text": "", "brandName": "", "color": "", "sku": "", "designer": "",
                        "http_status": None, "error": f"Exception: {exc}"
                    }
                # store result in mapping (single fetch maps to all duplicate rows)
                results_map[url] = res

            # after this batch completes, append any input rows that are now resolvable
            append_resolved_rows_to_output()

            # small pause between batches
            if batch_end < total_unique:
                print("⏳ Waiting 1.0s before next batch...")
                time.sleep(1.0)

    # After all batches done, ensure any rows still not written (should not happen) are handled
    if not all(written_flags):
        print("\n🔄 Writing any remaining rows (unfetched URLs will be filled with error info).")
        # For any remaining rows, if url present but not in results_map, mark error
        for idx, written in enumerate(written_flags):
            if not written:
                row = df.iloc[idx]
                url_val = str(row.get(url_col, "")).strip()
                if not url_val:
                    mapped = {"story_html":"", "story_text":"", "brandName":"", "color":"", "sku":"", "designer":"", "http_status":"", "error":"No URL"}
                else:
                    mapped = results_map.get(url_val, {"story_html":"", "story_text":"", "brandName":"", "color":"", "sku":"", "designer":"", "http_status":"", "error":"Not fetched"})
                rowdict = row.fillna("").to_dict()
                rowdict.update({
                    "story_html": mapped.get("story_html",""),
                    "story_text": mapped.get("story_text",""),
                    "brandName": mapped.get("brandName",""),
                    "color": mapped.get("color",""),
                    "sku": mapped.get("sku",""),
                    "designer": mapped.get("designer",""),
                    "http_status": mapped.get("http_status",""),
                    "error": mapped.get("error","")
                })
                # append single row
                write_mode = "a" if header_written else "w"
                with open(OUTPUT_CSV, write_mode, newline="", encoding="utf-8") as outf:
                    writer = _csv.DictWriter(outf, fieldnames=output_headers)
                    if not header_written:
                        writer.writeheader()
                        header_written = True
                    writer.writerow(rowdict)
                written_flags[idx] = True

    print("\n✅ All done.")
    print(f"Output saved to: {OUTPUT_CSV}")
    # summary
    errors = [v for v in results_map.values() if v.get("error")]
    print(f"Unique URLs fetched: {len(results_map)}; Input rows: {len(df)}; URLs with errors: {len(errors)}")

if __name__ == "__main__":
    main()
