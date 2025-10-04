import os
import asyncio
import time
import requests
import pandas as pd
from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from PIL import Image
import concurrent.futures
import re

# optional high-fidelity curl_cffi
try:
    from curl_cffi import requests as curl_requests
    HAVE_CURL_CFFI = True
except Exception:
    HAVE_CURL_CFFI = False

import cloudscraper

# ---------- Config ----------
CLIENT_SECRET_PATH = "/Users/nirbhay/shopify/client_secret_942057786652-9q64bdptkc29c67fnndrnndj596ibtr3.apps.googleusercontent.com.json"
INPUT_CSV_WOMEN = "shopify_import_women.csv"
INPUT_CSV_MEN = "shopify_import_men.csv"
OUTPUT_CSV_WOMEN = "shopify_import_women_with_images.csv"
OUTPUT_CSV_MEN = "shopify_import_men_with_images.csv"
TEMP_IMAGE_DIR = "temp_additional_images"
BATCH_SIZE = 50
CONCURRENCY = 6
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
DOWNLOAD_TIMEOUT = 30
TARGET_SIZE = 700
BORDER_WIDTH = 102.5
MAX_RETRIES = 5
TIMEOUT = 30
# ----------------------------

os.makedirs(TEMP_IMAGE_DIR, exist_ok=True)

# ---------- Auth ----------
print("🔐 Authenticating with Google Drive...")
gauth = GoogleAuth()
gauth.LoadClientConfigFile(CLIENT_SECRET_PATH)
try:
    gauth.LocalWebserverAuth()
except Exception:
    gauth.CommandLineAuth()
drive = GoogleDrive(gauth)
print("✅ Google Drive authenticated\n")

# ---------- Helper: Resize image with white border ----------
def resize_image_with_border(input_path: str, output_path: str) -> Tuple[bool, int, int]:
    """Resize image to 700x700 with white border"""
    try:
        with Image.open(input_path) as img:
            if img.mode != 'RGBA':
                img = img.convert('RGBA')

            content_size = int(TARGET_SIZE - (2 * BORDER_WIDTH))
            img.thumbnail((content_size, content_size), Image.Resampling.LANCZOS)

            new_img = Image.new('RGBA', (TARGET_SIZE, TARGET_SIZE), (255, 255, 255, 255))
            x = (TARGET_SIZE - img.width) // 2
            y = (TARGET_SIZE - img.height) // 2
            new_img.paste(img, (x, y), img if img.mode == 'RGBA' else None)

            final_img = Image.new('RGB', new_img.size, (255, 255, 255))
            final_img.paste(new_img, mask=new_img.split()[-1])
            final_img.save(output_path, 'PNG', quality=95)

            width, height = final_img.size
            return True, width, height
    except Exception as e:
        print(f"❌ Error resizing {input_path}: {e}")
        return False, 0, 0

# ---------- Helper: Download image ----------
async def download_image_async(url: str, filename: str) -> Optional[str]:
    """Download image from URL asynchronously"""
    if not url:
        return None
    
    loop = asyncio.get_event_loop()
    filepath = os.path.join(TEMP_IMAGE_DIR, filename)
    
    if os.path.exists(filepath):
        return filepath

    def _download():
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return filepath
        except Exception as e:
            print(f"[WARN] Failed to download {url}: {e}")
            return None
    
    return await loop.run_in_executor(None, _download)

# ---------- Upload to Drive ----------
async def upload_to_drive_async(file_path: str, title: str) -> Optional[str]:
    """Upload file to Google Drive asynchronously"""
    if not file_path or not os.path.exists(file_path):
        return None

    loop = asyncio.get_event_loop()

    def _upload():
        for attempt in range(RETRY_ATTEMPTS):
            try:
                gfile = drive.CreateFile({'title': title})
                gfile.SetContentFile(file_path)
                gfile.Upload()

                try:
                    gfile.InsertPermission({'type': 'anyone', 'value': 'anyone', 'role': 'reader'})
                except:
                    pass

                drive_url = gfile.get('webContentLink') or gfile.get('alternateLink')

                try:
                    os.remove(file_path)
                except:
                    pass

                return drive_url
            except Exception as e:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"[WARN] Upload attempt {attempt+1} failed: {e}. Retrying in {delay:.1f}s")
                time.sleep(delay)
        return None
    
    return await loop.run_in_executor(None, _upload)

# ---------- Helper functions for robust fetching ----------
def try_curl_cffi(url: str, headers: dict):
    """Try curl_cffi with multiple browser personas"""
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
            try:
                resp = curl_requests.get(url, **kwargs)
                return resp
            except Exception:
                continue
        return None
    except Exception:
        return None


def try_cloudscraper(url: str, headers: dict):
    """Try cloudscraper"""
    try:
        sess = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "darwin", "desktop": True})
        resp = sess.get(url, headers=headers, allow_redirects=True, timeout=TIMEOUT)
        return resp
    except Exception:
        return None


def decode_body(resp) -> str:
    """Decode response body handling brotli compression"""
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


# ---------- Fetch product images from HTML ----------
async def fetch_product_images_async(product_url: str) -> List[str]:
    """Fetch all product images from GOAT product page using robust multi-transport approach"""

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-language': 'en-GB,en;q=0.7',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'sec-gpc': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'referer': 'https://www.goat.com/',
        'accept-encoding': 'gzip, deflate, br'
    }

    loop = asyncio.get_event_loop()

    def _fetch():
        last_status = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = None

                # Try curl_cffi first
                if HAVE_CURL_CFFI:
                    resp = try_curl_cffi(product_url, headers)

                # Fallback to cloudscraper
                if resp is None:
                    resp = try_cloudscraper(product_url, headers)

                # Final fallback to requests
                if resp is None:
                    sess = requests.Session()
                    resp = sess.get(product_url, headers=headers, timeout=TIMEOUT)

                last_status = getattr(resp, "status_code", None) or getattr(resp, "status", None)

                if last_status == 429:
                    wait = 2 + attempt
                    print(f"   ⏳ Rate limited (429). Backing off {wait}s...")
                    time.sleep(wait)
                    continue

                if last_status != 200:
                    wait = 1 + attempt
                    print(f"   ⚠️ [{attempt+1}/{MAX_RETRIES}] HTTP {last_status}. Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

                # Successfully got 200 response
                body = decode_body(resp)
                soup = BeautifulSoup(body, 'html.parser')

                # Find all images in swiper-slide divs
                image_urls = []
                swiper_slides = soup.find_all('div', class_='swiper-slide')

                for slide in swiper_slides:
                    img_tag = slide.find('img')
                    if img_tag and img_tag.get('src'):
                        src = img_tag['src']
                        # Extract the main image URL (remove width parameter)
                        # Get the base URL without width parameter
                        if 'width=' in src:
                            # Extract base URL and use width=2000 for high quality
                            base_url = src.split('?')[0]
                            main_url = f"{base_url}?action=crop&width=2000"
                            image_urls.append(main_url)
                        else:
                            image_urls.append(src)

                return image_urls

            except Exception as e:
                wait = 1 + attempt
                print(f"   ⚠️ [{attempt+1}/{MAX_RETRIES}] Error: {e}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

        # All attempts failed
        print(f"[ERROR] Failed to fetch images from {product_url} after {MAX_RETRIES} attempts (last_status={last_status})")
        return []
    
    return await loop.run_in_executor(None, _fetch)


# ---------- Process single product ----------
async def process_product(handle: str, idx: int, total: int, df_columns: List[str]) -> List[Dict]:
    """Process a single product and return image rows"""
    print(f"\n[{idx}/{total}] Processing: {handle}")

    # Get product URL from Handle
    product_url = f"https://www.goat.com/sneakers/{handle}"

    # Fetch images from product page
    print(f"   🔍 Fetching images from {product_url}...")
    image_urls = await fetch_product_images_async(product_url)

    if not image_urls:
        print(f"   ⚠️  No images found")
        return []

    print(f"   ✅ Found {len(image_urls)} images")

    # Skip first image (already exists in CSV)
    additional_images = image_urls[1:]

    if not additional_images:
        print(f"   ℹ️  No additional images to process")
        return []

    # Process all images for this product in parallel
    new_rows = []
    
    for img_idx, img_url in enumerate(additional_images, start=2):  # Start at position 2
        print(f"   📥 Image {img_idx}/{len(image_urls)}: Downloading...")

        # Download
        original_filename = f"{handle}_img{img_idx}_original.jpg"
        original_path = await download_image_async(img_url, original_filename)

        if not original_path:
            print(f"   ❌ Failed to download image {img_idx}")
            continue

        # Resize (sync operation)
        print(f"   🖼️  Resizing with white border...")
        resized_filename = f"{handle}_img{img_idx}_resized.png"
        resized_path = os.path.join(TEMP_IMAGE_DIR, resized_filename)
        
        loop = asyncio.get_event_loop()
        success, width, height = await loop.run_in_executor(
            None, 
            resize_image_with_border, 
            original_path, 
            resized_path
        )

        if not success:
            print(f"   ❌ Failed to resize image {img_idx}")
            try:
                os.remove(original_path)
            except:
                pass
            continue

        # Clean up original
        try:
            os.remove(original_path)
        except:
            pass

        # Upload to Drive
        print(f"   ☁️  Uploading to Drive...")
        drive_title = f"{handle}_image_{img_idx}.png"
        drive_url = await upload_to_drive_async(resized_path, drive_title)

        if not drive_url:
            print(f"   ❌ Failed to upload image {img_idx}")
            continue

        print(f"   ✅ Uploaded: {drive_url}")

        # Create new row for this image - only populate Handle and image fields
        # All other fields should be empty for image-only rows
        new_row = {col: "" for col in df_columns}  # Start with all empty
        new_row['Handle'] = handle
        new_row['Row #'] = str(img_idx)
        new_row['Top Row'] = ""
        new_row['Image Src'] = drive_url
        new_row['Image Position'] = str(img_idx)
        new_row['Image Width'] = str(width)
        new_row['Image Height'] = str(height)
        new_row['Image Type'] = "Image"

        new_rows.append(new_row)

    return new_rows


# ---------- Save progress ----------
def save_progress(df_original: pd.DataFrame, new_image_rows: List[Dict], processed_handles: set, output_csv: str):
    """Save progress incrementally after each batch"""

    # Read existing output if it exists
    if os.path.exists(output_csv):
        df_existing = pd.read_csv(output_csv, dtype=str, encoding='utf-8-sig').fillna("")
    else:
        df_existing = df_original.copy()

    # For each processed handle, update with new images
    for handle in processed_handles:
        # Get all existing rows for this product
        handle_rows = df_original[df_original['Handle'] == handle].copy()

        # Get all new image rows for this product
        handle_new_images = [row for row in new_image_rows if row['Handle'] == handle]

        if not handle_new_images:
            continue

        # Total images = 1 (existing in first row) + additional images
        total_images = 1 + len(handle_new_images)
        num_existing_rows = len(handle_rows)

        # If we need more rows than we have, create additional empty rows
        if total_images > num_existing_rows:
            additional_needed = total_images - num_existing_rows
            for i in range(additional_needed):
                empty_row = {col: "" for col in df_original.columns}
                empty_row['Handle'] = handle
                handle_rows = pd.concat([handle_rows, pd.DataFrame([empty_row])], ignore_index=True)

        # Now assign images to rows
        # First row already has first image, start from row 2
        for idx, img_row in enumerate(handle_new_images):
            row_position = idx + 1  # 0-indexed, but second row (index 1)
            if row_position < len(handle_rows):
                # Update this row with image data
                handle_rows.iloc[row_position, handle_rows.columns.get_loc('Image Src')] = img_row['Image Src']
                handle_rows.iloc[row_position, handle_rows.columns.get_loc('Image Position')] = str(idx + 2)  # Position 2, 3, 4...
                handle_rows.iloc[row_position, handle_rows.columns.get_loc('Image Width')] = img_row['Image Width']
                handle_rows.iloc[row_position, handle_rows.columns.get_loc('Image Height')] = img_row['Image Height']
                handle_rows.iloc[row_position, handle_rows.columns.get_loc('Image Type')] = 'Image'

        # Renumber Row # consecutively
        handle_rows['Row #'] = range(1, len(handle_rows) + 1)
        handle_rows['Row #'] = handle_rows['Row #'].astype(str)

        # Set Top Row only for first row
        handle_rows['Top Row'] = ''
        handle_rows.iloc[0, handle_rows.columns.get_loc('Top Row')] = 'TRUE'

        # Remove old rows for this handle from existing data
        df_existing = df_existing[df_existing['Handle'] != handle]

        # Add updated rows
        df_existing = pd.concat([df_existing, handle_rows], ignore_index=True)

    # Sort by Handle and Row #
    df_existing['Row #'] = pd.to_numeric(df_existing['Row #'], errors='coerce')
    df_existing = df_existing.sort_values(['Handle', 'Row #'])
    df_existing['Row #'] = df_existing['Row #'].astype(int).astype(str)

    # Save
    df_existing.to_csv(output_csv, index=False, encoding='utf-8-sig')


# ---------- Process CSV ----------
async def process_csv(input_csv: str, output_csv: str, gender_label: str):
    """Process CSV file to add additional images"""

    if not os.path.exists(input_csv):
        print(f"⚠️  {input_csv} not found, skipping...")
        return

    print(f"\n{'='*70}")
    print(f"📋 Processing {gender_label} CSV: {input_csv}")
    print(f"{'='*70}")

    df = pd.read_csv(input_csv, dtype=str, encoding='utf-8-sig').fillna("")

    # Group by Handle to process unique products
    unique_handles = df[df['Top Row'] == 'TRUE']['Handle'].unique()
    print(f"   Found {len(unique_handles)} unique products")

    # Process products in batches with concurrency limit
    # Save after each batch to preserve progress
    processed_handles = set()

    for batch_start in range(0, len(unique_handles), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(unique_handles))
        batch_handles = unique_handles[batch_start:batch_end]

        print(f"\n{'='*70}")
        print(f"📦 Processing batch {batch_start//BATCH_SIZE + 1} ({batch_start+1}-{batch_end} of {len(unique_handles)})")
        print(f"{'='*70}")

        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def process_with_semaphore(handle, idx):
            async with semaphore:
                return await process_product(handle, idx, len(unique_handles), df.columns.tolist())

        # Process batch in parallel
        tasks = [
            process_with_semaphore(handle, batch_start + i + 1)
            for i, handle in enumerate(batch_handles)
        ]

        batch_results = await asyncio.gather(*tasks)

        # Collect new image rows from this batch
        batch_new_rows = []
        for rows in batch_results:
            batch_new_rows.extend(rows)

        print(f"\n✅ Batch complete. New image rows in this batch: {len(batch_new_rows)}")

        # Update processed handles
        for handle in batch_handles:
            processed_handles.add(handle)

        # Save progress after this batch
        print(f"💾 Saving progress after batch {batch_start//BATCH_SIZE + 1}...")
        save_progress(df, batch_new_rows, set(batch_handles), output_csv)
        print(f"✅ Progress saved to {output_csv}")

    print(f"\n✅ All batches complete! Final output saved to {output_csv}")

# ---------- Main ----------
async def main():
    print("🚀 Starting additional image processing...")
    print("=" * 70)

    # Process women's CSV
    await process_csv(INPUT_CSV_WOMEN, OUTPUT_CSV_WOMEN, "Women")

    # Process men's CSV
    await process_csv(INPUT_CSV_MEN, OUTPUT_CSV_MEN, "Men")

    print("\n" + "=" * 70)
    print("🎉 All done!")
    print("=" * 70)

    # Cleanup
    try:
        remaining_files = os.listdir(TEMP_IMAGE_DIR)
        if not remaining_files:
            os.rmdir(TEMP_IMAGE_DIR)
            print(f"🧹 Cleaned up temp directory")
        else:
            print(f"⚠️  {len(remaining_files)} files remain in {TEMP_IMAGE_DIR}")
    except:
        pass

if __name__ == "__main__":
    asyncio.run(main())
