import os
import asyncio
import time
import requests
from typing import Dict, Tuple, List, Optional
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pathlib import Path
from PIL import Image

# ---------- Config ----------
from dotenv import load_dotenv

load_dotenv()

# ---------- Config ----------
CLIENT_SECRET_PATH = os.getenv("CLIENT_SECRET_PATH")
INPUT_CSV = "goat_listings.csv"
OUTPUT_CSV = "goat_listings_with_drive_urls.csv"
TEMP_IMAGE_DIR = "temp_goat_images"
PICTURE_URL_COL = "Picture URL"
PRODUCT_ID_COL = "Product ID"  # Use Product ID as unique identifier
DRIVE_URL_COL = "drive_url"
IMAGE_WIDTH_COL = "Image Width"
IMAGE_HEIGHT_COL = "Image Height"
BATCH_SIZE = 50
CONCURRENCY = 6            # safe default; increase carefully
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0     # seconds
DOWNLOAD_TIMEOUT = 30      # seconds for image download
TARGET_SIZE = 700          # Target image size (700x700)
BORDER_WIDTH = 102.5       # White border width
# ----------------------------

# Create temp directory for downloaded images
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
print("✅ Google Drive authenticated")

# ---------- Load CSV ----------
if not os.path.exists(INPUT_CSV):
    raise SystemExit(f"[ERROR] Input CSV not found: {INPUT_CSV}")

print(f"📁 Loading CSV: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV, dtype=str, encoding='utf-8-sig').fillna("")
if PICTURE_URL_COL not in df.columns:
    raise SystemExit(f"[ERROR] Column '{PICTURE_URL_COL}' not found in CSV")
if PRODUCT_ID_COL not in df.columns:
    raise SystemExit(f"[ERROR] Column '{PRODUCT_ID_COL}' not found in CSV")

print(f"✅ Loaded {len(df)} rows from CSV")

# ---------- Resume: load existing output csv if present ----------
product_id_to_drive_url: Dict[str, Optional[str]] = {}
product_id_to_dimensions: Dict[str, Tuple[int, int]] = {}

if os.path.exists(OUTPUT_CSV):
    try:
        prev = pd.read_csv(OUTPUT_CSV, dtype=str, encoding='utf-8-sig').fillna("")
        if PRODUCT_ID_COL in prev.columns and DRIVE_URL_COL in prev.columns:
            for _, row in prev.iterrows():
                pid = row.get(PRODUCT_ID_COL, "").strip()
                drive_url = row.get(DRIVE_URL_COL, "").strip()
                img_width = row.get(IMAGE_WIDTH_COL, "").strip()
                img_height = row.get(IMAGE_HEIGHT_COL, "").strip()

                if pid and drive_url:
                    product_id_to_drive_url[pid] = drive_url

                if pid and img_width and img_height:
                    try:
                        product_id_to_dimensions[pid] = (int(float(img_width)), int(float(img_height)))
                    except:
                        pass

            print(f"[INFO] Resumed {len(product_id_to_drive_url)} products from existing {OUTPUT_CSV}")
    except Exception as e:
        print(f"[WARN] Could not read existing {OUTPUT_CSV} to resume: {e}")


# ---------- Helper: Resize image with white border ----------
def resize_image_with_border(input_path: str, output_path: str, target_size: int = 700, border_width: float = 102.5) -> Tuple[bool, int, int]:
    """
    Resize image to target_size with white border
    Returns: (success, width, height)
    """
    try:
        # Open the image
        with Image.open(input_path) as img:
            # Convert to RGBA to preserve transparency
            if img.mode != 'RGBA':
                img = img.convert('RGBA')

            # Calculate the size for the image content (excluding border)
            content_size = int(target_size - (2 * border_width))

            # Resize the image to fit the content area while maintaining aspect ratio
            img.thumbnail((content_size, content_size), Image.Resampling.LANCZOS)

            # Create a new image with the target size and white background
            new_img = Image.new('RGBA', (target_size, target_size), (255, 255, 255, 255))  # White background

            # Calculate position to center the image
            x = (target_size - img.width) // 2
            y = (target_size - img.height) // 2

            # Paste the resized image onto the white background, preserving transparency
            new_img.paste(img, (x, y), img if img.mode == 'RGBA' else None)

            # Convert back to RGB for final output
            final_img = Image.new('RGB', new_img.size, (255, 255, 255))  # White background
            final_img.paste(new_img, mask=new_img.split()[-1])  # Use alpha channel as mask

            # Save the final image
            final_img.save(output_path, 'PNG', quality=95)

            width, height = final_img.size
            print(f"✅ Resized: {output_path} ({width}x{height})")
            return True, width, height

    except Exception as e:
        print(f"❌ Error resizing {input_path}: {e}")
        return False, 0, 0


# ---------- Helper: Download image ----------
def download_image(url: str, product_id: str) -> Optional[str]:
    """
    Download image from URL and save to temp directory.
    Returns local file path on success, None on failure.
    """
    if not url or url == "N/A":
        return None

    try:
        # Extract file extension from URL
        ext = ".png"  # default
        if "." in url.split("/")[-1]:
            ext_candidate = "." + url.split(".")[-1].split("?")[0]
            if ext_candidate.lower() in [".png", ".jpg", ".jpeg", ".webp", ".gif"]:
                ext = ext_candidate

        # Create filename from product_id
        filename = f"{product_id}_original{ext}"
        filepath = os.path.join(TEMP_IMAGE_DIR, filename)

        # Skip if already downloaded
        if os.path.exists(filepath):
            return filepath

        # Download image
        response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        response.raise_for_status()

        # Save to file
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return filepath

    except Exception as e:
        print(f"[WARN] Failed to download image for product {product_id} from {url}: {e}")
        return None


# ---------- Blocking upload with retry ----------
def _upload_file_blocking_with_retries(drive: GoogleDrive, file_path: str, product_id: str) -> Optional[str]:
    """
    Upload file to Google Drive with retries.
    Returns Google Drive URL on success, None on failure.
    """
    if not file_path or not os.path.exists(file_path):
        return None

    attempts = 0
    while attempts < RETRY_ATTEMPTS:
        try:
            # Create file with descriptive title
            title = f"product_{product_id}_{os.path.basename(file_path)}"
            gfile = drive.CreateFile({'title': title})
            gfile.SetContentFile(file_path)
            gfile.Upload()

            # Set public permission
            try:
                gfile.InsertPermission({'type': 'anyone', 'value': 'anyone', 'role': 'reader'})
            except Exception:
                # keep going even if setting permission fails
                pass

            # Get direct link to file
            # Use 'webContentLink' for direct download or 'alternateLink' for Drive view
            drive_url = gfile.get('webContentLink') or gfile.get('alternateLink')

            # Clean up local file after successful upload
            try:
                os.remove(file_path)
            except:
                pass

            return drive_url

        except Exception as e:
            attempts += 1
            delay = RETRY_BASE_DELAY * (2 ** (attempts - 1))
            print(f"[WARN] Upload attempt {attempts}/{RETRY_ATTEMPTS} failed for {file_path}: {e}. Retrying in {delay:.1f}s")
            time.sleep(delay)

    print(f"[ERROR] Failed to upload after {RETRY_ATTEMPTS} attempts: {file_path}")
    return None


# ---------- Async wrapper ----------
async def _process_one_product(
    sema: asyncio.Semaphore,
    drive: GoogleDrive,
    product_id: str,
    picture_url: str,
    idx: int,
    total: int
) -> Tuple[str, Optional[str], Optional[Tuple[int, int]]]:
    """
    Download image, resize with border, upload to Drive for one product.
    Returns (product_id, drive_url, (width, height))
    """
    async with sema:
        # Skip if already have a valid drive URL
        if product_id in product_id_to_drive_url and product_id_to_drive_url[product_id]:
            dimensions = product_id_to_dimensions.get(product_id)
            print(f"[{idx}/{total}] SKIPPED: Product {product_id} (already uploaded)")
            return product_id, product_id_to_drive_url[product_id], dimensions

        # Download image
        print(f"[{idx}/{total}] Downloading image for product {product_id}...")
        original_path = await asyncio.to_thread(download_image, picture_url, product_id)

        if not original_path:
            print(f"[{idx}/{total}] FAILED download for product {product_id}")
            product_id_to_drive_url[product_id] = None
            return product_id, None, None

        # Resize image with border
        print(f"[{idx}/{total}] Resizing image with white border...")
        resized_path = original_path.replace("_original", "_resized")
        success, width, height = await asyncio.to_thread(resize_image_with_border, original_path, resized_path, TARGET_SIZE, BORDER_WIDTH)

        if not success:
            print(f"[{idx}/{total}] FAILED resize for product {product_id}")
            # Try to clean up
            try:
                os.remove(original_path)
            except:
                pass
            product_id_to_drive_url[product_id] = None
            return product_id, None, None

        # Clean up original file
        try:
            os.remove(original_path)
        except:
            pass

        # Upload to Drive
        print(f"[{idx}/{total}] Uploading to Drive: {os.path.basename(resized_path)}...")
        drive_url = await asyncio.to_thread(_upload_file_blocking_with_retries, drive, resized_path, product_id)

        product_id_to_drive_url[product_id] = drive_url
        dimensions = (width, height) if drive_url else None
        if dimensions:
            product_id_to_dimensions[product_id] = dimensions

        if drive_url:
            print(f"[{idx}/{total}] ✅ SUCCESS: Product {product_id} -> {drive_url} ({width}x{height})")
        else:
            print(f"[{idx}/{total}] ❌ FAILED upload for product {product_id}")

        return product_id, drive_url, dimensions


def _chunks(lst: List, n: int) -> List[List]:
    """Split list into chunks of size n"""
    return [lst[i:i + n] for i in range(0, len(lst), n)]


async def process_all_in_batches(drive: GoogleDrive, batch_size: int = BATCH_SIZE, concurrency: int = CONCURRENCY):
    """
    Process all products in batches:
    1. Download images
    2. Resize with white border
    3. Upload to Google Drive
    4. Map Drive URLs and dimensions back to CSV
    """
    # Get unique products with picture URLs
    products_to_process = []
    seen_product_ids = set()

    for _, row in df.iterrows():
        product_id = str(row.get(PRODUCT_ID_COL, "")).strip()
        picture_url = str(row.get(PICTURE_URL_COL, "")).strip()

        if product_id and picture_url and picture_url != "N/A" and product_id not in seen_product_ids:
            products_to_process.append((product_id, picture_url))
            seen_product_ids.add(product_id)

    total = len(products_to_process)
    print(f"\n📊 Found {total} unique products with picture URLs to process")

    sema = asyncio.Semaphore(concurrency)
    batches = _chunks(products_to_process, batch_size)
    processed = 0

    for b_idx, batch in enumerate(batches, start=1):
        print(f"\n{'='*60}")
        print(f"🔄 Processing batch {b_idx}/{len(batches)} ({len(batch)} products)")
        print(f"{'='*60}")

        tasks = []
        for i, (product_id, picture_url) in enumerate(batch, start=1):
            idx = processed + i

            # Skip if already processed
            if product_id in product_id_to_drive_url and product_id_to_drive_url[product_id]:
                continue

            tasks.append(_process_one_product(sema, drive, product_id, picture_url, idx, total))

        if tasks:
            results = await asyncio.gather(*tasks)
            for product_id, drive_url, dimensions in results:
                product_id_to_drive_url[product_id] = drive_url
                if dimensions:
                    product_id_to_dimensions[product_id] = dimensions

        # Update CSV after each batch
        df[DRIVE_URL_COL] = df[PRODUCT_ID_COL].map(product_id_to_drive_url)
        df[IMAGE_WIDTH_COL] = df[PRODUCT_ID_COL].map(lambda pid: product_id_to_dimensions.get(pid, (None, None))[0])
        df[IMAGE_HEIGHT_COL] = df[PRODUCT_ID_COL].map(lambda pid: product_id_to_dimensions.get(pid, (None, None))[1])
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"\n💾 [BATCH {b_idx}/{len(batches)}] Saved progress to {OUTPUT_CSV}")

        processed += len(batch)

    print(f"\n{'='*60}")
    print(f"✅ All batches completed!")
    print(f"{'='*60}")


# ---------- Run ----------
print(f"\n🚀 Starting image download, resize, and upload process...")
print(f"   Target size: {TARGET_SIZE}x{TARGET_SIZE} with {BORDER_WIDTH}px white border")
print(f"   Batch size: {BATCH_SIZE}")
print(f"   Concurrency: {CONCURRENCY}")
print(f"   Retry attempts: {RETRY_ATTEMPTS}\n")

asyncio.run(process_all_in_batches(drive, BATCH_SIZE, CONCURRENCY))

print(f"\n{'='*60}")
print(f"🎉 Done! Output saved to: {OUTPUT_CSV}")
print(f"📊 Total products processed: {len(product_id_to_drive_url)}")
print(f"✅ Successful uploads: {sum(1 for url in product_id_to_drive_url.values() if url)}")
print(f"❌ Failed uploads: {sum(1 for url in product_id_to_drive_url.values() if not url)}")
print(f"{'='*60}")

# Clean up temp directory
try:
    remaining_files = os.listdir(TEMP_IMAGE_DIR)
    if not remaining_files:
        os.rmdir(TEMP_IMAGE_DIR)
        print(f"🧹 Cleaned up temp directory: {TEMP_IMAGE_DIR}")
    else:
        print(f"⚠️  {len(remaining_files)} files remain in {TEMP_IMAGE_DIR}")
except:
    pass
