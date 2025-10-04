import os
import asyncio
import time
from typing import Dict, Tuple, List, Optional
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# ---------- Config ----------
CLIENT_SECRET_PATH = "/Users/nirbhay/shopify/client_secret_942057786652-9q64bdptkc29c67fnndrnndj596ibtr3.apps.googleusercontent.com.json"
INPUT_CSV = "Sample_Fixed.csv"
OUTPUT_CSV = "output_with_links.csv"
IMAGE_DIR = "SIZE_CHARTS"
HANDLE_COL = "Product Handle"      # use this column from CSV for matching (changed as you requested)
SIZE_CHART_COL = "size_chart_link"
BATCH_SIZE = 50
CONCURRENCY = 6            # safe default; increase carefully
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0     # seconds
# ----------------------------

# ---------- Auth ----------
gauth = GoogleAuth()
gauth.LoadClientConfigFile(CLIENT_SECRET_PATH)
try:
    gauth.LocalWebserverAuth()
except Exception:
    gauth.CommandLineAuth()
drive = GoogleDrive(gauth)

# ---------- Load CSV ----------
if not os.path.exists(INPUT_CSV):
    raise SystemExit(f"[ERROR] Input CSV not found: {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
if HANDLE_COL not in df.columns:
    raise SystemExit(f"[ERROR] Column '{HANDLE_COL}' not found in CSV")

# ---------- prepare image list ----------
png_files: List[str] = []
if os.path.isdir(IMAGE_DIR):
    for name in os.listdir(IMAGE_DIR):
        if name.lower().endswith(".png"):
            png_files.append(os.path.join(IMAGE_DIR, name))
else:
    print(f"[WARN] Image directory not found: {IMAGE_DIR}")
png_files_sorted = sorted(png_files, key=lambda p: os.path.basename(p).lower())

# ---------- resume: load existing output csv if present ----------
handle_to_link: Dict[str, Optional[str]] = {}
if os.path.exists(OUTPUT_CSV):
    try:
        prev = pd.read_csv(OUTPUT_CSV, dtype=str).fillna("")
        if HANDLE_COL in prev.columns and SIZE_CHART_COL in prev.columns:
            grouped = prev.groupby(HANDLE_COL)[SIZE_CHART_COL].first()
            for h, link in grouped.items():
                if isinstance(link, str) and link.strip() != "":
                    handle_to_link[h] = link
            print(f"[INFO] Resumed {len(handle_to_link)} handles from existing {OUTPUT_CSV}")
    except Exception as e:
        print(f"[WARN] Could not read existing {OUTPUT_CSV} to resume: {e}")

# ---------- helper: normalize ----------
def _normalize_handle(h: str) -> str:
    return h.strip().lower()

# ---------- helper: select png for a handle ----------
def _handle_from_filename_basename(bn: str) -> str:
    """
    Given a basename without extension, derive the handle by removing the last '_' segment
    if present. Example: 'armour-top-1_L' -> 'armour-top-1'
    """
    if "_" in bn:
        return bn.rsplit("_", 1)[0]
    return bn

def _select_png_for_handle(handle: str) -> Optional[str]:
    """
    Matching rules (robust):
      1) Candidate if file's derived-handle (filename without final _segment) equals handle.
         e.g. armour-top-1_L.png -> derived handle armour-top-1 matches.
      2) Exact basename match: armour-top-1.png
      3) Startswith handle + separator (non-alnum) e.g. armour-top-1-..., armour-top-1_...
      4) Fallback: any filename that startswith handle (lowercase)
    Choose candidate deterministically: prefer derived-handle exact, then exact basename,
    then largest file size among matches (tie-breaker).
    """
    if not handle:
        return None
    h_norm = _normalize_handle(handle)

    candidates = []

    for p in png_files_sorted:
        bn = os.path.basename(p).lower()
        name_only = bn[:-4] if bn.endswith(".png") else bn  # remove .png
        derived = _handle_from_filename_basename(name_only)
        # 1) derived-handle exact
        if derived == h_norm:
            candidates.append(p)
            continue
        # 2) exact basename match (without underscore)
        if name_only == h_norm:
            candidates.append(p)
            continue
        # 3) startswith handle + non-alnum separator
        if name_only.startswith(h_norm):
            if len(name_only) == len(h_norm):
                candidates.append(p)
            elif len(name_only) > len(h_norm):
                next_char = name_only[len(h_norm)]
                if not next_char.isalnum():
                    candidates.append(p)
                else:
                    # fallback candidate but lower priority
                    candidates.append(p)

    if not candidates:
        return None

    # deduplicate
    candidates = sorted(set(candidates))
    # prefer candidate with derived-handle exact (already likely in list first),
    # but as tie-breaker pick largest file (better resolution) then filename sort
    candidates.sort(key=lambda p: (-os.path.getsize(p), os.path.basename(p)))
    return candidates[0]

# ---------- blocking upload with retry ----------
def _upload_file_blocking_with_retries(drive: GoogleDrive, file_path: str) -> Optional[str]:
    if not file_path or not os.path.exists(file_path):
        return None
    attempts = 0
    while attempts < RETRY_ATTEMPTS:
        try:
            gfile = drive.CreateFile({'title': os.path.basename(file_path)})
            gfile.SetContentFile(file_path)
            gfile.Upload()
            # set public permission
            try:
                gfile.InsertPermission({'type': 'anyone', 'value': 'anyone', 'role': 'reader'})
            except Exception:
                # keep going even if setting permission fails
                pass
            return gfile.get('alternateLink')
        except Exception as e:
            attempts += 1
            delay = RETRY_BASE_DELAY * (2 ** (attempts - 1))
            print(f"[WARN] Upload attempt {attempts}/{RETRY_ATTEMPTS} failed for {file_path}: {e}. Retrying in {delay:.1f}s")
            time.sleep(delay)
    print(f"[ERROR] Failed to upload after {RETRY_ATTEMPTS} attempts: {file_path}")
    return None

# ---------- async wrapper ----------
async def _upload_one(sema: asyncio.Semaphore, drive: GoogleDrive, handle: str, idx: int, total: int) -> Tuple[str, Optional[str]]:
    async with sema:
        # skip if already have a valid link
        if handle in handle_to_link and handle_to_link[handle]:
            return handle, handle_to_link[handle]
        file_path = _select_png_for_handle(handle)
        if not file_path:
            print(f"[{idx}/{total}] MISSING for Handle '{handle}': no matching PNG")
            handle_to_link[handle] = None
            return handle, None
        link = await asyncio.to_thread(_upload_file_blocking_with_retries, drive, file_path)
        handle_to_link[handle] = link
        if link:
            print(f"[{idx}/{total}] UPLOADED: {os.path.basename(file_path)} -> {link}")
        else:
            print(f"[{idx}/{total}] FAILED upload: {os.path.basename(file_path)}")
        return handle, link

def _chunks(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]

async def process_all_in_batches(drive: GoogleDrive, image_dir: str, batch_size: int = BATCH_SIZE, concurrency: int = CONCURRENCY):
    # collect handles in deterministic order
    handles = [h for h in df[HANDLE_COL].unique() if isinstance(h, str) and h.strip() != ""]
    handles.sort(key=lambda s: s.lower())
    total = len(handles)
    print(f"Found {total} unique non-empty Product Handles to process.")
    sema = asyncio.Semaphore(concurrency)
    batches = _chunks(handles, batch_size)
    processed = 0
    for b_idx, batch in enumerate(batches, start=1):
        tasks = []
        for i, handle in enumerate(batch, start=1):
            idx = processed + i
            if handle in handle_to_link and handle_to_link[handle]:
                continue
            tasks.append(_upload_one(sema, drive, handle, idx, total))
        if tasks:
            results = await asyncio.gather(*tasks)
            for handle, link in results:
                handle_to_link[handle] = link
        # Update CSV after each batch
        df[SIZE_CHART_COL] = df[HANDLE_COL].map(handle_to_link)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"[BATCH {b_idx}/{len(batches)}] Wrote progress to {OUTPUT_CSV}")
        processed += len(batch)

# ---------- run ----------
asyncio.run(process_all_in_batches(drive, IMAGE_DIR))
print("✅ Done! Output saved/updated in batches at", OUTPUT_CSV)
