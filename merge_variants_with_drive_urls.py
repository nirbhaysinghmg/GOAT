import pandas as pd
import os

# ---------- Config ----------
VARIANTS_CSV = "goat_variants_complete.csv"
DRIVE_URLS_CSV = "goat_listings_with_drive_urls.csv"
OUTPUT_CSV = "goat_variants_with_drive_urls.csv"
PRODUCT_ID_COL = "Product ID"
# ----------------------------

print("🔄 Merging variants CSV with Drive URLs...")
print("=" * 70)

# Check if files exist
if not os.path.exists(VARIANTS_CSV):
    raise SystemExit(f"❌ Error: {VARIANTS_CSV} not found!")

if not os.path.exists(DRIVE_URLS_CSV):
    raise SystemExit(f"❌ Error: {DRIVE_URLS_CSV} not found!")

# Load CSVs
print(f"📁 Loading {VARIANTS_CSV}...")
df_variants = pd.read_csv(VARIANTS_CSV, dtype=str, encoding='utf-8-sig').fillna("")
print(f"   ✅ Loaded {len(df_variants)} rows")

print(f"📁 Loading {DRIVE_URLS_CSV}...")
df_drive_urls = pd.read_csv(DRIVE_URLS_CSV, dtype=str, encoding='utf-8-sig').fillna("")
print(f"   ✅ Loaded {len(df_drive_urls)} rows")

# Check if Product ID column exists in both
if PRODUCT_ID_COL not in df_variants.columns:
    raise SystemExit(f"❌ Error: '{PRODUCT_ID_COL}' column not found in {VARIANTS_CSV}")

if PRODUCT_ID_COL not in df_drive_urls.columns:
    raise SystemExit(f"❌ Error: '{PRODUCT_ID_COL}' column not found in {DRIVE_URLS_CSV}")

# Get the columns to merge from drive_urls CSV
columns_to_merge = ['drive_url', 'Image Width', 'Image Height']
missing_columns = [col for col in columns_to_merge if col not in df_drive_urls.columns]

if missing_columns:
    print(f"⚠️  Warning: The following columns are missing in {DRIVE_URLS_CSV}: {missing_columns}")
    print("   They will be created with empty values.")
    for col in missing_columns:
        df_drive_urls[col] = ""

# Create a lookup dictionary for Product ID -> (drive_url, width, height)
print(f"\n🔗 Creating Product ID lookup dictionary...")
product_id_lookup = {}
for _, row in df_drive_urls.iterrows():
    product_id = str(row.get(PRODUCT_ID_COL, "")).strip()
    if product_id:
        product_id_lookup[product_id] = {
            'drive_url': row.get('drive_url', ''),
            'Image Width': row.get('Image Width', ''),
            'Image Height': row.get('Image Height', '')
        }

print(f"   ✅ Created lookup for {len(product_id_lookup)} unique Product IDs")

# Perform left join by mapping Product ID
print(f"\n🔀 Performing left join on '{PRODUCT_ID_COL}'...")

# Add the new columns to variants dataframe
df_variants['drive_url'] = df_variants[PRODUCT_ID_COL].map(lambda pid: product_id_lookup.get(str(pid).strip(), {}).get('drive_url', ''))
df_variants['Image Width'] = df_variants[PRODUCT_ID_COL].map(lambda pid: product_id_lookup.get(str(pid).strip(), {}).get('Image Width', ''))
df_variants['Image Height'] = df_variants[PRODUCT_ID_COL].map(lambda pid: product_id_lookup.get(str(pid).strip(), {}).get('Image Height', ''))

print(f"   ✅ Merged successfully!")

# Count statistics
total_rows = len(df_variants)
rows_with_drive_url = len(df_variants[df_variants['drive_url'] != ''])
rows_without_drive_url = total_rows - rows_with_drive_url

print(f"\n📊 Merge Statistics:")
print(f"   Total variant rows: {total_rows}")
print(f"   Rows with Drive URL: {rows_with_drive_url} ({rows_with_drive_url/total_rows*100:.1f}%)")
print(f"   Rows without Drive URL: {rows_without_drive_url} ({rows_without_drive_url/total_rows*100:.1f}%)")

# Show sample of unique Product IDs
unique_product_ids = df_variants[PRODUCT_ID_COL].nunique()
print(f"   Unique Product IDs in variants: {unique_product_ids}")

# Save to output CSV
print(f"\n💾 Saving to {OUTPUT_CSV}...")
df_variants.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
print(f"   ✅ Saved {len(df_variants)} rows")

print("\n" + "=" * 70)
print(f"🎉 Done! Output saved to: {OUTPUT_CSV}")
print("=" * 70)

# Show sample of the first few rows with the new columns
print("\n📋 Sample of merged data (first 3 rows):")
print("=" * 70)
sample_columns = [PRODUCT_ID_COL, 'Product Name', 'drive_url', 'Image Width', 'Image Height']
existing_sample_columns = [col for col in sample_columns if col in df_variants.columns]
if len(df_variants) > 0:
    print(df_variants[existing_sample_columns].head(3).to_string(index=False))
else:
    print("No data to display")
print("=" * 70)
