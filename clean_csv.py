import pandas as pd
import os

# ---------- Config ----------
INPUT_CSV_1 = "failed1.csv"
INPUT_CSV_2 = "failed2.csv"
OUTPUT_CSV_1 = "cleaned_failed1.csv"
OUTPUT_CSV_2 = "cleaned_failed2.csv"
# ----------------------------

print("Post-processing Shopify CSV files to remove duplicate sizes...")
print("=" * 70)

def clean_duplicate_sizes(input_file, output_file):
    """Remove duplicate sizes from a Shopify CSV file"""

    # Check if file exists
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return False

    # Load CSV with multiple encoding attempts
    print(f"\nLoading {input_file}...")
    df = None
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']

    for encoding in encodings:
        try:
            df = pd.read_csv(input_file, dtype=str, encoding=encoding).fillna("")
            print(f"   Loaded {len(df)} rows (encoding: {encoding})")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"   Error with {encoding}: {e}")
            continue

    if df is None:
        print(f"   Error: Could not read file with any supported encoding")
        return False

    # Check required columns
    if 'Handle' not in df.columns or 'Option1 Value' not in df.columns:
        print(f"   Error: Required columns 'Handle' or 'Option1 Value' not found!")
        return False

    # Remove duplicate sizes
    print(f"   Checking for duplicate sizes per product...")
    initial_count = len(df)

    duplicates_removed = []
    df_cleaned = []

    for handle, group in df.groupby('Handle', sort=False):
        for size, size_group in group.groupby('Option1 Value', sort=False):
            if len(size_group) > 1 and size:  # Duplicates found
                # Priority 1: Remove rows where Variant Inventory Qty is 0
                if 'Variant Inventory Qty' in df.columns:
                    non_zero_qty = size_group[size_group['Variant Inventory Qty'] != '0']

                    if len(non_zero_qty) > 0:
                        # Keep first non-zero qty row
                        df_cleaned.append(non_zero_qty.iloc[[0]])
                        duplicates_removed.append(f"   WARNING: '{size}' for {handle}: removed {len(size_group) - 1} duplicate(s) (kept non-zero qty)")
                    else:
                        # All are 0, keep first one
                        df_cleaned.append(size_group.iloc[[0]])
                        duplicates_removed.append(f"   WARNING: '{size}' for {handle}: removed {len(size_group) - 1} duplicate(s) (all zero qty, kept first)")
                else:
                    # No inventory qty column, just keep first
                    df_cleaned.append(size_group.iloc[[0]])
                    duplicates_removed.append(f"   WARNING: '{size}' for {handle}: removed {len(size_group) - 1} duplicate(s) (kept first)")
            else:
                # No duplicates, keep the row(s)
                df_cleaned.append(size_group)

    if df_cleaned:
        df = pd.concat(df_cleaned, ignore_index=True)

    if duplicates_removed:
        print(f"   Removed {initial_count - len(df)} duplicate size row(s):")
        for msg in duplicates_removed[:10]:  # Show first 10
            print(msg)
        if len(duplicates_removed) > 10:
            print(f"   ... and {len(duplicates_removed) - 10} more")
    else:
        print(f"   No duplicate sizes found")

    # ---------- Recalculate Row # after removing duplicates ----------
    if 'Row #' in df.columns:
        df['Row #'] = df.groupby('Handle').cumcount() + 1
        df['Row #'] = df['Row #'].astype(str)

    # ---------- Recalculate Top Row after removing duplicates ----------
    if 'Top Row' in df.columns:
        df['Top Row'] = ""
        if len(df) > 0:
            handle_changed = df['Handle'].ne(df['Handle'].shift()).fillna(True)
            df.loc[handle_changed, 'Top Row'] = "TRUE"

    # ---------- Recalculate Total Inventory Qty ----------
    if 'Total Inventory Qty' in df.columns and 'Variant Inventory Qty' in df.columns:
        handle_variant_counts = df.groupby('Handle')['Variant Inventory Qty'].apply(
            lambda x: sum([int(qty) for qty in x if qty.isdigit()])
        )
        df['Total Inventory Qty'] = df['Handle'].map(handle_variant_counts).astype(str)

    # Save cleaned CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"   Saved {len(df)} cleaned rows to {output_file}")

    return True

# Process both files
print("\nProcessing File 1...")
print("-" * 70)
success_1 = clean_duplicate_sizes(INPUT_CSV_1, OUTPUT_CSV_1)

print("\nProcessing File 2...")
print("-" * 70)
success_2 = clean_duplicate_sizes(INPUT_CSV_2, OUTPUT_CSV_2)

# Summary
print("\n" + "=" * 70)
print("Post-processing complete!")
print("=" * 70)

if success_1:
    print(f"SUCCESS: {INPUT_CSV_1} -> {OUTPUT_CSV_1}")
if success_2:
    print(f"SUCCESS: {INPUT_CSV_2} -> {OUTPUT_CSV_2}")

if not success_1 and not success_2:
    print("ERROR: No files were processed successfully")

print("\n" + "=" * 70)
