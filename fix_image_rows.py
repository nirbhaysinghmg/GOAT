import pandas as pd

def fix_csv(input_csv: str, output_csv: str):
    """Fix CSV by ensuring enough rows exist for all images"""
    
    print(f"\n📋 Processing: {input_csv}")
    
    # Read CSV
    df = pd.read_csv(input_csv, dtype=str, encoding='utf-8-sig').fillna("")
    
    print(f"   Original rows: {len(df)}")
    
    fixed_rows = []
    
    for handle in df['Handle'].unique():
        if not handle:
            continue
            
        handle_rows = df[df['Handle'] == handle].copy()
        
        # Get all unique images for this handle (remove duplicates)
        all_images = handle_rows[handle_rows['Image Src'].str.strip() != ''][['Image Src', 'Image Width', 'Image Height']].drop_duplicates(subset=['Image Src'], keep='first')
        num_images = len(all_images)
        
        # Get existing product rows (rows with variant data OR the first row)
        product_rows = handle_rows[
            (handle_rows['Option1 Value'].str.strip() != '') | 
            (handle_rows['Top Row'] == 'TRUE')
        ].copy()
        
        # Remove duplicate rows (keep first occurrence)
        product_rows = product_rows.drop_duplicates(subset=['Handle', 'Option1 Value'], keep='first')
        
        num_product_rows = len(product_rows)
        
        # Clear all image fields from product rows first
        for col in ['Image Src', 'Image Position', 'Image Width', 'Image Height', 'Image Type']:
            if col in product_rows.columns:
                product_rows[col] = ''
        
        # If we need more rows than we have, create additional empty rows
        if num_images > num_product_rows:
            additional_needed = num_images - num_product_rows
            
            # Create empty rows with only Handle filled
            for i in range(additional_needed):
                empty_row = {col: "" for col in df.columns}
                empty_row['Handle'] = handle
                product_rows = pd.concat([product_rows, pd.DataFrame([empty_row])], ignore_index=True)
        
        # Now assign images to rows (one image per row)
        for idx, (_, img_data) in enumerate(all_images.iterrows()):
            if idx < len(product_rows):
                product_rows.iloc[idx, product_rows.columns.get_loc('Image Src')] = img_data['Image Src']
                product_rows.iloc[idx, product_rows.columns.get_loc('Image Position')] = str(idx + 1)
                product_rows.iloc[idx, product_rows.columns.get_loc('Image Width')] = img_data['Image Width']
                product_rows.iloc[idx, product_rows.columns.get_loc('Image Height')] = img_data['Image Height']
                if idx > 0:  # Image Type only for non-first rows
                    product_rows.iloc[idx, product_rows.columns.get_loc('Image Type')] = 'Image'
        
        # Renumber Row # consecutively starting from 1
        product_rows['Row #'] = range(1, len(product_rows) + 1)
        product_rows['Row #'] = product_rows['Row #'].astype(str)
        
        # Set Top Row = TRUE only for first row
        product_rows['Top Row'] = ''
        product_rows.iloc[0, product_rows.columns.get_loc('Top Row')] = 'TRUE'
        
        fixed_rows.append(product_rows)
    
    # Combine all handles
    df_fixed = pd.concat(fixed_rows, ignore_index=True)
    
    # Sort by Handle and Row #
    df_fixed['Row #'] = pd.to_numeric(df_fixed['Row #'], errors='coerce')
    df_fixed = df_fixed.sort_values(['Handle', 'Row #'])
    df_fixed['Row #'] = df_fixed['Row #'].astype(int).astype(str)
    
    # Save
    df_fixed.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    print(f"   Fixed rows: {len(df_fixed)}")
    print(f"   Removed/Added: {len(df_fixed) - len(df)} rows")
    print(f"   ✅ Saved to: {output_csv}")

# Fix both CSVs
print("🔧 Fixing CSV files...")
print("=" * 70)

fix_csv('shopify_import_women_with_images.csv', 'shopify_import_women_fixed.csv')
fix_csv('shopify_import_men_with_images.csv', 'shopify_import_men_fixed.csv')

print("\n" + "=" * 70)
print("🎉 Done! Fixed files:")
print("   - shopify_import_women_fixed.csv")
print("   - shopify_import_men_fixed.csv")
print("=" * 70)
