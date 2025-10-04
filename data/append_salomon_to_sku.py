import pandas as pd

def append_salomon_to_sku(input_csv: str, output_csv: str):
    """Append 'Salomon' to SKU line in Body HTML"""
    
    print(f"\n📋 Processing: {input_csv}")
    
    # Read CSV
    df = pd.read_csv(input_csv, dtype=str, encoding='utf-8-sig').fillna("")
    
    print(f"   Total rows: {len(df)}")
    
    updated_count = 0
    
    # Process each row
    for idx, row in df.iterrows():
        body_html = str(row.get('Body (HTML)', '')).strip()
        
        if not body_html or 'SKU :' not in body_html:
            continue
        
        # Check if "Salomon" is already in the SKU line
        if 'SKU :' in body_html and 'Salomon' not in body_html.split('Colorway :')[0]:
            # Find the SKU line and append Salomon
            # Split by 'Colorway :' to isolate SKU section
            parts = body_html.split('Colorway :')
            
            if len(parts) >= 2:
                sku_section = parts[0]
                colorway_section = 'Colorway :' + parts[1]
                
                # Append Salomon before the closing tag/newline in SKU section
                # Replace "SKU : [product_name]" with "SKU : [product_name] Salomon"
                if 'SKU :' in sku_section:
                    # Append Salomon right before the next line break or closing tag
                    sku_section = sku_section.rstrip()
                    sku_section += ' Salomon\n'
                    
                    # Reconstruct body HTML
                    new_body_html = sku_section + colorway_section
                    df.at[idx, 'Body (HTML)'] = new_body_html
                    updated_count += 1
    
    # Save
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    
    print(f"   Updated {updated_count} rows")
    print(f"   ✅ Saved to: {output_csv}")

# Process both CSVs
print("🔧 Appending 'Salomon' to SKU in Body HTML...")
print("=" * 70)

append_salomon_to_sku('shopify_import_women_with_images.csv', 'shopify_import_women_with_images.csv')
append_salomon_to_sku('shopify_import_men_with_images.csv', 'shopify_import_men_with_images.csv')

print("\n" + "=" * 70)
print("🎉 Done!")
print("=" * 70)
