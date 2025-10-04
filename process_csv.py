import csv
import re

def process_body_html(body_html, brand='Salomon'):
    """
    Process the Body (HTML) column to:
    1. Move brand from end of SKU line to beginning
    2. Replace "ON Running" with the brand name
    """
    if not body_html:
        return body_html

    # Replace "ON Running" with the brand name
    body_html = body_html.replace("ON Running", brand)

    # Process SKU line - move brand from end to beginning
    # Pattern: SKU : <product_name> <brand>
    sku_pattern = r'(SKU\s*:\s*)(.+?)\s+' + re.escape(brand)
    replacement = r'\1' + brand + r' \2'
    body_html = re.sub(sku_pattern, replacement, body_html)

    return body_html

def process_csv_file(input_file, output_file):
    """Process a CSV file and update the Body (HTML) column"""
    with open(input_file, 'r', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        rows = []
        for row in reader:
            if 'Body (HTML)' in row:
                row['Body (HTML)'] = process_body_html(row['Body (HTML)'])
            rows.append(row)

    with open(output_file, 'w', encoding='utf-8-sig', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Processed {len(rows)} rows from {input_file}")
    print(f"Output saved to {output_file}")

if __name__ == "__main__":
    # Process men's CSV
    process_csv_file(
        'shopify_import_men_with_images.csv',
        'shopify_import_men_with_images_updated.csv'
    )

    # Process women's CSV
    process_csv_file(
        'shopify_import_women_with_images.csv',
        'shopify_import_women_with_images_updated.csv'
    )
