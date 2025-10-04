import pandas as pd

# Read the CSV
df = pd.read_csv("goat_listings.csv")

# Drop duplicates based on "Product Id" column, keeping the first occurrence
df_dedup = df.drop_duplicates(subset=["Product Id"], keep="first")

# Save the final deduplicated DataFrame to a new CSV file
df_dedup.to_csv("Final_listings.csv", index=False)
