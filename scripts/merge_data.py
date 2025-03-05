import pandas as pd
from pathlib import Path

# Paths
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
PROCESSED_DIR = DATA_DIR / "processed"
LISTINGS_FILE = RAW_DIR / "listings.csv"
TRANSIT_FILE = EXTERNAL_DIR / "transit_distances.csv"
OUTPUT_FILE = PROCESSED_DIR / "listings_with_transit.csv"

def clean_listings(df):
    """Clean listings dataframe"""
    df['id'] = df['id'].astype(int)
    key_cols = ['id', 'latitude', 'longitude', 'price']
    return df.dropna(subset=key_cols)

def main():
    """Clean listings data by dropping nulls in key columns"""
    df_listings = pd.read_csv(LISTINGS_FILE)
    df_listings_clean = clean_listings(df_listings)

    # Load transit data
    df_transit = pd.read_csv(TRANSIT_FILE)
    # Merge listings with transit data
    merged_df = pd.merge(df_listings_clean, df_transit, how='left', left_on='id', right_on='listing_id')
    # Save
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(merged_df)} listings with transit data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
