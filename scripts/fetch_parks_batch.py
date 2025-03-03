import pandas as pd
import osmnx as ox
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
LISTINGS_FILE = RAW_DIR / "listings.csv"
OUTPUT_FILE = EXTERNAL_DIR / "parks_near_listings.csv"

def load_listings(file_path, num_rows):
    """Load listings from listings.csv"""
    df =  pd.read_csv(file_path, nrows=num_rows)
    df['id'] = df['id'].astype(int)
    return df[['id', 'latitude', 'longitude']]

def fetch_parks_near_listings(lat,lon, listing_id, distance=1000):
    """ fetch parks near a single listing within 1 km """
    tags = {"leisure": "park"}
    parks = ox.features_from_point((lat, lon), tags=tags, dist=distance)
    try:
        if not parks.empty:
            parks['listing_id'] = listing_id
            return parks[['listing_id', 'name', 'geometry']]
        return pd.DataFrame()   
    except Exception as e:
        logger.error(f"Error fetching parks for listing {listing_id}: {e}")
        return pd.DataFrame()
    
def main():
    # Load listings
    locations = load_listings(LISTINGS_FILE, num_rows=50)
    logger.info(f"Processing {len(locations)} listings")

    # Fetch parks within 1km of each listing
    all_parks = []
    for _, row in locations.iterrows():
        lat, lon = row['latitude'], row['longitude']
        listing_id = row['id']
        
        parks = fetch_parks_near_listings(lat, lon, listing_id)
        if not parks.empty:
            all_parks.append(parks)
            logger.info(f"Found {len(parks)} parks near listing {listing_id}")
    # Save
    if all_parks:
        parks_df = pd.concat(all_parks, ignore_index=True)
        # Write to data/external/parks_near_listings.csv
        EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
        parks_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Saved {len(parks_df)} to {OUTPUT_FILE}")
    else:
        parks_df = pd.DataFrame()
        logger.info("No parks found")

    

if __name__ == "__main__":
    main()
