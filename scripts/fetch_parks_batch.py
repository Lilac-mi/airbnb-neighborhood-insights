import pandas as pd
import osmnx as ox
from pathlib import Path

# Paths
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
LISTINGS_FILE = RAW_DIR / "listings.csv"
OUTPUT_FILE = EXTERNAL_DIR / "parks_near_listings.csv"

# Load listings (top 10 for now)
df =  pd.read_csv(LISTINGS_FILE, nrows=10)
df['id'] = df['id'].astype(int)
locations = df[["id","latitude", "longitude"]]
print(f"Processing {len(locations)} listings")

# Fetch parks within 1km of each listing
distance = 1000
tags = {"leisure": "park"}
all_parks = []

for _, row in locations.iterrows():
    lat, lon = row['latitude'], row['longitude']
    listing_id = row['id']
    try:
        parks = ox.features_from_point((lat, lon), tags=tags, dist=distance)
        if not parks.empty:
            parks['listing_id'] = listing_id
            all_parks.append(parks[['listing_id', 'name', 'geometry']])
            print(f"Found {len(parks)} parks near listing {listing_id}")
    except Exception as e:
        print(f"Error fetching parks for listing {listing_id}: {e}")

# Combine all parks into a single dataframe
if all_parks:
    parks_df = pd.concat(all_parks)
    print(f"Total parks found: {len(parks_df)} ")
else:
    parks_df = pd.DataFrame()
    print("No parks found")

# Write to data/external/parks_near_listings.csv
EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
parks_df.to_csv(OUTPUT_FILE, index=False)
print(f"Saved parks_near_listings.csv to {OUTPUT_FILE}")

        


 