import pandas as pd
import osmnx as ox
from geopy.distance import geodesic
from pathlib import Path
import logging
import numpy as np



# Paths
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
LISTINGS_FILE = RAW_DIR / "listings.csv"
OUTPUT_FILE = EXTERNAL_DIR / "transit_distances.csv"
LOG_DIR = Path('logs')  # New log directory constant

# Create logs directory and setup logging
LOG_DIR.mkdir(parents=True, exist_ok=True)  # Create logs/ if it doesn’t exist
logging.basicConfig(filename=LOG_DIR / 'transit_fetch.log', level=logging.INFO, 
                    format='%(asctime)s %(message)s')
ox.settings.log_console = True

def fetch_transit_stops(lat, lon, distance=2000):
    """ fetch transit stops (bus stations, train stations, etc) within 2 km radius  """
    tags = {'highway': 'bus_stop', 'railway': 'tram_stop', 'station': 'subway'}
    try:
        stops = ox.features_from_point((lat, lon), tags=tags, dist=distance)
        if not stops.empty:
            return stops
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching transit stops for listing: {e}")
        return pd.DataFrame()

def calculate_min_distance(listing, stops):
    """Calculate minimum great-circle distance to nearest transit stop using NumPy."""
    listing_lat, listing_lon = listing['latitude'], listing['longitude']
    if stops.empty:
        return None
    
    # Get centroids for all geometries (Points stay as-is, Polygons convert)
    centroids = stops.geometry.centroid
    lat2 = np.radians(centroids.y)  # Now works for all
    lon2 = np.radians(centroids.x)
    
    # Convert listing to radians
    lat1 = np.radians(listing_lat)
    lon1 = np.radians(listing_lon)
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371000  # Earth radius in meters
    distances = c * r
    
    return min(distances)  # Meters

def main():
    # Load listings
    df_listings = pd.read_csv(LISTINGS_FILE, nrows=50)  # Test with 50, scale later
    transit_distances = []

    for _, row in df_listings.iterrows():
        lat, lon = row['latitude'], row['longitude']
        listing_id = row['id']
        stops = fetch_transit_stops(lat, lon)
        min_distance = calculate_min_distance(row, stops)
        transit_distances.append({
            'listing_id': listing_id,
            'min_distance_to_transit': min_distance,
            'latitude': lat,  # Keep for Redshift
            'longitude': lon,
            'name': row['name'],
            'price': row['price']
        })
        logging.info(f"Processed listing {listing_id}: Transit distance = {min_distance}m")

    # Save with all needed columns
    transit_df = pd.DataFrame(transit_distances)
    transit_df = transit_df.rename(columns={'listing_id': 'id'})  # Match Redshift schema
    EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    transit_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(transit_df)} transit distances to {OUTPUT_FILE}")
        
    
    # Save
    transit_df = pd.DataFrame(transit_distances)
    EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    transit_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(transit_df)} transit distances to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

