import pandas as pd
import osmnx as ox
from geopy.distance import geodesic
from pathlib import Path


# Paths
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
EXTERNAL_DIR = DATA_DIR / "external"
LISTINGS_FILE = RAW_DIR / "listings.csv"
OUTPUT_FILE = EXTERNAL_DIR / "transit_distances.csv"

def fetch_transit_stops(lat, lon, distance=1000):
    """ fetch transit stops (bus stations, train stations, etc) within 1 km radius  """
    tags = {'amenity':'bus_station', 'railway': 'station', 'public_transport': 'stop_position'}
    try:
        stops = ox.features_from_point((lat, lon), tags=tags, dist=distance)
        if not stops.empty:
            return stops
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching transit stops for listing: {e}")
        return pd.DataFrame()

def calculate_min_distance(listing, stops):
    """calculate the minimum distance from listing to the nearest transit stop"""
    listing_lat, listing_lon = listing['latitude'], listing['longitude']
    if stops.empty:
        return float('inf')  # No stops found, use infinity for now
    distances =[]
    for _, stop in stops.iterrows():
        # Use centroid to handle both Point and Polygon geometries
        centroid = stop['geometry'].centroid
        stop_lat, stop_lon = centroid.y, centroid.x
        dist = geodesic((listing_lat, listing_lon), (stop_lat, stop_lon)).meters
        distances.append(dist)
    return min(distances) if distances else float('inf')

def main():
    # Load listings
    df_listings = pd.read_csv(LISTINGS_FILE, nrows=50) # Limit for testing, scale up later
    transit_distances = []
    for _, row in df_listings.iterrows():
        lat, lon = row['latitude'], row['longitude']
        listing_id = row['id']
        stops = fetch_transit_stops(lat, lon)
        min_distance = calculate_min_distance(row, stops)
        transit_distances.append({'listing_id': listing_id, 'min_distance_to_transit': min_distance})
    
    # Save
    transit_df = pd.DataFrame(transit_distances)
    EXTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    transit_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(transit_df)} transit distances to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

