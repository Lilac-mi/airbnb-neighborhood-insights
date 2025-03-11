import psycopg2
from dotenv import load_dotenv
import os
import csv

# Load env vars
load_dotenv()

# Redshift connection
REDSHIFT_HOST = "airbnb-redshift-lyra.761018858897.us-west-2.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "dev"
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
S3_BUCKET = os.getenv("AWS_BUCKET_NAME")
S3_FILE = "listings_with_transit.csv"
IAM_ROLE = "arn:aws:iam::761018858897:role/RedshiftS3ReadRole-lyra"

# Connect to Redshift
conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
cur = conn.cursor()

# Drop table if exists
drop_table = "DROP TABLE IF EXISTS listings;"
cur.execute(drop_table)
print("Table listings dropped!")

# Create table
create_table = """
CREATE TABLE listings (
    id BIGINT,
    name VARCHAR(255),
    price FLOAT,
    latitude FLOAT,  
    longitude FLOAT,
    min_transit_distance FLOAT
);
"""


cur.execute(create_table)
print("Table listings created!")

# Load data from S3
copy_query = f"""
COPY listings
FROM 's3://{S3_BUCKET}/{S3_FILE}'
IAM_ROLE '{IAM_ROLE}'
FORMAT CSV
IGNOREHEADER 1;
""".replace("\n", " ").strip()
print(copy_query)
cur.execute(copy_query)

# Commit and close
conn.commit()
cur.close()
conn.close()
print("Data loaded into Redshift!")

# Reconnect for query
conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    dbname=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
cur = conn.cursor()

# Simple query
query = "SELECT COUNT(*) AS listing_count FROM listings WHERE min_transit_distance < 1000;"
cur.execute(query)
result = cur.fetchone()
print(f"Listing count with min transit distance  < 1000m: {result[0]}")

cur.close()
conn.close()



conn = psycopg2.connect(
    dbname=REDSHIFT_DB,
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
cur = conn.cursor()

query = """
SELECT 
    CASE 
        WHEN min_transit_distance < 500 THEN '< 500m'
        WHEN min_transit_distance < 1000 THEN '500-1000m'
        ELSE '> 1000m'
    END AS transit_range,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) AS listing_count
FROM listings
GROUP BY 1
ORDER BY 1;
"""
cur.execute(query)
results = cur.fetchall()
for row in results:
    print(f"Transit Range: {row[0]}, Avg Price: ${row[1]}, Listings: {row[2]}")

cur.close()
conn.close()

# New connection for   query
conn = psycopg2.connect(
    dbname=REDSHIFT_DB,
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
cur = conn.cursor()

# Run insights query
neighborhood_query = """
SELECT 
    FLOOR(latitude * 100) / 100 AS lat_bin,
    FLOOR(longitude * 100) / 100 AS lon_bin,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) AS listing_count,
    ROUND(COUNT(*) / (0.01 * 0.01 * 111 * 111), 2) AS density_per_sq_km
FROM listings
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY avg_price DESC
LIMIT 10;
"""
cur.execute(neighborhood_query)
results = cur.fetchall()

# Export
with open('data/processed/neighborhood_insights.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['lat_bin', 'lon_bin', 'avg_price', 'listing_count', 'density_per_sq_km'])
    writer.writerows(results)

print("Top 10 pricey neighborhoods exported!")
for row in results:
    print(f"Lat: {row[0]}, Lon: {row[1]}, Avg Price: ${row[2]}, Listings: {row[3]}, Density: {row[4]}/sq km")

conn.commit()
cur.close()
conn.close()