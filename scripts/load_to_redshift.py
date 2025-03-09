import psycopg2
from dotenv import load_dotenv
import os

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

