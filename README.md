# Airbnb Neighborhood Insights
A data pipeline to uncover neighborhood vibes for Airbnb travelers using Python, Airflow, and AWS.

## Tech Stack
Python (pandas, osmnx), AWS (S3, Redshift), Streamlit.

## Setup
- Downloaded `listings.csv` and `reviews.csv` from Inside Airbnb (San Francisco) 
- Used 'osmnx' to fetch geospatial data (parks) and saved sample plot for aprks near sample Airbnb listing
- Script to batch-fecth parks for listings
- Script to fetch transit distances and merge with listings data
- Build interatcive Streamlit dashboard
- Integrated AWS S3 for data storage
- Set up Redshift Serverless, loaded S3 data into `listings` table, and debugged data mismatches


## Goals
- Build a pipeline to merge Airbnb data with geospatial insights.
- Create an interactive dashboard for travelers and hosts.
- Learn Airflow, and geospatial data skills along the way.

