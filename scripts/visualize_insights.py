import streamlit as st
import folium
from folium.plugins import MarkerCluster
import pandas as pd
from pathlib import Path
import plotly.express as px

# Paths
INSIGHTS_FILE = Path('data/processed/neighborhood_insights.csv')

# Load data
df = pd.read_csv(INSIGHTS_FILE)

# Streamlit app
st.title("Airbnb SF Neighborhood Insights")
st.write("Price and Listing Density by 0.01° Bins")

# Base map
m=folium.Map(location=[37.7749, -122.4194], zoom_start=12)

# Add marker clusters
for _,row in df.iterrows():
    folium.CircleMarker(
            location=[row['lat_bin'], row['lon_bin']],
            radius=row['listing_count'] / 10,  # Scale size by count
            popup=f"Avg Price: ${row['avg_price']}<br>Count: {row['listing_count']}<br>Density: {row['density_per_sq_km']}/sq km",
            color='blue' if row['avg_price'] < 300 else 'red',  # Color by price
            fill=True,
            fill_opacity=0.7
        ).add_to(m)
    
# Display map
st.components.v1.html(m._repr_html_(), height=600)

# Show table
st.write("Top 10 Priciest Areas:")
st.dataframe(df)


# # Option 2: Plotly
# # Streamlit app
# st.title("Airbnb SF Neighborhood Insights")
# st.write("Price and Listing Density by 0.01° Bins")

# # Plotly scatter
# fig = px.scatter(
#     df, x='lon_bin', y='lat_bin', size='listing_count', color='avg_price',
#     hover_data=['density_per_sq_km'], title="Price and Density Hotspots",
#     labels={'lon_bin': 'Longitude', 'lat_bin': 'Latitude', 'avg_price': 'Avg Price ($)'},
#     color_continuous_scale='Viridis', size_max=50
# )
# st.plotly_chart(fig)

# # Show table
# st.write("Top 10 Priciest Areas:")
# st.dataframe(df)