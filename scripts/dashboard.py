import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path


# Paths
DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
LISTINGS_FILE = PROCESSED_DIR / "listings_with_transit.csv"

# Load data
@st.cache_data  # Speeds up reloads
def load_data():
    df = pd.read_csv(LISTINGS_FILE)
    # Replace inf with a large number for viz
    df['min_distance_to_transit'] = df['min_distance_to_transit'].replace(float('inf'), 9999)
    df.rename(columns={'min_distance_to_transit': 'min_transit_distance'}, inplace=True)
    df['price'] = df['price'].str.replace('$', '').str.replace(',', '')
    df['price'] = df['price'].astype(float)
    return df

# Main app
st.title('Airbnb Neighborhoods Insights: San Francisco')
st.write('Explore listings by price and transit access!')

# Load data
df = load_data()
# Sidebar filters
st.sidebar.header('Filters')
price_range = st.sidebar.slider('Price Range ($)',
                                int(df['price'].min()), 
                                int(df['price'].max()),
                                (0, 500))
transit_max = st.sidebar.slider('Max Transit Distance (meters)',
                                0, 5000, 1000)

# Filter data
filtered_df = df[(df['price'].between(price_range[0], price_range[1])) &
                 (df['min_transit_distance'] <= transit_max)]

# Show stats
st.write(f"Showing {len(filtered_df)} listings")
st.write(filtered_df[['name','price', 'min_transit_distance']].head())

# Map plot
fig = px.scatter_mapbox(filtered_df,
                        lat='latitude',
                        lon='longitude',
                        color='price',
                        size='min_transit_distance',
                        color_continuous_scale=px.colors.sequential.Viridis,
                        size_max=15,
                        zoom=11,
                        mapbox_style='open-street-map')
st.plotly_chart(fig)

# Price histogram
st.subheader('Price Distribution')
hist_fig = px.histogram(filtered_df, x='price', nbins=30)
st.plotly_chart(hist_fig)

