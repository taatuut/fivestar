import pandas as pd
import geopandas as gpd
import folium
from pyarrow.parquet import ParquetDataset
import pyarrow as pa

# Define S3 paths
places_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/"
categories_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/"

# Load Parquet data
def load_parquet_from_s3(s3_path):
    dataset = ParquetDataset(s3_path)
    table = dataset.read()
    return table.to_pandas()

# Load datasets
print("Loading data...")
places = load_parquet_from_s3(places_path)
categories = load_parquet_from_s3(categories_path)

# Merge datasets on category_id
print("Processing data...")
places = places.merge(categories, left_on="category_id", right_on="id", suffixes=('_place', '_category'))

# Filter for beer-related categories
beer_related_categories = categories[categories['name'].str.contains('beer', case=False, na=False)]
beer_related_places = places[places['category_id'].isin(beer_related_categories['id'])]

# Group by country and category
beer_density = beer_related_places.groupby(['country', 'name_category']).size().reset_index(name='poi_count')

# Find the highest density category per country
highest_density = beer_density.loc[beer_density.groupby('country')['poi_count'].idxmax()]

# Map visualization using Folium
print("Creating map...")
map_center = [20, 0]  # Global centering
beer_map = folium.Map(location=map_center, zoom_start=2)

# Add markers to the map
for _, row in highest_density.iterrows():
    country = row['country']
    category = row['name_category']
    poi_count = row['poi_count']
    
    # Filter to get the latitude and longitude of POIs in this country/category
    country_category_places = beer_related_places[
        (beer_related_places['country'] == country) & 
        (beer_related_places['name_category'] == category)
    ]
    
    # Use the mean of coordinates as representative for the country's POIs
    lat = country_category_places['latitude'].mean()
    lon = country_category_places['longitude'].mean()
    
    # Add a marker
    folium.Marker(
        location=[lat, lon],
        popup=f"Country: {country}<br>Category: {category}<br>POI Count: {poi_count}",
        tooltip=f"{country} - {category}"
    ).add_to(beer_map)

# Save and display the map
map_file = "beer_density_map.html"
beer_map.save(map_file)
print(f"Map saved to {map_file}. Open it in a browser to view.")
