import pandas as pd
import geopandas as gpd
import folium

# File paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Step 1: Download datasets from S3
import os
import pyarrow.parquet as pq
import s3fs

def download_from_s3(s3_path, local_path):
    fs = s3fs.S3FileSystem()
    if not os.path.exists(local_path):
        print(f"Downloading {s3_path} to {local_path}...")
        fs.get(s3_path, local_path)
    else:
        print(f"{local_path} already exists. Skipping download.")

download_from_s3("s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/", places_file)
download_from_s3("s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/", categories_file)

# Step 2: Load only 10,000 rows from Places dataset
print("Loading Places dataset...")
places = pd.read_parquet(places_file, engine="pyarrow", columns=["id", "category_id", "latitude", "longitude", "country"], nrows=10000)

# Step 3: Filter Categories dataset for 'beer'-related categories
print("Filtering Categories dataset...")
categories = pd.read_parquet(categories_file, engine="pyarrow")
beer_categories = categories[categories['name'].str.contains('beer', case=False, na=False)]
beer_category_ids = beer_categories['id'].tolist()

# Step 4: Filter Places dataset for beer-related categories
print("Filtering Places dataset for beer-related categories...")
beer_places = places[places['category_id'].isin(beer_category_ids)]

# Step 5: Join beer-related Places with Categories
print("Joining Places with Categories...")
beer_places = beer_places.merge(beer_categories, left_on="category_id", right_on="id", suffixes=("_place", "_category"))

# Step 6: Analyze highest density categories per country
print("Analyzing data...")
beer_density = beer_places.groupby(['country', 'name_category']).size().reset_index(name='poi_count')
highest_density = beer_density.loc[beer_density.groupby('country')['poi_count'].idxmax()]

# Step 7: Create a stunning map visualization
print("Creating map...")
map_center = [20, 0]  # Global centering
beer_map = folium.Map(location=map_center, zoom_start=2)

# Add markers to the map
for _, row in highest_density.iterrows():
    country = row['country']
    category = row['name_category']
    poi_count = row['poi_count']
    
    # Filter to get coordinates of POIs in this country/category
    country_category_places = beer_places[
        (beer_places['country'] == country) & 
        (beer_places['name_category'] == category)
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
