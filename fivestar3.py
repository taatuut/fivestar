import pandas as pd
import geopandas as gpd
import folium
import requests
import os

# File paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Step 1: Download datasets using requests
def download_file(url, local_path):
    if not os.path.exists(local_path):
        print(f"Downloading {url} to {local_path}...")
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
    else:
        print(f"{local_path} already exists. Skipping download.")

# S3 public URLs for the datasets
places_url = "https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=2024-11-19/places/parquet/places.parquet"
categories_url = "https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=2024-11-19/categories/parquet/categories.parquet"

download_file(places_url, places_file)
download_file(categories_url, categories_file)

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
