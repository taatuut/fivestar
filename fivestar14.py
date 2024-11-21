import pandas as pd
import folium
from folium.plugins import HeatMap
import s3fs
import numpy as np
import os

# S3 paths for the datasets
places_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/"
categories_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/"

# Local file paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Step 1: Download datasets using s3fs with anonymous access
def download_parquet_from_s3(s3_dir_path, local_path):
    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        print(f"Downloading files from {s3_dir_path} to {local_path}...")
        fs = s3fs.S3FileSystem(anon=True)  # Anonymous access
        try:
            # List files in the directory
            files = fs.ls(s3_dir_path)
            # Use the first file in the directory
            parquet_file = next((f for f in files if f.endswith('.parquet')), None)
            if not parquet_file:
                raise FileNotFoundError(f"No Parquet file found in {s3_dir_path}")
            print(f"Downloading {parquet_file}...")
            with fs.open(parquet_file, 'rb') as s3_file:
                with open(local_path, 'wb') as local_file:
                    local_file.write(s3_file.read())
            # Verify if the file is downloaded correctly
            if os.path.getsize(local_path) > 0:
                print(f"Download successful: {local_path} ({os.path.getsize(local_path)} bytes)")
            else:
                raise Exception(f"Downloaded file {local_path} is empty.")
        except Exception as e:
            if os.path.exists(local_path):
                os.remove(local_path)  # Clean up incomplete file
            print(f"Error downloading from {s3_dir_path}: {e}")
            raise
    else:
        print(f"{local_path} already exists and is valid. Skipping download.")

# Download the datasets
download_parquet_from_s3(places_s3_path, places_file)
download_parquet_from_s3(categories_s3_path, categories_file)

# Step 2: Load the full Places dataset
print("Loading Places dataset...")
places = pd.read_parquet(places_file, engine="pyarrow", columns=["fsq_category_ids", "latitude", "longitude", "country"])

# Step 3: Filter Categories dataset for 'beer'-related categories
print("Filtering Categories dataset...")
categories = pd.read_parquet(categories_file, engine="pyarrow", columns=["category_id", "category_name"])
beer_categories = categories[categories['category_name'].str.contains('beer', case=False, na=False)]
beer_category_ids = set(beer_categories['category_id'])

# Step 4: Parse `fsq_category_ids`
def parse_fsq_category_ids(fsq_category_ids):
    """
    Parse the `fsq_category_ids` column into a list of strings.
    Handles empty values, space-separated strings, and valid formats.
    """
    try:
        if str(fsq_category_ids).strip() == "":
            return np.array([])  # Empty array for empty or NaN values
        # Strip the square brackets and split on spaces
        cleaned = str(fsq_category_ids).strip("[]").replace("'", "").split()
        return np.array(cleaned)  # Return as numpy array
    except Exception as e:
        print(f"Error parsing value: {fsq_category_ids}")
        raise e

# Apply the parser
places['fsq_category_ids'] = places['fsq_category_ids'].apply(parse_fsq_category_ids)

# Filter places that have beer-related categories
beer_places = places[places['fsq_category_ids'].apply(lambda ids: any(cat_id in beer_category_ids for cat_id in ids))]

# Step 5: Aggregate POI counts per country
print("Aggregating data...")
country_poi_counts = beer_places.groupby('country').size().reset_index(name='poi_count')

# Merge coordinates for heatmap visualization
country_coordinates = beer_places.groupby('country')[['latitude', 'longitude']].mean().reset_index()
country_poi_counts = country_poi_counts.merge(country_coordinates, on='country')

# Step 6: Create a heatmap visualization
print("Creating heatmap...")
map_center = [20, 0]  # Global centering
heatmap = folium.Map(location=map_center, zoom_start=2)

# Prepare data for the heatmap
heatmap_data = [
    [row['latitude'], row['longitude'], row['poi_count']]
    for _, row in country_poi_counts.iterrows()
]

# Add the heatmap layer
HeatMap(heatmap_data, radius=15, blur=10, max_zoom=1).add_to(heatmap)

# Save and display the map
heatmap_file = "beer_density_heatmap.html"
heatmap.save(heatmap_file)
print(f"Heatmap saved to {heatmap_file}. Open it in a browser to view.")
