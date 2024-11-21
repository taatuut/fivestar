import pandas as pd
import folium
import s3fs
import os
import ast

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

# Step 4: Filter Places dataset for beer-related categories
print("Filtering Places dataset for beer-related categories...")

def extract_category_ids(fsq_category_ids):
    """Extract category IDs from fsq_category_ids column, handling both array and string representations."""
    if pd.isna(fsq_category_ids):
        return []
    if isinstance(fsq_category_ids, list):
        return fsq_category_ids  # Already a list, return as is
    if isinstance(fsq_category_ids, str):
        try:
            # If it's a string that looks like a list, parse it
            return ast.literal_eval(fsq_category_ids)
        except (ValueError, SyntaxError):
            return []  # Return empty if it can't be parsed
    return []  # Default case

# Apply the extraction function
places['fsq_category_ids'] = places['fsq_category_ids'].apply(extract_category_ids)

# Filter places that have beer-related categories
beer_places = places[places['fsq_category_ids'].apply(lambda ids: any(cat_id in beer_category_ids for cat_id in ids))]

# Step 5: Join beer-related Places with Categories
print("Joining Places with Categories...")
beer_places = beer_places.explode('fsq_category_ids')  # Split rows for each category ID
beer_places = beer_places[beer_places['fsq_category_ids'].isin(beer_category_ids)]
beer_places = beer_places.merge(beer_categories, left_on="fsq_category_ids", right_on="category_id", suffixes=("_place", "_category"))

# Step 6: Analyze highest density categories per country
print("Analyzing data...")
beer_density = beer_places.groupby(['country', 'category_name']).size().reset_index(name='poi_count')
highest_density = beer_density.loc[beer_density.groupby('country')['poi_count'].idxmax()]

# Step 7: Create a stunning map visualization
print("Creating map...")
map_center = [20, 0]  # Global centering
beer_map = folium.Map(location=map_center, zoom_start=2)

# Add markers to the map
for _, row in highest_density.iterrows():
    country = row['country']
    category = row['category_name']
    poi_count = row['poi_count']
    
    # Filter to get coordinates of POIs in this country/category
    country_category_places = beer_places[
        (beer_places['country'] == country) & 
        (beer_places['category_name'] == category)
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
