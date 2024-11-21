import pandas as pd
import folium
import geopandas as gpd
import numpy as np
import os
import s3fs
import jenkspy
from jenkspy import JenksNaturalBreaks

# S3 paths for the datasets
places_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/"
categories_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/"

# Local file paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Geospatial data for country boundaries (GeoJSON)
world_geojson = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"

# Country surface area data in square kilometers (Alpha-2 codes)
country_areas = {
    "US": 9833517, "CA": 9984670, "BR": 8515767, "CN": 9596961, "AU": 7692024,
    "IN": 3287263, "RU": 17098242, "FR": 551695, "DE": 357022, "GB": 243610,
    "ZA": 1219090, "JP": 377975, "MX": 1964375, "AR": 2780400, "CO": 1141748,
    "KR": 100210, "TH": 513120, "TR": 783356, "IT": 301340, "ES": 505990,
    "NL": 41850, "BE": 30528, "PL": 312679, "SE": 450295, "NO": 385207,
    "FI": 338145, "DK": 42931, "CH": 41284, "AT": 83879, "HU": 93028,
    "PT": 92212, "GR": 131957, "IE": 70273, "CZ": 78865, "SK": 49037,
    "LU": 2586, "LT": 65300, "LV": 64589, "EE": 45228, "MT": 316,
}

# Step 1: Download datasets using s3fs with anonymous access
def download_parquet_from_s3(s3_dir_path, local_path):
    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        print(f"Downloading files from {s3_dir_path} to {local_path}...")
        fs = s3fs.S3FileSystem(anon=True)  # Anonymous access
        try:
            files = fs.ls(s3_dir_path)
            parquet_file = next((f for f in files if f.endswith('.parquet')), None)
            if not parquet_file:
                raise FileNotFoundError(f"No Parquet file found in {s3_dir_path}")
            with fs.open(parquet_file, 'rb') as s3_file:
                with open(local_path, 'wb') as local_file:
                    local_file.write(s3_file.read())
            print(f"Download successful: {local_path}")
        except Exception as e:
            print(f"Error downloading {s3_dir_path}: {e}")
            raise

download_parquet_from_s3(places_s3_path, places_file)
download_parquet_from_s3(categories_s3_path, categories_file)

# Step 2: Load datasets
places = pd.read_parquet(places_file, engine="pyarrow", columns=["fsq_category_ids", "latitude", "longitude", "country"])
categories = pd.read_parquet(categories_file, engine="pyarrow", columns=["category_id", "category_name"])

# Step 3: Filter for 'beer'-related categories
beer_categories = categories[categories['category_name'].str.contains('beer', case=False, na=False)]
beer_category_ids = set(beer_categories['category_id'])
print(f"Beer categories: {beer_category_ids}")

def parse_fsq_category_ids(fsq_category_ids):
    try:
        if str(fsq_category_ids).strip() == "":
            return np.array([])
        return np.array(str(fsq_category_ids).strip("[]").replace("'", "").split())
    except Exception as e:
        print(f"Error parsing value: {fsq_category_ids}")
        raise e

places['fsq_category_ids'] = places['fsq_category_ids'].apply(parse_fsq_category_ids)
beer_places = places[places['fsq_category_ids'].apply(lambda ids: any(cat_id in beer_category_ids for cat_id in ids))]

if beer_places.empty:
    raise ValueError("No beer-related POIs found after filtering.")
print(f"Filtered beer_places DataFrame:\n{beer_places.head()}")

# Step 4: Calculate POI density
country_poi_counts = beer_places.groupby('country').size().reset_index(name='poi_count')
country_poi_counts['surface_area'] = country_poi_counts['country'].map(country_areas)
country_poi_counts = country_poi_counts.dropna(subset=['surface_area'])
country_poi_counts['poi_density'] = country_poi_counts['poi_count'] / country_poi_counts['surface_area']
country_poi_counts['poi_density_scaled'] = country_poi_counts['poi_density'] * 1e6  # Scale density values
print(f"Calculated POI density:\n{country_poi_counts}")

# Step 5: Classify densities using natural breaks or fallback
poi_densities = country_poi_counts['poi_density_scaled'].dropna().values

try:
    if len(np.unique(poi_densities)) >= 5:
        jenks_breaks = jenkspy.jenks_breaks(poi_densities, n_classes=5)
    else:
        raise ValueError("Not enough unique values for Jenks Natural Breaks.")
except Exception as e:
    print(f"Error computing Jenks Natural Breaks: {e}")
    print("Falling back to equal intervals.")
    jenks_breaks = np.linspace(poi_densities.min(), poi_densities.max(), num=6)

print(f"Jenks breaks: {jenks_breaks}")
country_poi_counts['density_class'] = pd.cut(country_poi_counts['poi_density_scaled'], bins=jenks_breaks, labels=False)

print(country_poi_counts)

# Step 6: Load world GeoJSON for mapping
world = gpd.read_file(world_geojson)
world = world.merge(country_poi_counts, left_on="id", right_on="country", how="left")

# Step 7: Create the map
map_center = [20, 0]
range_map = folium.Map(location=map_center, zoom_start=2)
folium.Choropleth(
    geo_data=world,
    name='choropleth',
    data=country_poi_counts,
    columns=['country', 'density_class'],
    key_on='feature.properties.id',
    fill_color='YlGn',
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name='POI Density per Country'
).add_to(range_map)

range_map_file = "beer_density_range_map.html"
range_map.save(range_map_file)
print(f"Range map saved to {range_map_file}.")
