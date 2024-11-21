import pandas as pd
import folium
import geopandas as gpd
import numpy as np
import os
import s3fs
import jenkspy
import pycountry

# S3 paths for the datasets
places_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/"
categories_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/"

# Local file paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Geospatial data for country boundaries (GeoJSON)
world_geojson = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"

# Country surface area CSV
surface_area_csv = "https://datahub.io/core/geo-countries/r/countries.csv"

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

import pandas as pd
import folium
import geopandas as gpd
import numpy as np
import os
import s3fs
import jenkspy
import pycountry

# S3 paths for the datasets
places_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/"
categories_s3_path = "s3://fsq-os-places-us-east-1/release/dt=2024-11-19/categories/parquet/"

# Local file paths
places_file = "places.parquet"
categories_file = "categories.parquet"

# Comprehensive country surface area data (Alpha-2 codes) in square kilometers
country_areas = {
    "AF": 652230, "AL": 28748, "DZ": 2381741, "AS": 199, "AD": 468, "AO": 1246700,
    "AG": 443, "AR": 2780400, "AM": 29743, "AU": 7692024, "AT": 83879, "AZ": 86600,
    "BS": 13943, "BH": 760, "BD": 147570, "BB": 430, "BY": 207600, "BE": 30528,
    "BZ": 22966, "BJ": 112622, "BM": 54, "BT": 38394, "BO": 1098581, "BA": 51209,
    "BW": 581730, "BR": 8515767, "BN": 5765, "BG": 110994, "BF": 272967, "BI": 27834,
    "CV": 4033, "KH": 181035, "CM": 475442, "CA": 9984670, "KY": 264, "CF": 622984,
    "TD": 1284000, "CL": 756102, "CN": 9596961, "CO": 1141748, "KM": 2235,
    "CG": 342000, "CD": 2344858, "CR": 51100, "CI": 322463, "HR": 56594, "CU": 109884,
    "CY": 9251, "CZ": 78865, "DK": 42931, "DJ": 23200, "DM": 751, "DO": 48671,
    "EC": 276841, "EG": 1002450, "SV": 21041, "GQ": 28051, "ER": 117600, "EE": 45227,
    "SZ": 17364, "ET": 1104300, "FJ": 18274, "FI": 338424, "FR": 551695, "GA": 267668,
    "GM": 11295, "GE": 69700, "DE": 357022, "GH": 238533, "GR": 131957, "GD": 344,
    "GU": 549, "GT": 108889, "GN": 245857, "GW": 36125, "GY": 214969, "HT": 27750,
    "VA": 0.44, "HN": 112492, "HK": 1104, "HU": 93028, "IS": 103000, "IN": 3287263,
    "ID": 1904569, "IR": 1648195, "IQ": 438317, "IE": 70273, "IL": 20770, "IT": 301340,
    "JM": 10991, "JP": 377975, "JO": 89342, "KZ": 2724900, "KE": 580367, "KI": 726,
    "KR": 100210, "KW": 17818, "KG": 199951, "LA": 236800, "LV": 64559, "LB": 10452,
    "LS": 30355, "LR": 111369, "LY": 1759540, "LT": 65300, "LU": 2586, "MG": 587041,
    "MW": 118484, "MY": 330803, "MV": 298, "ML": 1240192, "MT": 316, "MH": 181,
    "MR": 1030700, "MU": 2040, "MX": 1964375, "FM": 702, "MD": 33846, "MC": 2.02,
    "MN": 1564110, "ME": 13812, "MA": 446550, "MZ": 801590, "MM": 676578, "NA": 825615,
    "NR": 21, "NP": 147516, "NL": 41850, "NZ": 270467, "NI": 130373, "NE": 1267000,
    "NG": 923768, "NO": 385207, "OM": 309500, "PK": 881913, "PW": 459, "PA": 75417,
    "PG": 462840, "PY": 406752, "PE": 1285216, "PH": 300000, "PL": 312679, "PT": 92212,
    "PR": 9104, "QA": 11586, "RO": 238397, "RU": 17098242, "RW": 26338, "KN": 261,
    "LC": 617, "VC": 389, "WS": 2842, "SM": 61, "ST": 964, "SA": 2149690, "SN": 196722,
    "RS": 77474, "SC": 459, "SL": 71740, "SG": 719, "SK": 49037, "SI": 20273,
    "SB": 28896, "SO": 637657, "ZA": 1219090, "ES": 505990, "LK": 65610, "SD": 1861484,
    "SR": 163820, "SE": 450295, "CH": 41284, "SY": 185180, "TW": 36197, "TJ": 143100,
    "TZ": 945087, "TH": 513120, "TL": 14874, "TG": 56785, "TO": 747, "TT": 5130,
    "TN": 163610, "TR": 783356, "TM": 488100, "UG": 241038, "UA": 603550, "AE": 83600,
    "GB": 243610, "US": 9833517, "UY": 176215, "UZ": 447400, "VU": 12189, "VE": 916445,
    "VN": 331212, "YE": 527968, "ZM": 752612, "ZW": 390757,
}

# Continue with the script...
print(f"Loaded country areas for {len(country_areas)} countries.")

# Step 4: Filter for 'beer'-related categories
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

# Step 5: Calculate POI density
country_poi_counts = beer_places.groupby('country').size().reset_index(name='poi_count')
country_poi_counts['surface_area'] = country_poi_counts['country'].map(country_areas)
country_poi_counts = country_poi_counts.dropna(subset=['surface_area'])
country_poi_counts['poi_density'] = country_poi_counts['poi_count'] / country_poi_counts['surface_area']
country_poi_counts['poi_density_scaled'] = country_poi_counts['poi_density'] * 1e6  # Scale density values
print(f"Calculated POI density:\n{country_poi_counts}")

# Step 6: Classify densities using natural breaks or fallback
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

# Step 7: Load world GeoJSON for mapping
world = gpd.read_file(world_geojson)
world = world.merge(country_poi_counts, left_on="id", right_on="country", how="left")

# Step 8: Create the map
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
