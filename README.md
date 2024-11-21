Create and source a Python virtual environment, e.g. use `~/.venv`.

Open a Terminal and run:

```
mkdir -p ~/.venv
python3 -m venv ~/.venv
source ~/.venv/bin/activate
```

Using the Terminal install Python modules (optional: update `pip`) adn source the environment variables.

```
python3 -m pip install pandas geopandas folium pyarrow s3fs requests jenkspy pycountry
python3 -m pip install --upgrade pip
```

Source environment variables if any

```
source .env
```

Run script `fivestar<x>.py`

```
python3 fivestar.py
```

Create more file versions

```
touch fivestar<x>.py
```

Links
---

https://opensource.foursquare.com/os-places/

https://docs.foursquare.com/data-products/docs/access-fsq-os-places

https://stackoverflow.com/questions/63865264/folium-black-colored-countries

https://en.wikipedia.org/wiki/Jenks_natural_breaks_optimization

https://marketplace.visualstudio.com/items?itemName=ms-toolsai.datawrangler
