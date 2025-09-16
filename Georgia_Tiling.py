# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 20:11:16 2025
 
@author: Faraz Gorginpaveh
"""

import geopandas as gpd
from shapely.geometry import box
import numpy as np
import os

# === Step 1: Load Georgia from shapefile
shapefile_path = "/tl_2024_us_state.shp"
states = gpd.read_file(shapefile_path).to_crs("EPSG:4326")

georgia = states[states['NAME'].str.lower() == 'georgia']

ga_geom = georgia.geometry.values[0]
ga_bounds = ga_geom.bounds  # (minx, miny, maxx, maxy)

# === Step 2: Generate grid (10x10) over bounding box
minx, miny, maxx, maxy = ga_bounds
rows, cols = 10, 10
width = (maxx - minx) / cols
height = (maxy - miny) / rows

tiles = []
for i in range(rows):
    for j in range(cols):
        x1 = minx + j * width
        y1 = miny + i * height
        x2 = x1 + width
        y2 = y1 + height
        tile = box(x1, y1, x2, y2)
        tiles.append(tile)

# === Step 3: Convert to GeoDataFrame and clip to Georgia boundary
tiles_gdf = gpd.GeoDataFrame(geometry=tiles, crs="EPSG:4326")
tiles_clipped = gpd.overlay(tiles_gdf, georgia, how="intersection")

# Add row and col indices to each tile
tiles_clipped["row"] = np.repeat(np.arange(rows), cols)[:len(tiles_clipped)]
tiles_clipped["col"] = np.tile(np.arange(cols), rows)[:len(tiles_clipped)]
tiles_clipped["tile_id"] = ["tile_%02d_%02d" % (r, c) for r, c in zip(tiles_clipped["row"], tiles_clipped["col"])]

# === Step 4: Save to file (optional)
output_path = "/GA_100_tiles.shp"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
tiles_clipped.to_file(output_path)

print(f"✅ Done. 100 tiles covering Georgia saved to:\n{output_path}")



#######################################
#######################################
#######################################
#######################################

import geopandas as gpd
from shapely.geometry import shape
from pystac_client import Client
from planetary_computer import sign
from shapely.ops import unary_union
import pandas as pd
from tqdm import tqdm

# === Load Georgia tiles
tile_path = "GA_100_tiles.shp"
tiles_gdf = gpd.read_file(tile_path).to_crs("EPSG:4326")

# === Connect to STAC API
client = Client.open("///")

# === Storage for results
tile_stats = []

print("🔍 Querying NAIP 2017 coverage per tile with simplified geometries...")

for idx, row in tqdm(tiles_gdf.iterrows(), total=len(tiles_gdf)):
    tile_geom = row.geometry.simplify(tolerance=0.001, preserve_topology=True)
    tile_id = row["tile_id"]

    try:
        search = client.search(
            collections=["naip"],
            intersects=tile_geom.__geo_interface__,
            datetime="2017-01-01/2018-12-31",
            limit=100
        )

        items = list(search.items())
        total_size = 0
        image_geoms = []

        for item in items:
            if "image" in item.assets:
                total_size += item.assets["image"].extra_fields.get("file:size", 0)
            if item.geometry:
                image_geoms.append(shape(item.geometry))

        # Calculate coverage
        if image_geoms:
            union_geom = unary_union(image_geoms)
            coverage_area = union_geom.intersection(tile_geom).area
            tile_area = tile_geom.area
            percent_covered = (coverage_area / tile_area) * 100
        else:
            percent_covered = 0

        tile_stats.append({
            "tile_id": tile_id,
            "num_images": len(items),
            "percent_covered": percent_covered
        })

    except Exception as e:
        print(f"❌ Error for tile {tile_id}: {e}")
        tile_stats.append({
            "tile_id": tile_id,
            "num_images": -1,
            "percent_covered": -1
        })

# === Save summary
df_stats = pd.DataFrame(tile_stats)
df_stats.to_csv("GA_NAIP2017_Tile_Coverage.csv", index=False)
print("\n✅ Done. Saved tile-by-tile coverage to GA_NAIP2017_Tile_Coverage.csv")

