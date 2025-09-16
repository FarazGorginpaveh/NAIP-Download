# -*- coding: utf-8 -*- 
"""
Created on Wed Jul 23 11:45:50 2025

@author: Faraz Gorginpaveh
"""

# ============================================================
# Parallel NAIP Download Only (One Tile per Core at a Time)
# ============================================================

import os
import requests
import geopandas as gpd
from planetary_computer import sign
from pystac_client import Client
import glob
from multiprocessing import Pool
from tqdm import tqdm

# === CONFIGURATION ===
year = 2017
tile_shapefile = "GA_100_tiles.shp"
base_dir = "Georgia"
client = Client.open("")

def download_tile(tile_id):
    download_dir = os.path.join(base_dir, f"{tile_id}_Images")
    os.makedirs(download_dir, exist_ok=True)

    # Load geometry for this tile
    try:
        tiles_gdf = gpd.read_file(tile_shapefile).to_crs("EPSG:4326")
        tile_row = tiles_gdf[tiles_gdf["tile_id"] == tile_id]
        if tile_row.empty:
            print(f"❌ {tile_id} not found in shapefile.")
            return
        geom = tile_row.iloc[0].geometry
    except Exception as e:
        print(f"❌ Failed to read geometry for {tile_id}: {e}")
        return

    # Skip if already downloaded
    existing_tifs = glob.glob(os.path.join(download_dir, "*.tif"))
    if len(existing_tifs) >= 1:  # or use a stricter threshold if needed
        print(f"⏩ Skipping {tile_id} — already has {len(existing_tifs)} TIFFs.")
        return

    # Query NAIP images
    print(f"🔍 Querying NAIP {year} for {tile_id}...")
    try:
        search = client.search(
            collections=["naip"],
            intersects=geom.__geo_interface__,
            datetime=f"{year}-01-01/{year}-12-31",
            limit=100
        )
        items = list(search.items())
    except Exception as e:
        print(f"❌ Search failed for {tile_id}: {e}")
        return

    print(f"📦 Found {len(items)} images for {tile_id}")

    # Download images
    for item in items:
        try:
            signed = sign(item)
            url = signed.assets["image"].href
            out_fp = os.path.join(download_dir, f"{item.id}.tif")

            if os.path.exists(out_fp):
                continue  # Skip if already downloaded

            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(out_fp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            print(f"❌ Error downloading {item.id} for {tile_id}: {e}")

    print(f"✅ Finished downloading {tile_id}")


def main():
    tiles_gdf = gpd.read_file(tile_shapefile)
    tile_ids = tiles_gdf["tile_id"].tolist()

    num_workers = 15
    with Pool(num_workers) as pool:
        for _ in tqdm(pool.imap_unordered(download_tile, tile_ids), total=len(tile_ids)):
            pass


if __name__ == "__main__":
    main()

