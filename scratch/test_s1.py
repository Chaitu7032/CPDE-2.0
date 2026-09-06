import os
import rasterio
import planetary_computer
import pystac_client
from rasterio.vrt import WarpedVRT

proj_dir = os.path.abspath(os.path.join(os.path.dirname(rasterio.__file__), "proj_data"))
os.environ["PROJ_LIB"] = proj_dir
os.environ["PROJ_DATA"] = proj_dir

client = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
s = client.search(collections=["sentinel-1-grd"], bbox=[81.35, 16.35, 81.45, 16.45], datetime="2026-08-01/2026-08-30", max_items=1)
it = next(s.items())
signed = planetary_computer.sign(it)
asset = signed.assets.get("vh")

with rasterio.Env(PROJ_LIB=proj_dir, PROJ_DATA=proj_dir, GTIFF_SRS_SOURCE="EPSG"):
    with rasterio.open(asset.href) as src:
        gcps, gcp_crs = src.gcps
        with WarpedVRT(src, crs="EPSG:4326", src_crs=gcp_crs) as vrt:
            val = list(vrt.sample([(81.4025, 16.4081)]))
            print("Successfully sampled SAR pixel:", val)
