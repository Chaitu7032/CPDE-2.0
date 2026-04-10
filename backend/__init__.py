"""Backend package root for CPDE.

Force the process to use the bundled PROJ data shipped with the Python
environment instead of an inherited PostGIS installation path.
"""

import os

from rasterio._env import GDALDataFinder, PROJDataFinder


_PROJ_DATA_DIR = PROJDataFinder().search()
_GDAL_DATA_DIR = GDALDataFinder().search()
if _PROJ_DATA_DIR:
	os.environ["PROJ_DATA"] = _PROJ_DATA_DIR
	os.environ["PROJ_LIB"] = _PROJ_DATA_DIR
if _GDAL_DATA_DIR:
	os.environ["GDAL_DATA"] = _GDAL_DATA_DIR
