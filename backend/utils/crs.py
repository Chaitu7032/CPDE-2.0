from __future__ import annotations

from typing import Any

from pyproj import CRS, Transformer
from shapely.geometry import shape, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

API_CRS_EPSG = 4326
STORAGE_CRS_EPSG = 32644

_API_CRS = CRS.from_epsg(API_CRS_EPSG)
_STORAGE_CRS = CRS.from_epsg(STORAGE_CRS_EPSG)

_TO_STORAGE = Transformer.from_crs(_API_CRS, _STORAGE_CRS, always_xy=True)
_TO_API = Transformer.from_crs(_STORAGE_CRS, _API_CRS, always_xy=True)


def shapely_wgs84_to_storage(geom: BaseGeometry) -> BaseGeometry:
    return transform(_TO_STORAGE.transform, geom)


def shapely_storage_to_api(geom: BaseGeometry) -> BaseGeometry:
    return transform(_TO_API.transform, geom)


def geometry_geojson_wgs84_to_storage(geometry_geojson: dict[str, Any]) -> dict[str, Any]:
    geom = shape(geometry_geojson)
    return mapping(shapely_wgs84_to_storage(geom))


def geometry_geojson_storage_to_api(geometry_geojson: dict[str, Any]) -> dict[str, Any]:
    geom = shape(geometry_geojson)
    return mapping(shapely_storage_to_api(geom))
