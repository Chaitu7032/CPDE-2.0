from __future__ import annotations

import logging
import math
from typing import Any

from pyproj import CRS, Transformer
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import transform, unary_union
from shapely.validation import explain_validity

LOGGER = logging.getLogger(__name__)

INPUT_EPSG = 4326
PROCESSING_EPSG = 32644
SENTINEL2_RESOLUTION_M = 10.0
MODIS_RESOLUTION_M = 1000.0


def _to_float(value: Any) -> float | None:
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(as_float):
        return None
    return as_float


def _ring_warnings(ring: Any, ring_path: str) -> list[str]:
    warnings: list[str] = []
    if not isinstance(ring, list) or len(ring) < 4:
        warnings.append(f"{ring_path}: must contain at least 4 coordinate points")
        return warnings

    first = ring[0]
    last = ring[-1]
    if not (
        isinstance(first, list)
        and isinstance(last, list)
        and len(first) >= 2
        and len(last) >= 2
        and first[0] == last[0]
        and first[1] == last[1]
    ):
        warnings.append(f"{ring_path}: polygon ring must be closed (first point equals last point)")

    for idx, pt in enumerate(ring):
        if not isinstance(pt, list) or len(pt) < 2:
            warnings.append(f"{ring_path}[{idx}]: invalid coordinate point")
            continue
        lon = _to_float(pt[0])
        lat = _to_float(pt[1])
        if lon is None or lat is None:
            warnings.append(f"{ring_path}[{idx}]: longitude/latitude must be numeric")
            continue
        if lon < -180.0 or lon > 180.0 or lat < -90.0 or lat > 90.0:
            warnings.append(f"{ring_path}[{idx}]: coordinates out of WGS84 lon/lat range")
    return warnings


def _geometry_structure_warnings(geometry: Any) -> tuple[list[str], str | None]:
    warnings: list[str] = []
    if not isinstance(geometry, dict):
        return ["geometry must be a GeoJSON object"], None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Polygon":
        if not isinstance(coords, list) or len(coords) == 0:
            warnings.append("Polygon coordinates are missing")
        else:
            warnings.extend(_ring_warnings(coords[0], "coordinates[0]"))
    elif geom_type == "MultiPolygon":
        if not isinstance(coords, list) or len(coords) == 0:
            warnings.append("MultiPolygon coordinates are missing")
        else:
            for poly_idx, polygon_rings in enumerate(coords):
                if not isinstance(polygon_rings, list) or len(polygon_rings) == 0:
                    warnings.append(f"coordinates[{poly_idx}]: polygon rings are missing")
                    continue
                warnings.extend(_ring_warnings(polygon_rings[0], f"coordinates[{poly_idx}][0]"))
    else:
        warnings.append("geometry type must be Polygon or MultiPolygon")

    return warnings, geom_type


def _estimate_pixel_coverage(area_sqm: float, bbox_width_m: float, bbox_height_m: float, resolution_m: float) -> dict[str, Any]:
    pixel_area_sqm = resolution_m * resolution_m
    area_based = area_sqm / pixel_area_sqm if pixel_area_sqm > 0 else None
    bbox_based = (bbox_width_m / resolution_m) * (bbox_height_m / resolution_m) if resolution_m > 0 else None

    return {
        "pixel_size_m": resolution_m,
        "pixel_area_sqm": pixel_area_sqm,
        "estimated_pixels_area_based": round(area_based, 3) if area_based is not None else None,
        "estimated_pixels_area_based_ceiling": int(math.ceil(area_based)) if area_based is not None else None,
        "estimated_pixels_bbox_based_upper_bound": int(math.ceil(bbox_based)) if bbox_based is not None else None,
    }


def compute_field_technical_details(geometry: Any) -> dict[str, Any]:
    """Compute transparent and defensive field technical details for a WGS84 GeoJSON polygon."""
    structure_warnings, geometry_type = _geometry_structure_warnings(geometry)
    warnings = list(structure_warnings)

    response: dict[str, Any] = {
        "success": True,
        "warnings": warnings,
        "validation": {
            "is_valid": False,
            "geometry_type": geometry_type,
            "shapely_valid": None,
            "validity_reason": None,
        },
        "crs": {
            "input_epsg": f"EPSG:{INPUT_EPSG}",
            "processing_epsg": f"EPSG:{PROCESSING_EPSG}",
            "transform_ok": False,
        },
        "metrics": {
            "area_sqm": None,
            "centroid_wgs84": {"lon": None, "lat": None},
            "centroid_utm": {"x": None, "y": None},
            "bbox_wgs84": {"min_lon": None, "min_lat": None, "max_lon": None, "max_lat": None},
            "bbox_utm": {"min_x": None, "min_y": None, "max_x": None, "max_y": None, "width_m": None, "height_m": None},
        },
        "pixel_coverage": {
            "sentinel2_10m": None,
            "modis_1000m": None,
            "notes": [
                "Pixel counts are estimates (area-based and bbox-based upper bound).",
                "Sentinel-2 estimate uses 10m resolution.",
                "MODIS estimate uses 1000m resolution as coarse reference.",
            ],
        },
    }

    try:
        raw_geom = shape(geometry)
    except Exception as exc:
        warnings.append(f"unable to parse geometry: {exc}")
        response["success"] = False
        response["validation"]["validity_reason"] = "unparseable geometry"
        return response

    if isinstance(raw_geom, MultiPolygon):
        geom = unary_union(list(raw_geom.geoms))
    else:
        geom = raw_geom

    if not isinstance(geom, Polygon):
        warnings.append("geometry is not polygonal after processing")
        response["success"] = False
        response["validation"]["validity_reason"] = "non-polygon geometry"
        return response

    shapely_valid = bool(geom.is_valid)
    validity_reason = explain_validity(geom)
    response["validation"]["shapely_valid"] = shapely_valid
    response["validation"]["validity_reason"] = validity_reason

    if not shapely_valid:
        warnings.append(f"invalid polygon: {validity_reason}")
        response["success"] = False
        return response

    if warnings:
        response["success"] = False
        return response

    try:
        to_utm = Transformer.from_crs(CRS.from_epsg(INPUT_EPSG), CRS.from_epsg(PROCESSING_EPSG), always_xy=True)
        to_wgs84 = Transformer.from_crs(CRS.from_epsg(PROCESSING_EPSG), CRS.from_epsg(INPUT_EPSG), always_xy=True)
        geom_utm = transform(to_utm.transform, geom)
    except Exception as exc:
        LOGGER.exception("Failed CRS transform for field technical details")
        warnings.append(f"crs transform failed: {exc}")
        response["success"] = False
        return response

    response["crs"]["transform_ok"] = True

    area_sqm = abs(float(geom_utm.area))
    centroid_utm = geom_utm.centroid
    centroid_lon, centroid_lat = to_wgs84.transform(centroid_utm.x, centroid_utm.y)

    min_lon, min_lat, max_lon, max_lat = geom.bounds
    min_x, min_y, max_x, max_y = geom_utm.bounds
    width_m = max_x - min_x
    height_m = max_y - min_y

    response["metrics"] = {
        "area_sqm": round(area_sqm, 3),
        "centroid_wgs84": {
            "lon": round(float(centroid_lon), 8),
            "lat": round(float(centroid_lat), 8),
        },
        "centroid_utm": {
            "x": round(float(centroid_utm.x), 3),
            "y": round(float(centroid_utm.y), 3),
        },
        "bbox_wgs84": {
            "min_lon": round(float(min_lon), 8),
            "min_lat": round(float(min_lat), 8),
            "max_lon": round(float(max_lon), 8),
            "max_lat": round(float(max_lat), 8),
        },
        "bbox_utm": {
            "min_x": round(float(min_x), 3),
            "min_y": round(float(min_y), 3),
            "max_x": round(float(max_x), 3),
            "max_y": round(float(max_y), 3),
            "width_m": round(float(width_m), 3),
            "height_m": round(float(height_m), 3),
        },
    }

    response["pixel_coverage"] = {
        "sentinel2_10m": _estimate_pixel_coverage(area_sqm, width_m, height_m, SENTINEL2_RESOLUTION_M),
        "modis_1000m": _estimate_pixel_coverage(area_sqm, width_m, height_m, MODIS_RESOLUTION_M),
        "notes": response["pixel_coverage"]["notes"],
    }

    response["validation"]["is_valid"] = True
    return response
