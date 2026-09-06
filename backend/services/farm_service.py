"""
CPDE v2 Farm & Field Registration Service
Implements rigorous geospatial validation, UTM canonical projections,
reverse geocoding, and 10m adaptive grid generation.
"""

from __future__ import annotations
import json
import logging
import math
from datetime import date, datetime
from typing import Any, Optional

import httpx
from pyproj import Transformer
from shapely.geometry import Polygon, Point, mapping, shape
from shapely.validation import explain_validity
from sqlalchemy import select, text

from backend.db.connection import async_session
from backend.db.models import Farm, Field, FieldGridCell, User
from backend.pipelines.grid_generation import generate_rotated_grid
from backend.utils.crs import STORAGE_CRS_EPSG

logger = logging.getLogger(__name__)

# Transformers
WGS84_TO_UTM44N = Transformer.from_crs("EPSG:4326", f"EPSG:{STORAGE_CRS_EPSG}", always_xy=True)
UTM44N_TO_WGS84 = Transformer.from_crs(f"EPSG:{STORAGE_CRS_EPSG}", "EPSG:4326", always_xy=True)


def validate_field_polygon(coords: list[list[float]]) -> dict[str, Any]:
    """Perform instant research-grade topological & geometric checks.
    
    Expected format: [[lon, lat], [lon, lat], ...]
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(coords, list) or len(coords) < 3:
        return {
            "valid": False,
            "errors": ["Polygon requires at least 3 distinct geographic coordinates."],
            "warnings": warnings,
            "quality_score": 0.0,
        }

    # 1. Coordinate range check (WGS84)
    for idx, pt in enumerate(coords):
        if not isinstance(pt, (list, tuple)) or len(pt) < 2:
            errors.append(f"Coordinate #{idx + 1} is malformed.")
            continue
        lon, lat = pt[0], pt[1]
        if not (isinstance(lon, (int, float)) and isinstance(lat, (int, float))):
            errors.append(f"Coordinate #{idx + 1} must be numeric.")
            continue
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
            errors.append(f"Coordinate #{idx + 1} ({lon}, {lat}) is outside valid WGS84 boundaries.")

    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings, "quality_score": 0.0}

    # 2. Check closure (auto-close if necessary)
    closed_coords = list(coords)
    if closed_coords[0][0] != closed_coords[-1][0] or closed_coords[0][1] != closed_coords[-1][1]:
        closed_coords.append(closed_coords[0])

    if len(closed_coords) < 4:
        return {
            "valid": False,
            "errors": ["Closed polygon requires at least 4 coordinate vertices."],
            "warnings": warnings,
            "quality_score": 0.0,
        }

    # 3. Duplicate sequential vertices check
    cleaned: list[list[float]] = [closed_coords[0]]
    for pt in closed_coords[1:]:
        last = cleaned[-1]
        if abs(pt[0] - last[0]) > 1e-9 or abs(pt[1] - last[1]) > 1e-9:
            cleaned.append(pt)

    if len(cleaned) < 4:
        return {
            "valid": False,
            "errors": ["Polygon collapses to line or point after removing duplicate vertices."],
            "warnings": warnings,
            "quality_score": 0.0,
        }

    # 4. Construct Shapely Polygon & check validity / self-intersection
    try:
        poly_wgs = Polygon(cleaned)
    except Exception as exc:
        return {
            "valid": False,
            "errors": [f"Failed to construct geometric polygon: {exc}"],
            "warnings": warnings,
            "quality_score": 0.0,
        }

    if not poly_wgs.is_valid:
        reason = explain_validity(poly_wgs)
        return {
            "valid": False,
            "errors": [f"Geometry is invalid (self-intersecting or bow-tie): {reason}"],
            "warnings": warnings,
            "quality_score": 0.0,
        }

    # 5. Transform to metric UTM Zone 44N
    utm_pts = [WGS84_TO_UTM44N.transform(lon, lat) for lon, lat in poly_wgs.exterior.coords]
    poly_utm = Polygon(utm_pts)

    area_sqm = float(poly_utm.area)
    area_ha = area_sqm / 10000.0
    perimeter_m = float(poly_utm.length)

    # 6. Area reasonableness (0.01 ha to 50,000 ha)
    if area_ha < 0.01:
        errors.append(f"Parcel area ({area_ha:.4f} ha / {area_sqm:.1f} m²) is too small for satellite grid analysis (< 100 m²).")
    elif area_ha > 10000.0:
        warnings.append(f"Parcel area ({area_ha:.1f} ha) is exceptionally large; analysis will be subdivided.")

    # 7. Geometry Quality Score (isoperimetric quotient & vertex sanity)
    # Perfect circle = 1.0, square ~ 0.785, long thin strip < 0.2
    compactness = (4.0 * math.pi * area_sqm) / (perimeter_m ** 2) if perimeter_m > 0 else 0.0
    compactness = max(min(compactness, 1.0), 0.0)

    quality_score = round(min(max(compactness * 0.5 + 0.5, 0.4), 1.0), 3)

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "polygon_wgs": poly_wgs,
        "polygon_utm": poly_utm,
        "area_sqm": round(area_sqm, 2),
        "area_ha": round(area_ha, 4),
        "perimeter_m": round(perimeter_m, 2),
        "centroid_wgs": [float(poly_wgs.centroid.x), float(poly_wgs.centroid.y)],
        "centroid_utm": [float(poly_utm.centroid.x), float(poly_utm.centroid.y)],
        "bbox_wgs": list(poly_wgs.bounds),
        "quality_score": quality_score,
        "registration_confidence": round(quality_score * 0.95, 3),
    }


async def reverse_geocode(lon: float, lat: float) -> dict[str, str]:
    """Reverse geocode lon/lat into Village, Mandal, District, State, Country using OpenStreetMap Nominatim."""
    result = {
        "village": "Local Agricultural Region",
        "mandal": "Taluk / Mandal",
        "district": "Agricultural District",
        "state": "State",
        "country": "India",
    }
    try:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "zoom": 14,
            "addressdetails": 1,
        }
        headers = {"User-Agent": "CPDE-Precision-Ag-Platform/2.0"}
        async with httpx.AsyncClient(timeout=4.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                addr = data.get("address", {})
                result["village"] = addr.get("village") or addr.get("hamlet") or addr.get("suburb") or addr.get("neighbourhood") or result["village"]
                result["mandal"] = addr.get("county") or addr.get("subdistrict") or addr.get("municipality") or result["mandal"]
                result["district"] = addr.get("state_district") or addr.get("district") or result["district"]
                result["state"] = addr.get("state") or result["state"]
                result["country"] = addr.get("country") or result["country"]
    except Exception as exc:
        logger.warning("Reverse geocoding timed out or failed: %s", exc)

    return result


async def register_field(
    farm_id: int,
    name: str,
    coordinates: list[list[float]],
    crop_type: Optional[str] = None,
    sowing_date: Optional[date] = None,
) -> dict[str, Any]:
    """Validate, project, store field and automatically generate 10m analysis grid cells."""
    val = validate_field_polygon(coordinates)
    if not val["valid"]:
        raise ValueError("; ".join(val["errors"]))

    poly_utm = val["polygon_utm"]
    poly_wgs = val["polygon_wgs"]
    centroid_utm = Point(val["centroid_utm"])
    centroid_wgs = Point(val["centroid_wgs"])

    # Reverse geocode centroid
    loc = await reverse_geocode(val["centroid_wgs"][0], val["centroid_wgs"][1])

    async with async_session() as session:
        # Verify farm exists
        farm_res = await session.execute(select(Farm).where(Farm.id == farm_id))
        farm = farm_res.scalar_one_or_none()
        if not farm:
            raise ValueError(f"Farm with id {farm_id} does not exist.")

        # Insert Field using PostGIS geometry
        # Store polygon in UTM Zone 44N (SRID 32644)
        wkt_geom = poly_utm.wkt
        wkt_centroid = centroid_utm.wkt

        q = text("""
            INSERT INTO fields (
                farm_id, name, crop_type, sowing_date,
                geom, centroid, area_sqm, area_ha, perimeter_m,
                utm_epsg, bbox, village, mandal, district, state, country,
                geometry_quality_score, registration_confidence, created_at
            ) VALUES (
                :farm_id, :name, :crop_type, :sowing_date,
                ST_GeomFromText(:wkt_geom, :epsg),
                ST_GeomFromText(:wkt_centroid, :epsg),
                :area_sqm, :area_ha, :perimeter_m,
                :epsg, :bbox, :village, :mandal, :district, :state, :country,
                :geom_score, :confidence, now()
            ) RETURNING id
        """)

        params = {
            "farm_id": farm_id,
            "name": name.strip(),
            "crop_type": crop_type.strip() if crop_type else None,
            "sowing_date": sowing_date,
            "wkt_geom": wkt_geom,
            "wkt_centroid": wkt_centroid,
            "epsg": STORAGE_CRS_EPSG,
            "area_sqm": val["area_sqm"],
            "area_ha": val["area_ha"],
            "perimeter_m": val["perimeter_m"],
            "bbox": json.dumps(val["bbox_wgs"]),
            "village": loc["village"],
            "mandal": loc["mandal"],
            "district": loc["district"],
            "state": loc["state"],
            "country": loc["country"],
            "geom_score": val["quality_score"],
            "confidence": val["registration_confidence"],
        }

        res = await session.execute(q, params)
        new_field_id = res.scalar_one()

        # Generate 10m analysis grid cells aligned with field orientation
        grid_records = generate_rotated_grid(poly_utm, cell_size_m=10.0)

        grid_inserts = []
        for idx, rec in enumerate(grid_records):
            grid_id_str = f"f{new_field_id}_r{rec.row}_c{rec.col}_p{rec.part}"
            grid_inserts.append({
                "field_id": new_field_id,
                "grid_id": grid_id_str,
                "grid_num": idx + 1,
                "row_idx": rec.row,
                "col_idx": rec.col,
                "wkt_geom": rec.geometry.wkt,
                "wkt_centroid": rec.geometry.centroid.wkt,
                "epsg": STORAGE_CRS_EPSG,
                "area_sqm": float(rec.geometry.area),
                "is_water": False,
            })

        if grid_inserts:
            grid_q = text("""
                INSERT INTO field_grid_cells (
                    field_id, grid_id, grid_num, row_idx, col_idx,
                    geom, centroid, area_sqm, is_water
                ) VALUES (
                    :field_id, :grid_id, :grid_num, :row_idx, :col_idx,
                    ST_GeomFromText(:wkt_geom, :epsg),
                    ST_GeomFromText(:wkt_centroid, :epsg),
                    :area_sqm, :is_water
                )
            """)
            await session.execute(grid_q, grid_inserts)

        await session.commit()

        return {
            "field_id": new_field_id,
            "name": name,
            "farm_id": farm_id,
            "crop_type": crop_type,
            "area_sqm": val["area_sqm"],
            "area_ha": val["area_ha"],
            "perimeter_m": val["perimeter_m"],
            "grid_cell_count": len(grid_records),
            "location": loc,
            "quality_score": val["quality_score"],
            "registration_confidence": val["registration_confidence"],
            "centroid": val["centroid_wgs"],
        }
