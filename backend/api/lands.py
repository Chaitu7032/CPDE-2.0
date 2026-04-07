from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import unary_union
from shapely.validation import explain_validity
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from backend.db.connection import async_session
from backend.utils.crs import STORAGE_CRS_EPSG, geometry_geojson_storage_to_api, shapely_wgs84_to_storage
router = APIRouter(prefix="/lands", tags=["lands"])


class LandCreate(BaseModel):
    # DB schema uses farmer_name; frontend label is "Farmer name".
    # Accept both to avoid client/server mismatch.
    owner: str | None = None
    farmer_name: str | None = None
    crop_type: str | None = None
    # GeoJSON geometry object (Polygon) in WGS84 lon/lat
    geometry: dict


class LandCreateResponse(BaseModel):
    land_id: str
    utm_epsg: int | None = None
    area_sqm: float | None = None


def _validate_polygon_coords(coords: list) -> None:
    if not coords or not isinstance(coords, list):
        raise HTTPException(status_code=400, detail="Invalid Polygon coordinates")
    ring = coords[0] if coords else None
    if not isinstance(ring, list) or len(ring) < 4:
        raise HTTPException(status_code=400, detail="Polygon ring must have at least 4 points")
    first = ring[0]
    last = ring[-1]
    if not (isinstance(first, list) and isinstance(last, list) and len(first) >= 2 and len(last) >= 2):
        raise HTTPException(status_code=400, detail="Invalid Polygon coordinate points")
    if first[0] != last[0] or first[1] != last[1]:
        raise HTTPException(status_code=400, detail="Polygon must be closed (first point must equal last point)")
    for pt in ring:
        if not (isinstance(pt, list) and len(pt) >= 2):
            raise HTTPException(status_code=400, detail="Invalid Polygon coordinate point")
        lon, lat = pt[0], pt[1]
        if not (isinstance(lon, (int, float)) and isinstance(lat, (int, float))):
            raise HTTPException(status_code=400, detail="Polygon coordinates must be numeric lon/lat")
        if lon < -180 or lon > 180 or lat < -90 or lat > 90:
            raise HTTPException(status_code=400, detail="Polygon coordinates out of lon/lat range")


@router.post("/", status_code=201, response_model=LandCreateResponse)
async def register_land(payload: LandCreate):
    farmer_name = (payload.farmer_name or payload.owner or "").strip()
    if not farmer_name:
        raise HTTPException(status_code=400, detail="farmer_name is required")

    # Validate raw GeoJSON structure early (closure + coordinate ranges)
    g = payload.geometry
    if not isinstance(g, dict) or "type" not in g:
        raise HTTPException(status_code=400, detail="geometry must be a GeoJSON object")
    gtype = g.get("type")
    if gtype == "Polygon":
        _validate_polygon_coords(g.get("coordinates"))
    elif gtype == "MultiPolygon":
        polys = g.get("coordinates")
        if not isinstance(polys, list) or not polys:
            raise HTTPException(status_code=400, detail="Invalid MultiPolygon coordinates")
        for poly_coords in polys:
            _validate_polygon_coords(poly_coords)
    else:
        raise HTTPException(status_code=400, detail="Geometry must be a Polygon or MultiPolygon")

    # Validate GeoJSON
    try:
        geom = shape(payload.geometry)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid geometry: {e}")

    if isinstance(geom, MultiPolygon):
        # dissolve into a single polygon for storage/processing
        geom = unary_union(list(geom.geoms))

    if not isinstance(geom, Polygon):
        raise HTTPException(status_code=400, detail="Geometry must be a Polygon or MultiPolygon")

    if not geom.is_valid:
        reason = explain_validity(geom)
        raise HTTPException(status_code=400, detail=f"Invalid polygon: {reason}")

    try:
        geom_utm = shapely_wgs84_to_storage(geom)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CRS transform failed: {e}")

    if geom_utm.is_empty:
        raise HTTPException(status_code=400, detail="Geometry became empty after CRS transformation")

    area_sqm = float(abs(geom_utm.area))
    rp_utm = geom_utm.representative_point()
    utm_epsg = STORAGE_CRS_EPSG

    geojson_str = json.dumps(mapping(geom_utm))

    async with async_session() as session:
        try:
            # Use PostGIS functions to convert GeoJSON safely and set SRID.
            # Try to persist optional columns if they exist; fallback if schema is older.
            try:
                res = await session.execute(
                    text(
                        "INSERT INTO lands (farmer_name, crop_type, geom, centroid, utm_epsg, area_sqm, created_at) "
                        "VALUES (:farmer_name, :crop_type, "
                        "  ST_SetSRID(ST_GeomFromGeoJSON(:geojson), :srid), "
                        "  ST_SetSRID(ST_Point(:x, :y), :srid), "
                        "  :utm_epsg, :area_sqm, :created_at "
                        ") RETURNING land_id"
                    ),
                    {
                        "farmer_name": farmer_name,
                        "crop_type": payload.crop_type,
                        "geojson": geojson_str,
                        "x": float(rp_utm.x),
                        "y": float(rp_utm.y),
                        "srid": int(STORAGE_CRS_EPSG),
                        "utm_epsg": int(utm_epsg),
                        "area_sqm": area_sqm,
                        "created_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    },
                )
            except SQLAlchemyError:
                # Older schema: rollback tainted transaction and retry with fewer columns
                await session.rollback()
                res = await session.execute(
                    text(
                        "INSERT INTO lands (farmer_name, crop_type, geom, centroid, utm_epsg) "
                        "VALUES (:farmer_name, :crop_type, "
                        "  ST_SetSRID(ST_GeomFromGeoJSON(:geojson), :srid), "
                        "  ST_SetSRID(ST_Point(:x, :y), :srid), "
                        "  :utm_epsg "
                        ") RETURNING land_id"
                    ),
                    {
                        "farmer_name": farmer_name,
                        "crop_type": payload.crop_type,
                        "geojson": geojson_str,
                        "x": float(rp_utm.x),
                        "y": float(rp_utm.y),
                        "srid": int(STORAGE_CRS_EPSG),
                        "utm_epsg": int(utm_epsg),
                    },
                )

            land_id = res.scalar_one()
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(e))

    return {"land_id": str(land_id), "utm_epsg": utm_epsg, "area_sqm": area_sqm}


@router.get("/{land_id}")
async def get_land(land_id: str):
    lid = int(land_id)
    async with async_session() as session:
        res = await session.execute(
            text(
                "SELECT land_id, farmer_name, crop_type, ST_AsGeoJSON(geom) as geojson, area_sqm "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": lid},
        )
        row = res.first()
        if not row:
            raise HTTPException(status_code=404, detail="land not found")

    geometry_storage = json.loads(row[3]) if row[3] else None
    geometry_wgs84 = geometry_geojson_storage_to_api(geometry_storage) if geometry_storage else None

    return {
        "land_id": str(row[0]),
        "farmer_name": row[1],
        "crop_type": row[2],
        "geometry": geometry_wgs84,
        "area_sqm": row[4],
    }
