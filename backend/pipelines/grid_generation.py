from typing import List
import json
from shapely.geometry import Polygon, shape
from shapely.ops import transform
from pyproj import CRS, Transformer
from math import floor
import uuid

from sqlalchemy import text

from backend.db.connection import async_session
from backend.db.models import Land, LandGrid

def lonlat_to_utm_crs(lon: float, lat: float) -> CRS:
    """Return appropriate UTM CRS for given lon/lat point."""
    zone = int((lon + 180) / 6) + 1
    if lat >= 0:
        epsg = 32600 + zone
    else:
        epsg = 32700 + zone
    return CRS.from_epsg(epsg)


def reproject_geom_to_crs(geom: Polygon, target_crs: CRS) -> Polygon:
    """Reproject shapely geometry to target CRS."""
    src_crs = CRS.from_epsg(4326)
    transformer = Transformer.from_crs(src_crs, target_crs, always_xy=True)
    return transform(lambda x, y: transformer.transform(x, y), geom)


def reproject_geom_to_wgs84(geom: Polygon, source_crs: CRS) -> Polygon:
    transformer = Transformer.from_crs(source_crs, CRS.from_epsg(4326), always_xy=True)
    return transform(lambda x, y: transformer.transform(x, y), geom)


def generate_fixed_grid(poly_proj: Polygon, cell_size_m: float = 10.0) -> List[Polygon]:
    """Generate deterministic grid (list of square Polygons) that cover the projected polygon bounds.

    poly_proj: projected polygon (units in meters)
    """
    minx, miny, maxx, maxy = poly_proj.bounds
    # Align grid to origin at floor(min / cell) * cell to ensure deterministic tiling
    start_x = floor(minx / cell_size_m) * cell_size_m
    start_y = floor(miny / cell_size_m) * cell_size_m

    cols = int((maxx - start_x) // cell_size_m) + 1
    rows = int((maxy - start_y) // cell_size_m) + 1

    cells: List[Polygon] = []
    for i in range(cols):
        for j in range(rows):
            x0 = start_x + i * cell_size_m
            y0 = start_y + j * cell_size_m
            cell = Polygon([(x0, y0), (x0 + cell_size_m, y0), (x0 + cell_size_m, y0 + cell_size_m), (x0, y0 + cell_size_m), (x0, y0)])
            # only keep if intersects the polygon
            if cell.intersects(poly_proj):
                inter = cell.intersection(poly_proj)
                if not inter.is_empty:
                    cells.append(inter)

    return cells


async def generate_and_store_grids(land_id, cell_size_m: float = 10.0) -> List[str]:
    """Fetch land by `land_id`, generate 10m grids, store clipped cells in `land_grid_cells` table.

    Returns list of grid_id inserted.
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    async with async_session() as session:
        # fetch land geometry (WGS84) + cached UTM EPSG if available
        res = await session.execute(
            text("SELECT land_id, ST_AsGeoJSON(geom) AS geojson, utm_epsg FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        row = res.first()
        if not row:
            raise ValueError(f"land_id {land_id} not found")

        land_geojson = row[1]
        cached_utm_epsg = row[2]
        land_shape = shape(json.loads(land_geojson))

        # Determine UTM CRS and reproject (use cached EPSG if present)
        lon, lat = land_shape.representative_point().x, land_shape.representative_point().y
        utm_crs = CRS.from_epsg(int(cached_utm_epsg)) if cached_utm_epsg else lonlat_to_utm_crs(lon, lat)
        if not cached_utm_epsg:
            # cache centroid + utm epsg on lands to make later phases consistent
            await session.execute(
                text(
                    "UPDATE lands SET utm_epsg = :epsg, centroid = ST_SetSRID(ST_Point(:lon,:lat),4326) WHERE land_id = :lid"
                ),
                {"epsg": int(utm_crs.to_epsg()), "lon": lon, "lat": lat, "lid": land_id},
            )
        land_proj = reproject_geom_to_crs(land_shape, utm_crs)

        # Generate grid cells in projected CRS
        cells_proj = generate_fixed_grid(land_proj, cell_size_m=cell_size_m)

        grid_ids = []
        for idx, cell_proj in enumerate(cells_proj):
            # reproject clipped cell back to WGS84
            cell_wgs = reproject_geom_to_wgs84(cell_proj, utm_crs)
            # deterministic grid id: uuid5 using land_id + UTM centroid (meter grid)
            centroid_proj = cell_proj.representative_point()
            name = f"{land_id}-{int(utm_crs.to_epsg())}-{round(centroid_proj.x,3)}-{round(centroid_proj.y,3)}"
            grid_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, name))

            centroid_wgs = cell_wgs.representative_point()

            # insert into DB
            await session.execute(
                text(
                    "INSERT INTO land_grid_cells (grid_id, land_id, geom, centroid) VALUES (:grid_id, :land_id, ST_SetSRID(ST_GeomFromText(:wkt),4326), ST_SetSRID(ST_Point(:lon,:lat),4326)) ON CONFLICT (grid_id) DO NOTHING"
                ),
                {"grid_id": grid_uuid, "land_id": land_id, "wkt": cell_wgs.wkt, "lon": centroid_wgs.x, "lat": centroid_wgs.y},
            )
            grid_ids.append(grid_uuid)

        await session.commit()

    return grid_ids
