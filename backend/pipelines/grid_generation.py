from typing import List
import json
from shapely.geometry import Polygon, shape
from shapely.affinity import rotate
from math import floor, atan2, degrees
import uuid

from sqlalchemy import text

from backend.db.connection import async_session
from backend.utils.crs import STORAGE_CRS_EPSG


def _extract_polygons(geom) -> List[Polygon]:
    """Return only polygonal parts from any Shapely geometry."""
    if geom.is_empty:
        return []
    if geom.geom_type == "Polygon":
        return [geom]
    if geom.geom_type == "MultiPolygon":
        return list(geom.geoms)
    if geom.geom_type == "GeometryCollection":
        out: List[Polygon] = []
        for part in geom.geoms:
            out.extend(_extract_polygons(part))
        return out
    return []


def generate_rotated_grid(
    polygon: Polygon,
    cell_size_m: float = 10.0,
) -> List[Polygon]:
    """Generate a 10m grid aligned with field orientation in UTM CRS.

    Steps:
    1) Compute field orientation from minimum rotated rectangle first edge.
    2) Rotate polygon by -angle around original centroid.
    3) Build axis-aligned square grid at fixed resolution.
    4) Clip each cell to rotated polygon.
    5) Rotate clipped cells back by +angle around original centroid.
    """
    if polygon.is_empty:
        return []

    polygon_utm = polygon

    # 1) Orientation from minimum rotated rectangle, using first non-zero edge.
    mrr = polygon_utm.minimum_rotated_rectangle
    coords = list(mrr.exterior.coords)
    if len(coords) < 2:
        return []

    x1, y1 = coords[0]
    x2, y2 = coords[1]
    dx, dy = (x2 - x1), (y2 - y1)
    if dx == 0.0 and dy == 0.0:
        for i in range(1, len(coords) - 1):
            ax, ay = coords[i]
            bx, by = coords[i + 1]
            dx, dy = (bx - ax), (by - ay)
            if dx != 0.0 or dy != 0.0:
                break
    angle_deg = degrees(atan2(dy, dx))

    original_centroid = polygon_utm.centroid

    # 2) Align the field with axes so grid generation is exact and simple.
    aligned_polygon = rotate(
        polygon_utm,
        -angle_deg,
        origin=(original_centroid.x, original_centroid.y),
        use_radians=False,
    )

    # 3) Generate axis-aligned 10m grid in aligned space.
    minx, miny, maxx, maxy = aligned_polygon.bounds
    start_x = floor(minx / cell_size_m) * cell_size_m
    start_y = floor(miny / cell_size_m) * cell_size_m
    cols = int((maxx - start_x) // cell_size_m) + 1
    rows = int((maxy - start_y) // cell_size_m) + 1

    cells_utm: List[Polygon] = []
    for i in range(cols):
        for j in range(rows):
            x0 = start_x + i * cell_size_m
            y0 = start_y + j * cell_size_m
            axis_cell = Polygon(
                [
                    (x0, y0),
                    (x0 + cell_size_m, y0),
                    (x0 + cell_size_m, y0 + cell_size_m),
                    (x0, y0 + cell_size_m),
                    (x0, y0),
                ]
            )
            if not axis_cell.intersects(aligned_polygon):
                continue

            # 4) Clip each candidate cell to polygon in aligned space.
            clipped = axis_cell.intersection(aligned_polygon)
            for clipped_poly in _extract_polygons(clipped):
                if clipped_poly.is_empty or clipped_poly.area <= 0.0:
                    continue

                # 5) Rotate back to original orientation around original centroid.
                restored = rotate(
                    clipped_poly,
                    angle_deg,
                    origin=(original_centroid.x, original_centroid.y),
                    use_radians=False,
                )

                # Enforce strict in-field cells for downstream spatial analysis.
                if not restored.covered_by(polygon_utm):
                    restored = restored.intersection(polygon_utm)

                for final_poly in _extract_polygons(restored):
                    if final_poly.is_empty or final_poly.area <= 0.0:
                        continue
                    cells_utm.append(final_poly)

    return cells_utm


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
        # fetch land geometry in canonical UTM storage CRS
        res = await session.execute(
            text("SELECT land_id, ST_AsGeoJSON(geom) AS geojson FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        row = res.first()
        if not row:
            raise ValueError(f"land_id {land_id} not found")

        land_geojson = row[1]
        land_shape = shape(json.loads(land_geojson))

        # Generate field-oriented cells directly in UTM CRS.
        cells_proj = generate_rotated_grid(land_shape, cell_size_m=cell_size_m)

        grid_ids = []
        for cell_proj in cells_proj:
            # deterministic grid id: uuid5 using land_id + UTM centroid (meter grid)
            centroid_proj = cell_proj.representative_point()
            name = f"{land_id}-{STORAGE_CRS_EPSG}-{round(centroid_proj.x,3)}-{round(centroid_proj.y,3)}"
            grid_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, name))

            # insert into DB
            await session.execute(
                text(
                    "INSERT INTO land_grid_cells (grid_id, land_id, geom, centroid) VALUES (:grid_id, :land_id, ST_SetSRID(ST_GeomFromText(:wkt),:srid), ST_SetSRID(ST_Point(:x,:y),:srid)) ON CONFLICT (grid_id) DO NOTHING"
                ),
                {
                    "grid_id": grid_uuid,
                    "land_id": land_id,
                    "wkt": cell_proj.wkt,
                    "x": float(centroid_proj.x),
                    "y": float(centroid_proj.y),
                    "srid": int(STORAGE_CRS_EPSG),
                },
            )
            grid_ids.append(grid_uuid)

        await session.commit()

    return grid_ids
