import asyncio
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import rasterio
from pyproj import Transformer
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from shapely.geometry import shape
from sqlalchemy import text

from backend.db.connection import async_session


PC_STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"


def _scl_is_clear(scl: int) -> bool:
    # Sentinel-2 L2A Scene Classification Layer (SCL) values
    # Keep: 2 (dark features), 4 (vegetation), 5 (bare soil), 7 (unclassified)
    # Mask: 0 (no data), 1 (saturated/defective), 3 (cloud shadow), 6 (water),
    #       8/9/10 (clouds), 11 (snow/ice)
    return scl in (2, 4, 5, 7)


def _pick_best_item(items: Sequence[Any]) -> Optional[Any]:
    if not items:
        return None

    def cloud_cover(it: Any) -> float:
        try:
            v = it.properties.get("eo:cloud_cover")
            return float(v) if v is not None else 1e9
        except Exception:
            return 1e9

    # Lowest cloud cover first
    return sorted(items, key=cloud_cover)[0]


def _item_sort_key(item: Any) -> tuple[float, float]:
    dt_str = item.properties.get("datetime") if hasattr(item, "properties") else None
    if dt_str:
        try:
            item_ts = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00")).timestamp()
        except Exception:
            item_ts = float("-inf")
    else:
        item_ts = float("-inf")

    cloud_cover = item.properties.get("eo:cloud_cover") if hasattr(item, "properties") else None
    try:
        cloud_score = float(cloud_cover) if cloud_cover is not None else 1e9
    except Exception:
        cloud_score = 1e9

    # Newer scenes first, then lower cloud cover for tie-breaking.
    return (item_ts, -cloud_score)


def _compute_indices_for_points(
    item: Any,
    points_lonlat: Sequence[Tuple[float, float]],
) -> List[Dict[str, Any]]:
    """Compute NDVI/NDMI at points by sampling bands.

    - B04, B08 sampled at 10m native resolution
    - B11 (20m) is resampled to 10m grid of B08 (bilinear)
    - SCL (20m) is resampled to 10m grid of B08 (nearest)
    """
    import planetary_computer  # type: ignore

    signed_item = planetary_computer.sign(item)
    b04_href = signed_item.assets["B04"].href
    b08_href = signed_item.assets["B08"].href
    b11_href = signed_item.assets["B11"].href
    scl_href = signed_item.assets["SCL"].href

    results: List[Dict[str, Any]] = []

    with rasterio.Env():
        with rasterio.open(b08_href) as b08_src, rasterio.open(b04_href) as b04_src, rasterio.open(b11_href) as b11_src, rasterio.open(scl_href) as scl_src:
            # Transform points to item CRS
            transformer = Transformer.from_crs("EPSG:4326", b08_src.crs, always_xy=True)
            pts_xy = [transformer.transform(lon, lat) for lon, lat in points_lonlat]

            # Build VRTs for resampling B11 and SCL onto 10m grid
            b11_vrt = WarpedVRT(
                b11_src,
                crs=b08_src.crs,
                transform=b08_src.transform,
                width=b08_src.width,
                height=b08_src.height,
                resampling=Resampling.bilinear,
            )
            scl_vrt = WarpedVRT(
                scl_src,
                crs=b08_src.crs,
                transform=b08_src.transform,
                width=b08_src.width,
                height=b08_src.height,
                resampling=Resampling.nearest,
            )

            # Sample in one pass per dataset
            red_vals = list(b04_src.sample(pts_xy, masked=True))
            nir_vals = list(b08_src.sample(pts_xy, masked=True))
            swir_vals = list(b11_vrt.sample(pts_xy, masked=True))
            scl_vals = list(scl_vrt.sample(pts_xy, masked=True))

            for idx in range(len(points_lonlat)):
                red = float(red_vals[idx][0]) if not red_vals[idx].mask[0] else np.nan
                nir = float(nir_vals[idx][0]) if not nir_vals[idx].mask[0] else np.nan
                swir = float(swir_vals[idx][0]) if not swir_vals[idx].mask[0] else np.nan
                scl = int(scl_vals[idx][0]) if not scl_vals[idx].mask[0] else -1

                is_water = True if scl == 6 else False
                is_clear = _scl_is_clear(scl)

                ndvi = None
                ndmi = None
                pixel_count = 0

                if is_clear and (not is_water) and np.isfinite(red) and np.isfinite(nir) and np.isfinite(swir):
                    denom1 = (nir + red)
                    denom2 = (nir + swir)
                    if denom1 != 0 and denom2 != 0:
                        ndvi = float((nir - red) / denom1)
                        ndmi = float((nir - swir) / denom2)
                        pixel_count = 1

                results.append(
                    {
                        "ndvi": ndvi,
                        "ndmi": ndmi,
                        "pixel_count": pixel_count,
                        "scl": scl,
                        "is_water": is_water,
                    }
                )

            b11_vrt.close()
            scl_vrt.close()

    return results


async def process_sentinel2_for_land_day(land_id, date: str) -> dict:
    """Phase 3 — Sentinel-2 L2A ingestion (authoritative via Planetary Computer STAC).

    For a target UTC date (YYYY-MM-DD), searches up to 10 days backward to find the
    most recent cloud-free image:
    - Query STAC with land geometry
    - Pick the least-cloudy item
    - Sample per-grid centroids (10m grid => 1 pixel per cell)
    - Apply SCL masking (cloud/snow/water)
    - Compute NDVI/NDMI; persist in land_daily_indices
    - Persist is_water flag on grid cells
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    # Fetch land geometry for STAC intersects query
    async with async_session() as session:
        land_res = await session.execute(
            text("SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)) FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        land_row = land_res.first()
        if not land_row or not land_row[0]:
            return {"processed": 0, "reason": "land not found"}

        grids_res = await session.execute(
            text(
                "SELECT grid_id, "
                "ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
                "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lat "
                "FROM land_grid_cells WHERE land_id = :lid ORDER BY grid_id"
            ),
            {"lid": land_id},
        )
        grid_rows = grids_res.fetchall()

    if not grid_rows:
        return {"processed": 0, "reason": "no grids for land"}

    land_geom = shape(__import__("json").loads(land_row[0]))

    # Query STAC — search a 10-day window backward to find available imagery
    from pystac_client import Client  # type: ignore
    from datetime import timedelta

    client = Client.open(PC_STAC_API)
    target = datetime.fromisoformat(date).date()
    start_date = (target - timedelta(days=10)).strftime("%Y-%m-%d")
    end_date = target.strftime("%Y-%m-%d")
    dt = f"{start_date}T00:00:00Z/{end_date}T23:59:59Z"
    search = client.search(collections=["sentinel-2-l2a"], intersects=land_geom.__geo_interface__, datetime=dt, max_items=50)
    items = list(search.items())
    # keep items with required assets
    items = [it for it in items if all(k in it.assets for k in ("B04", "B08", "B11", "SCL"))]

    if not items:
        return {"processed": 0, "reason": f"no Sentinel-2 L2A items found for land in {start_date} to {end_date}"}

    # Pick the newest scene that actually yields at least one usable pixel.
    # Cloud cover alone is not enough: a low-cloud scene can still be cloudy over
    # this specific field, which leaves every grid cell blank.
    grid_ids = [str(r[0]) for r in grid_rows]
    points = [(float(r[1]), float(r[2])) for r in grid_rows]

    selected_item = None
    idx_results: list[dict[str, Any]] | None = None
    for candidate in sorted(items, key=_item_sort_key, reverse=True):
        try:
            candidate_results = await asyncio.to_thread(_compute_indices_for_points, candidate, points)
        except Exception as e:
            logger.warning("Sentinel-2 sampling error for item %s: %s", getattr(candidate, "id", None), e)
            continue

        usable_count = sum(1 for result in candidate_results if result["pixel_count"] > 0)
        if usable_count > 0:
            selected_item = candidate
            idx_results = candidate_results
            break

    if selected_item is None or idx_results is None:
        return {"processed": 0, "reason": f"no usable Sentinel-2 pixels found for land in {start_date} to {end_date}"}

    # Use the actual acquisition date from the selected item, not the requested date.
    item_dt_str = selected_item.properties.get("datetime", "")
    if item_dt_str:
        actual_date = datetime.fromisoformat(item_dt_str.replace("Z", "+00:00")).date()
    else:
        actual_date = target

    date_obj = actual_date
    upsert_sql = text(
        "INSERT INTO land_daily_indices (land_id, grid_id, date, ndvi, ndmi, pixel_count) "
        "VALUES (:land_id, :grid_id, :date, :ndvi, :ndmi, :pixel_count) "
        "ON CONFLICT (grid_id, date) DO UPDATE SET ndvi = EXCLUDED.ndvi, ndmi = EXCLUDED.ndmi, pixel_count = EXCLUDED.pixel_count"
    )

    processed = 0
    async with async_session() as session:
        # batch writes
        params = []
        water_updates = []
        for gid_str, (gid_raw, _lon, _lat), vals in zip(grid_ids, grid_rows, idx_results):
            params.append(
                {
                    "land_id": land_id,
                    "grid_id": gid_str,
                    "date": date_obj,
                    "ndvi": vals["ndvi"],
                    "ndmi": vals["ndmi"],
                    "pixel_count": vals["pixel_count"],
                }
            )
            # update water mask only when SCL was valid
            if vals.get("scl", -1) >= 0:
                water_updates.append({"grid_id": gid_raw, "is_water": bool(vals.get("is_water"))})
            processed += 1

        if params:
            await session.execute(upsert_sql, params)

        if water_updates:
            await session.execute(
                text("UPDATE land_grid_cells SET is_water = :is_water WHERE grid_id = :grid_id"),
                water_updates,
            )

        await session.commit()

    return {
        "processed": processed,
        "stac_item_id": getattr(selected_item, "id", None),
        "datetime": selected_item.properties.get("datetime") if hasattr(selected_item, "properties") else None,
        "cloud_cover": selected_item.properties.get("eo:cloud_cover") if hasattr(selected_item, "properties") else None,
    }
