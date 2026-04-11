import asyncio
import logging
from datetime import datetime, timedelta
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
logger = logging.getLogger(__name__)

# Global timeout for STAC searches (seconds). Prevents silent hangs.
STAC_SEARCH_TIMEOUT_S = 60


def _rasterio_env_kwargs() -> dict[str, str]:
    return {"GTIFF_SRS_SOURCE": "EPSG"}


def _extract_tile_id(item: Any) -> str | None:
    props = item.properties if hasattr(item, "properties") and isinstance(item.properties, dict) else {}
    for key in ("s2:mgrs_tile", "mgrs:tile", "tile_id", "tile", "s2:tile_id"):
        value = props.get(key)
        if value:
            return str(value)
    return None


def _extract_item_datetime(item: Any) -> datetime | None:
    props = item.properties if hasattr(item, "properties") and isinstance(item.properties, dict) else {}
    value = props.get("datetime")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_cloud_cover(item: Any) -> float | None:
    props = item.properties if hasattr(item, "properties") and isinstance(item.properties, dict) else {}
    value = props.get("eo:cloud_cover")
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


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

    logger.info(
        "Sentinel-2 raster sampling start item=%s points=%d b04=%s b08=%s b11=%s scl=%s",
        getattr(item, "id", None),
        len(points_lonlat),
        b04_href,
        b08_href,
        b11_href,
        scl_href,
    )

    with rasterio.Env(**_rasterio_env_kwargs()):
        with (
            rasterio.open(b08_href) as b08_src,
            rasterio.open(b04_href) as b04_src,
            rasterio.open(b11_href) as b11_src,
            rasterio.open(scl_href) as scl_src,
        ):
            target_crs = b08_src.crs
            if target_crs is None:
                props = item.properties if hasattr(item, "properties") and isinstance(item.properties, dict) else {}
                proj_code = props.get("proj:code") or props.get("proj:epsg")
                if isinstance(proj_code, str) and proj_code:
                    target_crs = rasterio.crs.CRS.from_string(proj_code)
                elif proj_code is not None:
                    target_crs = rasterio.crs.CRS.from_epsg(int(proj_code))
                if target_crs is not None:
                    logger.info(
                        "Sentinel-2 using STAC projection fallback item=%s proj_code=%s",
                        getattr(item, "id", None),
                        proj_code,
                    )

            logger.info(
                "Sentinel-2 raster metadata item=%s crs=%s target_crs=%s bounds=%s shape=%sx%s",
                getattr(item, "id", None),
                b08_src.crs,
                target_crs,
                b08_src.bounds,
                b08_src.width,
                b08_src.height,
            )

            if target_crs is None:
                raise RuntimeError(
                    f"Sentinel-2 item {getattr(item, 'id', None)} has no usable CRS metadata"
                )

            try:
                transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
                pts_xy = [transformer.transform(lon, lat) for lon, lat in points_lonlat]

                with (
                    WarpedVRT(
                        b11_src,
                        crs=target_crs,
                        transform=b08_src.transform,
                        width=b08_src.width,
                        height=b08_src.height,
                        resampling=Resampling.bilinear,
                    ) as b11_vrt,
                    WarpedVRT(
                        scl_src,
                        crs=target_crs,
                        transform=b08_src.transform,
                        width=b08_src.width,
                        height=b08_src.height,
                        resampling=Resampling.nearest,
                    ) as scl_vrt,
                ):
                    red_vals = list(b04_src.sample(pts_xy, masked=True))
                    nir_vals = list(b08_src.sample(pts_xy, masked=True))
                    swir_vals = list(b11_vrt.sample(pts_xy, masked=True))
                    scl_vals = list(scl_vrt.sample(pts_xy, masked=True))

                    for idx in range(len(points_lonlat)):
                        red = float(red_vals[idx][0]) if not red_vals[idx].mask[0] else np.nan
                        nir = float(nir_vals[idx][0]) if not nir_vals[idx].mask[0] else np.nan
                        swir = float(swir_vals[idx][0]) if not swir_vals[idx].mask[0] else np.nan
                        scl = int(scl_vals[idx][0]) if not scl_vals[idx].mask[0] else -1

                        is_water = scl == 6
                        is_clear = _scl_is_clear(scl)

                        ndvi = None
                        ndmi = None
                        b04 = None
                        b08 = None
                        b11 = None
                        pixel_count = 0

                        if (
                            is_clear
                            and not is_water
                            and np.isfinite(red)
                            and np.isfinite(nir)
                            and np.isfinite(swir)
                        ):
                            denom1 = nir + red
                            denom2 = nir + swir
                            if denom1 != 0 and denom2 != 0:
                                ndvi = float((nir - red) / denom1)
                                ndmi = float((nir - swir) / denom2)
                                pixel_count = 1
                                b04 = float(red)
                                b08 = float(nir)
                                b11 = float(swir)

                        results.append(
                            {
                                "b04": b04,
                                "b08": b08,
                                "b11": b11,
                                "ndvi": ndvi,
                                "ndmi": ndmi,
                                "pixel_count": pixel_count,
                                "scl": scl,
                                "is_water": is_water,
                            }
                        )
            except Exception as exc:
                logger.exception(
                    "Sentinel-2 reprojection/sampling failed for item=%s",
                    getattr(item, "id", None),
                )
                raise RuntimeError(
                    f"Sentinel-2 reprojection failed for item {getattr(item, 'id', None)}: {exc}"
                ) from exc

    return results


async def _stac_search_sentinel2(
    land_geom: Any,
    start_date: str,
    end_date: str,
    land_id: Any,
) -> list[Any]:
    """Run Sentinel-2 STAC search with a hard timeout to prevent hangs."""
    from pystac_client import Client  # type: ignore

    dt = f"{start_date}T00:00:00Z/{end_date}T23:59:59Z"
    logger.info(
        "Sentinel-2 STAC search land=%s collection=sentinel-2-l2a datetime=%s",
        land_id,
        dt,
    )

    def _search() -> list[Any]:
        client = Client.open(PC_STAC_API)
        search = client.search(
            collections=["sentinel-2-l2a"],
            intersects=land_geom.__geo_interface__,
            datetime=dt,
            max_items=50,
        )
        return list(search.items())

    try:
        items = await asyncio.wait_for(
            asyncio.to_thread(_search),
            timeout=STAC_SEARCH_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Sentinel-2 STAC search timed out after %ss for land=%s",
            STAC_SEARCH_TIMEOUT_S,
            land_id,
        )
        raise RuntimeError(
            f"Sentinel-2 STAC search timed out after {STAC_SEARCH_TIMEOUT_S}s"
        )

    items = [it for it in items if all(k in it.assets for k in ("B04", "B08", "B11", "SCL"))]
    logger.info(
        "Sentinel-2 STAC search returned %d candidate items for land=%s",
        len(items),
        land_id,
    )
    if items:
        logger.info(
            "Sentinel-2 candidate item ids=%s",
            [getattr(it, "id", None) for it in items[:10]],
        )
    return items


async def process_sentinel2_for_land_day(
    land_id,
    date: str,
    allow_fallback: bool = True,
    cloud_threshold_pct: float | None = None,
    preloaded_items: Sequence[Any] | None = None,
    preferred_item: Any | None = None,
) -> dict:
    """Phase 3 — Sentinel-2 L2A ingestion (authoritative via Planetary Computer STAC).

    For a target UTC date (YYYY-MM-DD), searches up to 10 days backward to find the
    most recent cloud-free image:
    - Query STAC with land geometry (with hard timeout)
    - Pick the least-cloudy item
    - Sample per-grid centroids (10m grid => 1 pixel per cell)
    - Apply SCL masking (cloud/snow/water)
    - Compute NDVI/NDMI; persist in land_daily_indices
    - Persist is_water flag on grid cells
    """
    land_id = int(land_id)
    logger.info(
        "Sentinel-2 processing start land=%s requested_date=%s allow_fallback=%s cloud_threshold_pct=%s",
        land_id,
        date,
        allow_fallback,
        cloud_threshold_pct,
    )

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
    target = datetime.fromisoformat(date).date()

    # FIX: define these in outer scope so they're always accessible
    start_date: str = target.strftime("%Y-%m-%d")
    end_date: str = target.strftime("%Y-%m-%d")

    if preferred_item is not None:
        items = [preferred_item]
        logger.info(
            "Sentinel-2 reusing preselected STAC item land=%s item=%s",
            land_id,
            getattr(preferred_item, "id", None),
        )
    elif preloaded_items is not None:
        items = [
            it
            for it in preloaded_items
            if hasattr(it, "assets") and all(k in it.assets for k in ("B04", "B08", "B11", "SCL"))
        ]
        logger.info(
            "Sentinel-2 reusing %d preloaded STAC items for land=%s",
            len(items),
            land_id,
        )
    else:
        start_date = (
            (target - timedelta(days=10)).strftime("%Y-%m-%d")
            if allow_fallback
            else target.strftime("%Y-%m-%d")
        )
        end_date = target.strftime("%Y-%m-%d")
        try:
            items = await _stac_search_sentinel2(land_geom, start_date, end_date, land_id)
        except RuntimeError as exc:
            return {"processed": 0, "reason": str(exc)}

    if not items:
        return {
            "processed": 0,
            "reason": f"no Sentinel-2 L2A items found for land in {start_date} to {end_date}",
        }

    grid_ids = [str(r[0]) for r in grid_rows]
    points = [(float(r[1]), float(r[2])) for r in grid_rows]

    selected_item = None
    idx_results: list[dict[str, Any]] | None = None
    last_error: str | None = None

    for candidate in sorted(items, key=_item_sort_key, reverse=True):
        try:
            cloud_cover = _extract_cloud_cover(candidate)
            if cloud_threshold_pct is not None and (
                cloud_cover is None or cloud_cover > cloud_threshold_pct
            ):
                continue
            logger.info(
                "Sentinel-2 evaluating item=%s datetime=%s cloud_cover=%s",
                getattr(candidate, "id", None),
                _extract_item_datetime(candidate),
                cloud_cover,
            )
            candidate_results = await asyncio.wait_for(
                asyncio.to_thread(_compute_indices_for_points, candidate, points),
                timeout=120,  # 2-min hard cap per item
            )
        except asyncio.TimeoutError:
            last_error = "raster sampling timed out"
            logger.error(
                "Sentinel-2 raster sampling timed out for item=%s",
                getattr(candidate, "id", None),
            )
            continue
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            logger.exception(
                "Sentinel-2 sampling error for item %s",
                getattr(candidate, "id", None),
            )
            continue

        usable_count = sum(1 for result in candidate_results if result["pixel_count"] > 0)
        logger.info(
            "Sentinel-2 candidate usable_count=%d total_points=%d item=%s",
            usable_count,
            len(candidate_results),
            getattr(candidate, "id", None),
        )
        if usable_count > 0:
            selected_item = candidate
            idx_results = candidate_results
            break

    if selected_item is None or idx_results is None:
        reason = (
            f"Sentinel-2 reprojection/sampling failed for land in {start_date} to {end_date}: {last_error}"
            if last_error
            else f"no usable Sentinel-2 pixels found for land in {start_date} to {end_date}"
        )
        return {"processed": 0, "reason": reason}

    item_dt = _extract_item_datetime(selected_item)
    actual_date = item_dt.date() if item_dt else target

    stac_item_id = getattr(selected_item, "id", None)
    tile_id = _extract_tile_id(selected_item)
    cloud_cover = _extract_cloud_cover(selected_item)
    acquisition_datetime = item_dt.replace(tzinfo=None) if item_dt else None
    date_obj = actual_date

    upsert_sql = text(
        "INSERT INTO land_daily_indices "
        "(land_id, grid_id, date, stac_item_id, acquisition_datetime, tile_id, "
        " cloud_cover_pct, b04, b08, b11, ndvi, ndmi, pixel_count) "
        "VALUES (:land_id, :grid_id, :date, :stac_item_id, :acquisition_datetime, "
        "        :tile_id, :cloud_cover_pct, :b04, :b08, :b11, :ndvi, :ndmi, :pixel_count) "
        "ON CONFLICT (grid_id, date) DO UPDATE SET "
        "  stac_item_id        = EXCLUDED.stac_item_id, "
        "  acquisition_datetime = EXCLUDED.acquisition_datetime, "
        "  tile_id             = EXCLUDED.tile_id, "
        "  cloud_cover_pct     = EXCLUDED.cloud_cover_pct, "
        "  b04                 = EXCLUDED.b04, "
        "  b08                 = EXCLUDED.b08, "
        "  b11                 = EXCLUDED.b11, "
        "  ndvi                = EXCLUDED.ndvi, "
        "  ndmi                = EXCLUDED.ndmi, "
        "  pixel_count         = EXCLUDED.pixel_count"
    )

    processed = 0
    async with async_session() as session:
        params = []
        water_updates = []
        for gid_str, (gid_raw, _lon, _lat), vals in zip(grid_ids, grid_rows, idx_results):
            params.append(
                {
                    "land_id": land_id,
                    "grid_id": gid_str,
                    "date": date_obj,
                    "stac_item_id": stac_item_id,
                    "acquisition_datetime": acquisition_datetime,
                    "tile_id": tile_id,
                    "cloud_cover_pct": cloud_cover,
                    "b04": vals["b04"],
                    "b08": vals["b08"],
                    "b11": vals["b11"],
                    "ndvi": vals["ndvi"],
                    "ndmi": vals["ndmi"],
                    "pixel_count": vals["pixel_count"],
                }
            )
            if vals.get("scl", -1) >= 0:
                water_updates.append(
                    {"grid_id": gid_raw, "is_water": bool(vals.get("is_water"))}
                )
            processed += 1

        if params:
            await session.execute(upsert_sql, params)
        if water_updates:
            await session.execute(
                text(
                    "UPDATE land_grid_cells SET is_water = :is_water WHERE grid_id = :grid_id"
                ),
                water_updates,
            )
        await session.commit()

    logger.info(
        "Sentinel-2 processing complete land=%s date=%s processed=%d stac_item=%s",
        land_id,
        date_obj,
        processed,
        stac_item_id,
    )

    return {
        "processed": processed,
        "stac_item_id": stac_item_id,
        "datetime": item_dt.isoformat() if item_dt else None,
        "cloud_cover": cloud_cover,
        "tile_id": tile_id,
    }