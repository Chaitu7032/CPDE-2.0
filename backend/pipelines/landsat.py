import os
import sys

# Ensure rasterio proj_data path is loaded on Windows before importing rasterio/pyproj
try:
    import rasterio._env
    _proj_paths = rasterio._env.get_proj_data_search_paths()
    if _proj_paths:
        os.environ["PROJ_LIB"] = _proj_paths[0]
        os.environ["PROJ_DATA"] = _proj_paths[0]
except Exception:
    pass

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import rasterio
from pyproj import Transformer
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from shapely.geometry import shape
from sqlalchemy import text

from backend.db.connection import async_session
from backend.pipelines.sentinel2 import _safe_sample_val

PC_STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"
logger = logging.getLogger(__name__)

STAC_SEARCH_TIMEOUT_S = 60
LANDSAT_COLLECTION = "landsat-c2-l2"

# USGS Collection 2 Level 2 Surface Temperature constants
# Formula: LST (Kelvin) = DN * 0.00341802 + 149.0
ST_SCALE_FACTOR = 0.00341802
ST_OFFSET = 149.0
KELVIN_TO_CELSIUS = 273.15


def _rasterio_env_kwargs() -> dict[str, str]:
    proj_dir = os.path.abspath(os.path.join(os.path.dirname(rasterio.__file__), "proj_data"))
    return {
        "GTIFF_SRS_SOURCE": "EPSG",
        "PROJ_LIB": proj_dir,
        "PROJ_DATA": proj_dir,
    }


async def _stac_search_landsat(
    land_geom: Any,
    start_date: str,
    end_date: str,
    land_id: Any,
    max_cloud_cover_pct: float = 60.0,
) -> list[Any]:
    """Search Planetary Computer STAC for Landsat 8/9 C2 L2 items."""
    from pystac_client import Client  # type: ignore

    dt = f"{start_date}T00:00:00Z/{end_date}T23:59:59Z"
    logger.info(
        "Landsat STAC search land=%s collection=%s datetime=%s",
        land_id,
        LANDSAT_COLLECTION,
        dt,
    )

    def _search() -> list[Any]:
        client = Client.open(PC_STAC_API)
        search = client.search(
            collections=[LANDSAT_COLLECTION],
            intersects=land_geom.__geo_interface__,
            datetime=dt,
            query={"eo:cloud_cover": {"lt": max_cloud_cover_pct}},
            max_items=30,
        )
        return list(search.items())

    try:
        items = await asyncio.wait_for(
            asyncio.to_thread(_search),
            timeout=STAC_SEARCH_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        logger.error(
            "Landsat STAC search timed out after %ss for land=%s",
            STAC_SEARCH_TIMEOUT_S,
            land_id,
        )
        return []
    except Exception as exc:
        logger.error("Landsat STAC search error for land=%s: %s", land_id, exc)
        return []

    # Keep items that have lwir11 or ST_B10 asset
    valid_items = [
        it for it in items
        if hasattr(it, "assets") and ("lwir11" in it.assets or "st_b10" in it.assets or "thermal" in it.assets)
    ]
    logger.info("Landsat STAC search returned %d valid thermal items for land=%s", len(valid_items), land_id)
    return valid_items


def _sample_landsat_lst(
    item: Any,
    points_lonlat: Sequence[Tuple[float, float]],
) -> List[Dict[str, Any]]:
    """Sample Landsat Collection 2 Level-2 Surface Temperature asset at points."""
    import planetary_computer  # type: ignore

    signed_item = planetary_computer.sign(item)
    st_asset = signed_item.assets.get("lwir11") or signed_item.assets.get("st_b10") or signed_item.assets.get("thermal")
    qa_asset = signed_item.assets.get("qa_pixel")

    if not st_asset:
        return []

    st_href = st_asset.href
    qa_href = qa_asset.href if qa_asset else None

    results: List[Dict[str, Any]] = []

    with rasterio.Env(**_rasterio_env_kwargs()):
        with rasterio.open(st_href) as st_src:
            target_crs = st_src.crs
            if target_crs is None:
                props = item.properties if hasattr(item, "properties") and isinstance(item.properties, dict) else {}
                proj_code = props.get("proj:code") or props.get("proj:epsg")
                if proj_code:
                    target_crs = f"EPSG:{proj_code}" if isinstance(proj_code, int) else proj_code
                else:
                    target_crs = "EPSG:32644"  # Andhra Pradesh UTM Zone 44N

            transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
            pts_xy = [transformer.transform(lon, lat) for lon, lat in points_lonlat]

            # Sample directly at projected point coordinates
            st_vals = list(st_src.sample(pts_xy, masked=True))

            for idx in range(len(points_lonlat)):
                raw_dn = _safe_sample_val(st_vals[idx], default=np.nan)
                lst_c = None

                if np.isfinite(raw_dn) and raw_dn > 0:
                    # Apply USGS Collection 2 scaling: ST_K = DN * 0.00341802 + 149.0
                    st_k = raw_dn * ST_SCALE_FACTOR + ST_OFFSET
                    celsius = st_k - KELVIN_TO_CELSIUS
                    # Defensible agricultural thermal range (-10°C to +60°C)
                    if -10.0 <= celsius <= 60.0:
                        lst_c = round(float(celsius), 2)

                results.append({
                    "lst_c": lst_c,
                    "raw_dn": raw_dn if np.isfinite(raw_dn) else None,
                    "native_resolution_m": 30.0,
                    "source_sensor": "LANDSAT_8" if "LC08" in getattr(item, "id", "") else "LANDSAT_9",
                })

    return results


async def process_landsat_for_land_day(
    land_id: int,
    date_str: str,
    lookback_days: int = 16,
    max_cloud_cover_pct: float = 60.0,
) -> dict[str, Any]:
    """Phase 9: Ingest Landsat 8/9 ~30m field-scale Surface Temperature."""
    land_id = int(land_id)
    logger.info("Landsat processing started land=%s target_date=%s", land_id, date_str)

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
    target_dt = datetime.fromisoformat(date_str).date()
    start_date = (target_dt - timedelta(days=lookback_days)).isoformat()
    end_date = target_dt.isoformat()

    items = await _stac_search_landsat(land_geom, start_date, end_date, land_id, max_cloud_cover_pct)
    if not items:
        return {"processed": 0, "reason": f"No Landsat items found in window {start_date} to {end_date}"}

    # Sort descending by date, lowest cloud cover
    def _item_key(it: Any) -> tuple[datetime, float]:
        props = getattr(it, "properties", {}) or {}
        val = props.get("datetime")
        dt = datetime.min
        if val:
            try:
                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                pass
        cc = float(props.get("eo:cloud_cover", 100.0))
        return (dt, -cc)

    sorted_items = sorted(items, key=_item_key, reverse=True)
    selected_item = sorted_items[0]
    points = [(float(r[1]), float(r[2])) for r in grid_rows]
    grid_ids = [str(r[0]) for r in grid_rows]

    try:
        st_samples = await asyncio.wait_for(
            asyncio.to_thread(_sample_landsat_lst, selected_item, points),
            timeout=120,
        )
    except Exception as exc:
        logger.exception("Landsat raster sampling failed for item %s: %s", getattr(selected_item, "id", None), exc)
        return {"processed": 0, "reason": f"Sampling failed: {exc}"}

    valid_count = sum(1 for s in st_samples if s["lst_c"] is not None)
    if valid_count == 0:
        return {"processed": 0, "reason": "No valid Landsat surface temperature pixels sampled"}

    props = getattr(selected_item, "properties", {}) or {}
    val = props.get("datetime")
    obs_date = datetime.fromisoformat(str(val).replace("Z", "+00:00")).date() if val else target_dt
    stac_item_id = getattr(selected_item, "id", None)
    sensor_name = "LANDSAT_8" if "LC08" in str(stac_item_id) else "LANDSAT_9"

    # Upsert into land_daily_lst with source_sensor='LANDSAT_8' / 'LANDSAT_9' and native_resolution_m=30.0
    upsert_sql = text(
        "INSERT INTO land_daily_lst "
        "(land_id, grid_id, date, lst_c, qc, source_sensor, native_resolution_m, is_modeled) "
        "VALUES (:land_id, :grid_id, :date, :lst_c, 0, :source_sensor, 30.0, FALSE) "
        "ON CONFLICT (grid_id, date) DO UPDATE SET "
        "  lst_c = EXCLUDED.lst_c, "
        "  source_sensor = EXCLUDED.source_sensor, "
        "  native_resolution_m = EXCLUDED.native_resolution_m, "
        "  is_modeled = FALSE"
    )

    params = []
    for gid, sample in zip(grid_ids, st_samples):
        params.append({
            "land_id": land_id,
            "grid_id": gid,
            "date": obs_date,
            "lst_c": sample["lst_c"],
            "source_sensor": sensor_name,
        })

    async with async_session() as session:
        await session.execute(upsert_sql, params)
        await session.commit()

    logger.info("Landsat processing complete land=%s date=%s processed=%d", land_id, obs_date, len(params))
    return {
        "processed": len(params),
        "date": obs_date.isoformat(),
        "stac_item_id": stac_item_id,
        "valid_count": valid_count,
        "source_sensor": sensor_name,
        "mean_lst_c": float(np.nanmean([s["lst_c"] for s in st_samples if s["lst_c"] is not None])) if valid_count > 0 else None,
    }
