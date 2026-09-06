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
import math
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
SAR_COLLECTION = "sentinel-1-grd"


def _rasterio_env_kwargs() -> dict[str, str]:
    proj_dir = os.path.abspath(os.path.join(os.path.dirname(rasterio.__file__), "proj_data"))
    return {
        "GTIFF_SRS_SOURCE": "EPSG",
        "PROJ_LIB": proj_dir,
        "PROJ_DATA": proj_dir,
    }


async def _stac_search_sentinel1(
    land_geom: Any,
    start_date: str,
    end_date: str,
    land_id: Any,
) -> list[Any]:
    """Search Planetary Computer STAC for Sentinel-1 GRD items intersecting land geometry."""
    from pystac_client import Client  # type: ignore

    dt = f"{start_date}T00:00:00Z/{end_date}T23:59:59Z"
    logger.info(
        "Sentinel-1 STAC search land=%s collection=%s datetime=%s",
        land_id,
        SAR_COLLECTION,
        dt,
    )

    def _search() -> list[Any]:
        client = Client.open(PC_STAC_API)
        search = client.search(
            collections=[SAR_COLLECTION],
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
            "Sentinel-1 STAC search timed out after %ss for land=%s",
            STAC_SEARCH_TIMEOUT_S,
            land_id,
        )
        return []
    except Exception as exc:
        logger.error("Sentinel-1 STAC search error for land=%s: %s", land_id, exc)
        return []

    # Keep items that have vv and vh assets
    valid_items = [
        it for it in items
        if hasattr(it, "assets") and ("vv" in it.assets or "vh" in it.assets)
    ]
    logger.info("Sentinel-1 STAC search returned %d valid items for land=%s", len(valid_items), land_id)
    return valid_items


def _sample_sar_for_points(
    item: Any,
    points_lonlat: Sequence[Tuple[float, float]],
) -> List[Dict[str, Any]]:
    """Sample VV and VH backscatter assets at points."""
    import planetary_computer  # type: ignore

    signed_item = planetary_computer.sign(item)
    vv_asset = signed_item.assets.get("vv")
    vh_asset = signed_item.assets.get("vh")

    if not vv_asset and not vh_asset:
        return []

    vv_href = vv_asset.href if vv_asset else None
    vh_href = vh_asset.href if vh_asset else None

    results: List[Dict[str, Any]] = []

    with rasterio.Env(**_rasterio_env_kwargs()):
        primary_href = vv_href or vh_href
        with rasterio.open(primary_href) as ref_src:
            target_crs = ref_src.crs
            src_crs = None
            if target_crs is None:
                gcps, gcp_crs = ref_src.gcps
                target_crs = gcp_crs
                src_crs = gcp_crs
                pts_xy = points_lonlat
            else:
                transformer = Transformer.from_crs("EPSG:4326", target_crs, always_xy=True)
                pts_xy = [transformer.transform(lon, lat) for lon, lat in points_lonlat]

            vv_vals = [None] * len(points_lonlat)
            vh_vals = [None] * len(points_lonlat)

            if vv_href:
                with rasterio.open(vv_href) as vv_src:
                    vrt_kwargs = {"resampling": Resampling.bilinear}
                    if target_crs is not None:
                        vrt_kwargs["crs"] = target_crs
                    if src_crs is not None:
                        vrt_kwargs["src_crs"] = src_crs
                    with WarpedVRT(vv_src, **vrt_kwargs) as vrt:
                        vv_vals = list(vrt.sample(pts_xy, masked=True))

            if vh_href:
                with rasterio.open(vh_href) as vh_src:
                    vrt_kwargs = {"resampling": Resampling.bilinear}
                    if target_crs is not None:
                        vrt_kwargs["crs"] = target_crs
                    if src_crs is not None:
                        vrt_kwargs["src_crs"] = src_crs
                    with WarpedVRT(vh_src, **vrt_kwargs) as vrt:
                        vh_vals = list(vrt.sample(pts_xy, masked=True))

            for idx in range(len(points_lonlat)):
                raw_vv = _safe_sample_val(vv_vals[idx], default=np.nan)
                raw_vh = _safe_sample_val(vh_vals[idx], default=np.nan)

                # Convert linear power to dB if values are linear (> 0)
                vv_db = 10.0 * math.log10(raw_vv) if np.isfinite(raw_vv) and raw_vv > 0 else (raw_vv if np.isfinite(raw_vv) else None)
                vh_db = 10.0 * math.log10(raw_vh) if np.isfinite(raw_vh) and raw_vh > 0 else (raw_vh if np.isfinite(raw_vh) else None)

                # Cross-ratio: VH/VV in dB is (VH_dB - VV_dB), in linear is raw_vh / raw_vv
                ratio = (vh_db - vv_db) if (vv_db is not None and vh_db is not None) else None

                results.append({
                    "vv": vv_db,
                    "vh": vh_db,
                    "vh_vv_ratio": ratio,
                    "raw_vv": raw_vv if np.isfinite(raw_vv) else None,
                    "raw_vh": raw_vh if np.isfinite(raw_vh) else None,
                })

    return results


async def process_sentinel1_for_land_day(
    land_id: int,
    date_str: str,
    lookback_days: int = 12,
) -> dict[str, Any]:
    """Phase 5: Ingest Sentinel-1 C-band SAR observations for cloud-independent monitoring."""
    land_id = int(land_id)
    logger.info("Sentinel-1 processing started land=%s target_date=%s", land_id, date_str)

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

    items = await _stac_search_sentinel1(land_geom, start_date, end_date, land_id)
    if not items:
        return {"processed": 0, "reason": f"No Sentinel-1 items found in window {start_date} to {end_date}"}

    # Sort descending by date
    def _item_dt(it: Any) -> datetime:
        props = getattr(it, "properties", {}) or {}
        val = props.get("datetime")
        if val:
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except Exception:
                pass
        return datetime.min.replace(tzinfo=None)

    sorted_items = sorted(items, key=_item_dt, reverse=True)
    selected_item = sorted_items[0]
    points = [(float(r[1]), float(r[2])) for r in grid_rows]
    grid_ids = [str(r[0]) for r in grid_rows]

    try:
        sar_samples = await asyncio.wait_for(
            asyncio.to_thread(_sample_sar_for_points, selected_item, points),
            timeout=120,
        )
    except Exception as exc:
        logger.exception("Sentinel-1 raster sampling failed for item %s: %s", getattr(selected_item, "id", None), exc)
        return {"processed": 0, "reason": f"Sampling failed: {exc}"}

    valid_count = sum(1 for s in sar_samples if s["vv"] is not None or s["vh"] is not None)
    if valid_count == 0:
        return {"processed": 0, "reason": "No valid SAR backscatter pixels sampled"}

    item_dt = _item_dt(selected_item)
    obs_date = item_dt.date() if item_dt != datetime.min.date() else target_dt
    props = getattr(selected_item, "properties", {}) or {}
    orbit_direction = props.get("sat:orbit_state") or props.get("orbit_direction") or "descending"
    stac_item_id = getattr(selected_item, "id", None)

    # Upsert into land_daily_sar
    upsert_sql = text(
        "INSERT INTO land_daily_sar "
        "(land_id, grid_id, date, stac_item_id, acquisition_datetime, orbit_direction, "
        " vv, vh, vh_vv_ratio, native_resolution_m, quality_flag) "
        "VALUES (:land_id, :grid_id, :date, :stac_item_id, :acquisition_datetime, :orbit_direction, "
        "        :vv, :vh, :vh_vv_ratio, 10.0, 'OBSERVED') "
        "ON CONFLICT (grid_id, date) DO UPDATE SET "
        "  stac_item_id = EXCLUDED.stac_item_id, "
        "  acquisition_datetime = EXCLUDED.acquisition_datetime, "
        "  orbit_direction = EXCLUDED.orbit_direction, "
        "  vv = EXCLUDED.vv, "
        "  vh = EXCLUDED.vh, "
        "  vh_vv_ratio = EXCLUDED.vh_vv_ratio, "
        "  quality_flag = EXCLUDED.quality_flag"
    )

    params = []
    for gid, sample in zip(grid_ids, sar_samples):
        params.append({
            "land_id": land_id,
            "grid_id": gid,
            "date": obs_date,
            "stac_item_id": stac_item_id,
            "acquisition_datetime": item_dt if item_dt != datetime.min else None,
            "orbit_direction": str(orbit_direction),
            "vv": sample["vv"],
            "vh": sample["vh"],
            "vh_vv_ratio": sample["vh_vv_ratio"],
        })

    async with async_session() as session:
        await session.execute(upsert_sql, params)
        await session.commit()

    logger.info("Sentinel-1 processing complete land=%s date=%s processed=%d", land_id, obs_date, len(params))
    return {
        "processed": len(params),
        "date": obs_date.isoformat(),
        "stac_item_id": stac_item_id,
        "valid_count": valid_count,
        "mean_vv": float(np.nanmean([s["vv"] for s in sar_samples if s["vv"] is not None])) if valid_count > 0 else None,
        "mean_vh": float(np.nanmean([s["vh"] for s in sar_samples if s["vh"] is not None])) if valid_count > 0 else None,
    }
