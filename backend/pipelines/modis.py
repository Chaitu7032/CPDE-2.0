import asyncio
import json
import logging
from datetime import date as _date
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import rasterio
from pyproj import CRS, Transformer
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from shapely.geometry import Point, shape
from shapely.prepared import prep
from sqlalchemy import text

from backend.db.connection import async_session
from backend.utils.crs import STORAGE_CRS_EPSG


PC_STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"
logger = logging.getLogger(__name__)

# Locked data source per requirements: Planetary Computer STAC.
# Planetary Computer collection for MODIS LST daily (MOD11A1/MYD11A1 v061).
DEFAULT_MODIS_STAC_COLLECTION = "modis-11A1-061"

# Keep these for backwards-compatibility in API responses/logs.
DEFAULT_MODIS_LST_SHORT_NAME = "MOD11A1"
DEFAULT_MODIS_LST_VERSION = "061"

def _parse_valid_range_tag(tag_val: str | None) -> tuple[int | None, int | None]:
    if not tag_val:
        return (None, None)
    try:
        parts = [p.strip() for p in tag_val.split(",")]
        if len(parts) != 2:
            return (None, None)
        return (int(float(parts[0])), int(float(parts[1])))
    except Exception:
        return (None, None)


def _qc_ok(qc_val: int) -> bool:
    # MOD11A1 QC_Day bits (common interpretation)
    # bits 0-1: Mandatory QA flags. 00 is best; 01 is still a produced pixel with
    #           lower confidence and is commonly useful for downstream analysis.
    # bits 2-3: Data quality. Keep only the usable classes.
    mandatory = qc_val & 0b11
    data_quality = (qc_val >> 2) & 0b11
    return mandatory in (0, 1) and data_quality == 0


def _rasterio_env_kwargs() -> dict[str, str]:
    return {"GTIFF_SRS_SOURCE": "EPSG"}


def _sample_modis_lst_from_cogs(
    *,
    lst_href: str,
    qc_href: str | None,
    utm_epsg: int,
    points_lonlat: Sequence[Tuple[float, float]],
) -> List[Dict[str, Any]]:
    """Sample MODIS LST (day) at lon/lat points from Planetary Computer COG assets.

    Scientific / safety constraints:
    - Uses *nearest neighbor* only (no interpolation).
    - Applies QC mask (MOD11A1/MYD11A1 QC_Day bits).
    - Applies scale factor and converts K -> °C.
    - Returns None when missing/invalid; never fills or interpolates.
    """
    results: List[Dict[str, Any]] = []
    if not points_lonlat:
        return results

    dst_crs = CRS.from_epsg(int(utm_epsg))
    to_utm = Transformer.from_crs("EPSG:4326", dst_crs, always_xy=True)
    pts_xy = [to_utm.transform(lon, lat) for lon, lat in points_lonlat]

    logger.info("MODIS raster sampling start href=%s points=%d utm_epsg=%s", lst_href, len(points_lonlat), utm_epsg)

    with rasterio.Env(**_rasterio_env_kwargs()):
        with rasterio.open(lst_href) as lst_src:
            qc_src = rasterio.open(qc_href) if qc_href else None

            logger.info(
                "MODIS raster metadata crs=%s target_crs=%s bounds=%s shape=%sx%s nodata=%s",
                lst_src.crs,
                dst_crs,
                lst_src.bounds,
                lst_src.width,
                lst_src.height,
                lst_src.nodata,
            )

            lst_vrt = None
            qc_vrt = None
            try:
                # Nearest-neighbor resampling is required to avoid fabricated values.
                lst_vrt = WarpedVRT(lst_src, crs=dst_crs, resampling=Resampling.nearest)
                qc_vrt = WarpedVRT(qc_src, crs=dst_crs, resampling=Resampling.nearest) if qc_src else None

                # Scale/offset are present in COG metadata for this collection.
                try:
                    scale = float(lst_src.scales[0]) if getattr(lst_src, "scales", None) and lst_src.scales[0] not in (None, 0) else 0.02
                except Exception:
                    scale = 0.02
                try:
                    offset = float(lst_src.offsets[0]) if getattr(lst_src, "offsets", None) and lst_src.offsets[0] not in (None,) else 0.0
                except Exception:
                    offset = 0.0

                vmin, vmax = _parse_valid_range_tag(lst_src.tags().get("valid_range"))
                if vmin is None:
                    vmin = 1
                if vmax is None:
                    vmax = 65535

                lst_vals = list(lst_vrt.sample(pts_xy, masked=True))
                # QC in this collection can validly be 0; do not use masked=True because dataset nodata is 0.
                qc_vals = list(qc_vrt.sample(pts_xy, masked=False)) if qc_vrt else [None] * len(pts_xy)

                for i in range(len(pts_xy)):
                    lst_dn: int | None = None
                    if lst_vals[i] is not None and not lst_vals[i].mask[0]:
                        try:
                            lst_dn = int(lst_vals[i][0])
                        except Exception:
                            lst_dn = None

                    qc_value: int | None = None
                    qc_ok = True
                    if qc_vrt and qc_vals[i] is not None:
                        try:
                            qc_value = int(qc_vals[i][0])
                            qc_ok = _qc_ok(qc_value)
                        except Exception:
                            qc_value = None
                            qc_ok = False

                    lst_c: float | None = None
                    if lst_dn is not None and (vmin <= lst_dn <= vmax) and qc_ok:
                        k = (lst_dn * scale) + offset
                        lst_c = float(k - 273.15)

                    results.append({"lst_c": lst_c, "qc": qc_value, "qc_ok": qc_ok})
            except Exception as exc:
                logger.exception("MODIS reprojection/sampling failed for href=%s", lst_href)
                raise RuntimeError(f"MODIS reprojection failed for target EPSG:{utm_epsg}: {exc}") from exc
            finally:
                if lst_vrt is not None:
                    lst_vrt.close()
                if qc_vrt is not None:
                    qc_vrt.close()
                if qc_src is not None:
                    qc_src.close()

    return results


async def _sample_modis_day(
    *,
    items: Sequence[Any],
    points_lonlat: Sequence[Tuple[float, float]],
    utm_epsg: int,
    land_geom: Any,
) -> tuple[list[dict[str, Any]], list[Any], int]:
    import planetary_computer  # type: ignore

    signed_items = [planetary_computer.sign(it) for it in items]
    tiles: list[tuple[Any, Any]] = [
        (it, prep(shape(it.geometry)))
        for it in signed_items
        if getattr(it, "geometry", None)
    ]
    if not tiles and signed_items:
        # Extremely defensive fallback: if tile geometries are missing, sample all points from the first item.
        tiles = [(signed_items[0], prep(land_geom))]

    if not tiles:
        return [], signed_items, 0

    assignments: list[int | None] = [None] * len(points_lonlat)
    for i, (lon, lat) in enumerate(points_lonlat):
        pt = Point(lon, lat)
        for tile_idx, (_it, geom) in enumerate(tiles):
            # Use covers() so points on tile boundaries are included.
            if geom.covers(pt):
                assignments[i] = tile_idx
                break

    # Prepare result list aligned to grids.
    samples: list[dict[str, Any]] = [{"lst_c": None, "qc": None, "qc_ok": False} for _ in range(len(points_lonlat))]

    # Sample each tile once for all its points.
    for tile_idx, (it, _geom) in enumerate(tiles):
        idxs = [i for i, a in enumerate(assignments) if a == tile_idx]
        if not idxs:
            continue
        sub_points = [points_lonlat[i] for i in idxs]

        lst_href = it.assets["LST_Day_1km"].href
        qc_href = it.assets["QC_Day"].href if "QC_Day" in it.assets else None
        tile_samples = await asyncio.to_thread(
            _sample_modis_lst_from_cogs,
            lst_href=lst_href,
            qc_href=qc_href,
            utm_epsg=int(utm_epsg),
            points_lonlat=sub_points,
        )
        for j, grid_i in enumerate(idxs):
            samples[grid_i] = tile_samples[j]

    valid_count = sum(1 for sample in samples if sample["lst_c"] is not None and np.isfinite(sample["lst_c"]))
    logger.info("MODIS day sampling completed valid_count=%d total_points=%d", valid_count, len(points_lonlat))
    return samples, signed_items, valid_count


async def process_modis_for_land_day(land_id, date: str, collection_concept_id: str = None, allow_fallback: bool = True) -> dict:
    """Phase 4 — MODIS LST ingestion via Planetary Computer STAC (no Earthdata OAuth).

    - Queries Planetary Computer STAC for MODIS LST daily (collection `modis-11A1-061` by default)
    - Uses COG assets `LST_Day_1km` + `QC_Day`
    - Nearest-neighbor only (no interpolation); QC-masked; scaled; Kelvin -> Celsius
    - Excludes water grids (from Sentinel-2 SCL==6)
    - Samples grid centroids and stores per-grid daily LST in `land_daily_lst`

    Returns a dict compatible with existing API responses, plus:
        - lst_mean: mean LST (°C) across processed non-null samples
        - lst_date: the actual MODIS day used (YYYY-MM-DD)
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    logger.info(
        "MODIS processing start land=%s requested_date=%s allow_fallback=%s collection_override=%s",
        land_id,
        date,
        allow_fallback,
        collection_concept_id,
    )
    # fetch bbox, grid centroids, and land UTM EPSG
    async with async_session() as session:
        land_res = await session.execute(
            text(
                "SELECT "
                "ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
                "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lat, "
                "utm_epsg "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        land_row = land_res.first()
        if not land_row:
            return {"processed": 0, "reason": "land not found"}
        _land_lon, _land_lat, utm_epsg = float(land_row[0]), float(land_row[1]), land_row[2]

        if not utm_epsg:
            utm_epsg = int(STORAGE_CRS_EPSG)
            await session.execute(text("UPDATE lands SET utm_epsg = :epsg WHERE land_id = :lid"), {"epsg": int(utm_epsg), "lid": land_id})

        grids_res = await session.execute(
            text(
                "SELECT grid_id, "
                "ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
                "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lat "
                "FROM land_grid_cells WHERE land_id = :lid AND COALESCE(is_water, FALSE) = FALSE ORDER BY grid_id"
            ),
            {"lid": land_id},
        )
        grid_rows = grids_res.fetchall()

        bbox_res = await session.execute(
            text(
                "SELECT ST_XMin(ext) AS minx, ST_YMin(ext) AS miny, ST_XMax(ext) AS maxx, ST_YMax(ext) AS maxy "
                "FROM (SELECT ST_Extent(ST_Transform(geom, 4326)) AS ext FROM land_grid_cells WHERE land_id = :lid) q"
            ),
            {"lid": land_id},
        )
        bbox_row = bbox_res.first()

    if not grid_rows:
        return {"processed": 0, "reason": "no non-water grids for land"}
    if not bbox_row:
        return {"processed": 0, "reason": "cannot compute bbox"}

    bbox = [float(bbox_row[0]), float(bbox_row[1]), float(bbox_row[2]), float(bbox_row[3])]

    # Determine STAC collection id.
    stac_collection_id = DEFAULT_MODIS_STAC_COLLECTION
    if collection_concept_id:
        # Support overrides while keeping backward compatibility with the API param name.
        # If the provided value looks like a Planetary Computer collection id, use it.
        if collection_concept_id.startswith("modis-"):
            stac_collection_id = collection_concept_id

    short_name = DEFAULT_MODIS_LST_SHORT_NAME
    version = DEFAULT_MODIS_LST_VERSION

    # Fetch land geometry for STAC intersects query
    async with async_session() as session:
        land_geom_res = await session.execute(
            text("SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)) FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        land_geom_row = land_geom_res.first()

    if not land_geom_row or not land_geom_row[0]:
        return {"processed": 0, "reason": "land geometry not found"}

    land_geom = shape(json.loads(land_geom_row[0]))

    # Search backward until we find a day that actually yields usable LST, or only the exact day in strict mode.
    from datetime import timedelta as _td
    from pystac_client import Client  # type: ignore

    client = Client.open(PC_STAC_API)
    target = datetime.fromisoformat(date).date()
    chosen_date: _date | None = None
    chosen_items: list[Any] = []
    samples: list[dict[str, Any]] = []
    # PC MODIS archive has up to ~12-day latency; search 14 days back to ensure a hit.
    day_offsets = range(0, 15) if allow_fallback else range(0, 1)
    for day_offset in day_offsets:
        day = target - _td(days=day_offset)
        dt = f"{day.strftime('%Y-%m-%d')}T00:00:00Z/{day.strftime('%Y-%m-%d')}T23:59:59Z"
        logger.info(
            "MODIS STAC search land=%s collection=%s datetime=%s",
            land_id,
            stac_collection_id,
            dt,
        )
        search = client.search(collections=[stac_collection_id], intersects=land_geom.__geo_interface__, datetime=dt, max_items=200)
        items = list(search.items())
        # Keep only items with required assets.
        items = [it for it in items if hasattr(it, "assets") and ("LST_Day_1km" in it.assets)]
        logger.info("MODIS STAC returned %d candidate items for day=%s land=%s", len(items), day, land_id)
        if items:
            try:
                candidate_samples, candidate_items, valid_count = await _sample_modis_day(
                    items=items,
                    points_lonlat=[(float(r[1]), float(r[2])) for r in grid_rows],
                    utm_epsg=int(utm_epsg),
                    land_geom=land_geom,
                )
            except Exception as exc:
                logger.exception("MODIS sampling failed for land=%s day=%s", land_id, day)
                return {"processed": 0, "reason": f"MODIS reprojection/sampling failed for land {land_id} on {day.strftime('%Y-%m-%d')}: {exc}"}

            qc_values = sorted({sample["qc"] for sample in candidate_samples if sample.get("qc") is not None})
            logger.info(
                "MODIS candidate day=%s valid_count=%d qc_values=%s",
                day,
                valid_count,
                qc_values[:10],
            )
            if valid_count > 0:
                chosen_date = day
                chosen_items = candidate_items
                samples = candidate_samples
                break

    if not chosen_items or chosen_date is None or not samples:
        logger.warning(
            "MODIS processing found no usable LST for land=%s requested_date=%s collection=%s",
            land_id,
            date,
            stac_collection_id,
        )
        return {"processed": 0, "reason": "no MODIS scenes with usable LST found for land within 14-day window"}

    # land_grid_cells.grid_id is INT in the current DB; pipeline tables store grid_id as VARCHAR
    grid_ids = [str(r[0]) for r in grid_rows]

    date_obj = chosen_date
    upsert_sql = text(
        "INSERT INTO land_daily_lst (land_id, grid_id, date, lst_c, qc) "
        "VALUES (:land_id, :grid_id, :date, :lst_c, :qc) "
        "ON CONFLICT (grid_id, date) DO UPDATE SET lst_c = EXCLUDED.lst_c, qc = EXCLUDED.qc"
    )

    processed = 0
    lst_nonnull: list[float] = []
    async with async_session() as session:
        params = []
        for gid, vals in zip(grid_ids, samples):
            params.append({"land_id": land_id, "grid_id": gid, "date": date_obj, "lst_c": vals["lst_c"], "qc": vals["qc"]})
            if vals.get("lst_c") is not None and np.isfinite(vals.get("lst_c")):
                lst_nonnull.append(float(vals["lst_c"]))
            processed += 1
        await session.execute(upsert_sql, params)
        await session.commit()

    lst_mean: float | None = (sum(lst_nonnull) / len(lst_nonnull)) if lst_nonnull else None

    # Backwards-compat fields: granule_id/hdf_url
    granule_id = getattr(chosen_items[0], "id", None)
    hdf_url = None
    try:
        if "hdf" in chosen_items[0].assets:
            hdf_url = chosen_items[0].assets["hdf"].href
    except Exception:
        hdf_url = None

    return {
        "processed": processed,
        "short_name": short_name,
        "version": version,
        "granule_id": granule_id,
        "hdf_url": hdf_url,
        "stac_collection": stac_collection_id,
        "lst_mean": lst_mean,
        "lst_date": chosen_date.strftime("%Y-%m-%d"),
    }
