from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from shapely.geometry import shape
from sqlalchemy import text

from backend.db.connection import async_session
from backend.pipelines.anomaly import (
    VARIABLE_SOURCES,
    build_climatology_for_variable,
    compute_anomalies_for_date,
)
from backend.pipelines.grid_generation import generate_and_store_grids
from backend.pipelines.modis import (
    DEFAULT_MODIS_STAC_COLLECTION,
    PC_STAC_API as MODIS_PC_STAC_API,
    _sample_modis_day,
    process_modis_for_land_day,
)
from backend.pipelines.nasa_power import (
    POWER_REQUEST_TIMEOUT_S,
    fetch_power_point,
    process_weather_for_land,
)
from backend.pipelines.risk import compute_risk_for_land_date
from backend.pipelines.sentinel2 import (
    PC_STAC_API as SENTINEL2_PC_STAC_API,
    _compute_indices_for_points,
    _extract_cloud_cover,
    _extract_item_datetime,
    _extract_tile_id,
    _item_sort_key,
    process_sentinel2_for_land_day,
)
from backend.utils.crs import STORAGE_CRS_EPSG, geometry_geojson_storage_to_api


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["dashboard"])

FIND_DATE_TIMEOUT_S = 600
S2_AVAILABILITY_TIMEOUT_S = 90
MODIS_AVAILABILITY_TIMEOUT_S = 90
NO_DATA_COLOR = "#808080"

COLOR_SCALES = {
    "ndvi": [
        {"range": "< 0.2", "label": "Severe stress", "color": "#7f1d1d"},
        {"range": "0.2 - 0.4", "label": "Stressed", "color": "#f97316"},
        {"range": "0.4 - 0.6", "label": "Moderate", "color": "#facc15"},
        {"range": ">= 0.6", "label": "Healthy", "color": "#16a34a"},
    ],
    "ndmi": [
        {"range": "< -0.1", "label": "Dry", "color": "#dc2626"},
        {"range": "-0.1 - 0", "label": "Slightly dry", "color": "#f59e0b"},
        {"range": "0 - 0.2", "label": "Moderate", "color": "#7dd3fc"},
        {"range": ">= 0.2", "label": "Wet", "color": "#2563eb"},
    ],
    "lst": [
        {"range": "< 25", "label": "Cool", "color": "#2563eb"},
        {"range": "25 - 30", "label": "Normal", "color": "#16a34a"},
        {"range": "30 - 35", "label": "Warm", "color": "#f59e0b"},
        {"range": ">= 35", "label": "Hot stress", "color": "#dc2626"},
    ],
    "no_data_color": NO_DATA_COLOR,
}

_AVAILABILITY_CACHE: dict[tuple[int, str, float], dict[str, Any]] = {}
_EXACT_CONTEXT_CACHE: dict[tuple[int, str, float], dict[str, Any]] = {}


def _availability_cache_key(land_id: int, date_str: str, cloud_threshold_pct: float) -> tuple[int, str, float]:
    return (int(land_id), str(date_str), float(cloud_threshold_pct))


def _get_cached_exact_availability(land_id: int, date_str: str, cloud_threshold_pct: float) -> dict[str, Any] | None:
    return _AVAILABILITY_CACHE.get(_availability_cache_key(land_id, date_str, cloud_threshold_pct))


def _store_cached_exact_availability(land_id: int, date_str: str, cloud_threshold_pct: float, payload: dict[str, Any]) -> None:
    _AVAILABILITY_CACHE[_availability_cache_key(land_id, date_str, cloud_threshold_pct)] = payload


def _get_exact_context(land_id: int, date_str: str, cloud_threshold_pct: float) -> dict[str, Any] | None:
    return _EXACT_CONTEXT_CACHE.get(_availability_cache_key(land_id, date_str, cloud_threshold_pct))


def _store_exact_context(land_id: int, date_str: str, cloud_threshold_pct: float, payload: dict[str, Any]) -> None:
    _EXACT_CONTEXT_CACHE[_availability_cache_key(land_id, date_str, cloud_threshold_pct)] = payload


def _invalidate_land_cache(land_id: int) -> None:
    lid = int(land_id)
    for cache in (_AVAILABILITY_CACHE, _EXACT_CONTEXT_CACHE):
        for key in [cache_key for cache_key in cache if cache_key[0] == lid]:
            cache.pop(key, None)


def _is_number(value: Any) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _to_float(value: Any) -> float | None:
    return float(value) if _is_number(value) else None


def _to_iso_string(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _stats(values: list[float | None]) -> dict[str, Any] | None:
    finite = [float(v) for v in values if _is_number(v)]
    if not finite:
        return None
    return {
        "mean": sum(finite) / len(finite),
        "min": min(finite),
        "max": max(finite),
        "count": len(finite),
    }


def _bounds(values: list[float | None]) -> tuple[float | None, float | None]:
    finite = [float(v) for v in values if _is_number(v)]
    if not finite:
        return None, None
    return min(finite), max(finite)


def _normalize(value: float | None, minimum: float | None, maximum: float | None) -> float | None:
    if value is None or minimum is None or maximum is None or maximum == minimum:
        return None
    return max(0.0, min(1.0, (float(value) - minimum) / (maximum - minimum)))


def _threshold_color(value: float | None, thresholds: list[tuple[float, str]]) -> str:
    if value is None:
        return NO_DATA_COLOR
    numeric = float(value)
    for upper, color in thresholds:
        if numeric < upper:
            return color
    return thresholds[-1][1] if thresholds else NO_DATA_COLOR


def _ndvi_color(value: float | None) -> str:
    return _threshold_color(value, [(0.2, "#7f1d1d"), (0.4, "#f97316"), (0.6, "#facc15"), (math.inf, "#16a34a")])


def _ndmi_color(value: float | None) -> str:
    return _threshold_color(value, [(-0.1, "#dc2626"), (0.0, "#f59e0b"), (0.2, "#7dd3fc"), (math.inf, "#2563eb")])


def _lst_color(value: float | None) -> str:
    return _threshold_color(value, [(25.0, "#2563eb"), (30.0, "#16a34a"), (35.0, "#f59e0b"), (math.inf, "#dc2626")])


def _unique_dates(*dates: str | None) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in dates:
        if value and value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _safe_s2_date(s2: dict[str, Any], fallback: str) -> str:
    raw = s2.get("datetime") or ""
    if len(raw) >= 10:
        return raw[:10]
    return fallback


async def _set_status(land_id: int, status: str, step: str, error: str | None = None) -> None:
    async with async_session() as session:
        await session.execute(
            text(
                "INSERT INTO processing_jobs (land_id, status, step, error, updated_at) "
                "VALUES (:lid, :status, :step, :error, now()) "
                "ON CONFLICT (land_id) DO UPDATE SET status = EXCLUDED.status, step = EXCLUDED.step, error = EXCLUDED.error, updated_at = EXCLUDED.updated_at"
            ),
            {"lid": land_id, "status": status, "step": step, "error": error},
        )
        await session.commit()


async def _get_status(land_id: int) -> dict[str, Any]:
    async with async_session() as session:
        res = await session.execute(
            text("SELECT status, step, error FROM processing_jobs WHERE land_id = :lid"),
            {"lid": land_id},
        )
        row = res.first()
    if row is None:
        return {"status": "unknown", "step": None, "error": None}
    return {"status": row[0], "step": row[1], "error": row[2]}


async def _set_dashboard_state(land_id: int, mode: str, selected_date: str | None) -> None:
    selected_date_value = None
    if selected_date is not None:
        selected_date_value = datetime.fromisoformat(selected_date).date() if isinstance(selected_date, str) else selected_date

    async with async_session() as session:
        await session.execute(
            text(
                "INSERT INTO land_dashboard_state (land_id, mode, selected_date, updated_at) "
                "VALUES (:lid, :mode, :selected_date, now()) "
                "ON CONFLICT (land_id) DO UPDATE SET mode = EXCLUDED.mode, selected_date = EXCLUDED.selected_date, updated_at = EXCLUDED.updated_at"
            ),
            {"lid": land_id, "mode": mode, "selected_date": selected_date_value},
        )
        await session.commit()


async def _get_dashboard_state(land_id: int) -> dict[str, Any]:
    async with async_session() as session:
        res = await session.execute(
            text("SELECT mode, selected_date FROM land_dashboard_state WHERE land_id = :lid"),
            {"lid": land_id},
        )
        row = res.first()
    if row is None:
        return {"mode": "latest", "selected_date": None}
    return {"mode": row[0] or "latest", "selected_date": str(row[1]) if row[1] else None}


async def _get_latest_complete_date(land_id: int) -> str | None:
    async with async_session() as session:
        res = await session.execute(
            text(
                "WITH grid_counts AS ("
                "  SELECT COUNT(*) AS total_grids, COUNT(*) FILTER (WHERE COALESCE(is_water, FALSE) = FALSE) AS non_water_grids "
                "  FROM land_grid_cells WHERE land_id = :lid"
                "), s2_dates AS ("
                "  SELECT date FROM land_daily_indices WHERE land_id = :lid GROUP BY date "
                "  HAVING COUNT(DISTINCT grid_id) = (SELECT total_grids FROM grid_counts)"
                "), modis_dates AS ("
                "  SELECT date FROM land_daily_lst WHERE land_id = :lid GROUP BY date "
                "  HAVING COUNT(DISTINCT grid_id) = (SELECT non_water_grids FROM grid_counts)"
                "), weather_dates AS ("
                "  SELECT date FROM land_daily_weather WHERE land_id = :lid GROUP BY date"
                ") SELECT MAX(date) FROM ("
                "  SELECT date FROM s2_dates INTERSECT SELECT date FROM modis_dates INTERSECT SELECT date FROM weather_dates"
                ") q"
            ),
            {"lid": land_id},
        )
        row = res.first()
    return str(row[0]) if row and row[0] else None


async def _get_latest_satellite_date(land_id: int) -> str | None:
    async with async_session() as session:
        res = await session.execute(text("SELECT MAX(date) FROM land_daily_indices WHERE land_id = :lid"), {"lid": land_id})
        row = res.first()
    return str(row[0]) if row and row[0] else None


async def _load_land_context(land_id: int, *, non_water_only: bool = False) -> tuple[Any, list[tuple[Any, float, float]], int]:
    async with async_session() as session:
        land_res = await session.execute(
            text("SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)), COALESCE(utm_epsg, :fallback_epsg) FROM lands WHERE land_id = :lid"),
            {"lid": land_id, "fallback_epsg": int(STORAGE_CRS_EPSG)},
        )
        land_row = land_res.first()
        if not land_row or not land_row[0]:
            raise HTTPException(status_code=404, detail="Land not found")

        grid_sql = (
            "SELECT grid_id, ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
            "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lat "
            "FROM land_grid_cells WHERE land_id = :lid"
        )
        if non_water_only:
            grid_sql += " AND COALESCE(is_water, FALSE) = FALSE"
        grid_sql += " ORDER BY grid_id"

        grid_res = await session.execute(text(grid_sql), {"lid": land_id})
        grid_rows = grid_res.fetchall()

    land_geom = shape(json.loads(land_row[0]))
    utm_epsg = int(land_row[1] or STORAGE_CRS_EPSG)
    points = [(row[0], float(row[1]), float(row[2])) for row in grid_rows]
    return land_geom, points, utm_epsg


async def _compute_anomalies_for_dates(land_id: int, dates: list[str]) -> None:
    for date_str in _unique_dates(*dates):
        try:
            await compute_anomalies_for_date(
                land_id,
                date_str,
                variables=["ndvi", "ndmi", "lst", "t2m", "rh2m", "prectotcorr"],
            )
        except Exception as exc:
            logger.warning("Anomaly compute error for date %s: %s", date_str, exc)


async def _check_sentinel2_exact_availability(land_id: int, date_str: str, cloud_threshold_pct: float = 60.0) -> dict[str, Any]:
    cached_result = _get_cached_exact_availability(land_id, date_str, cloud_threshold_pct)
    if cached_result is not None and cached_result.get("sources", {}).get("sentinel2") is not None:
        logger.info("availability cache hit source=sentinel2 land_id=%s date=%s", land_id, date_str)
        return cached_result["sources"]["sentinel2"]

    land_geom, grid_points, _ = await _load_land_context(land_id)
    if not grid_points:
        result = {"available": False, "source": "sentinel2", "reason": "no grids available for this land"}
        _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
        return result

    def _search_s2() -> list[Any]:
        from pystac_client import Client  # type: ignore

        target = datetime.fromisoformat(date_str).date()
        dt = f"{target:%Y-%m-%d}T00:00:00Z/{target:%Y-%m-%d}T23:59:59Z"
        client = Client.open(SENTINEL2_PC_STAC_API)
        search = client.search(
            collections=["sentinel-2-l2a"],
            intersects=land_geom.__geo_interface__,
            datetime=dt,
            max_items=50,
        )
        items = list(search.items())
        return [it for it in items if hasattr(it, "assets") and all(k in it.assets for k in ("B04", "B08", "B11", "SCL"))]

    try:
        items = await asyncio.wait_for(asyncio.to_thread(_search_s2), timeout=S2_AVAILABILITY_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.error("Sentinel-2 STAC availability search timed out land=%s date=%s", land_id, date_str)
        result = {"available": False, "source": "sentinel2", "reason": f"Sentinel-2 STAC search timed out after {S2_AVAILABILITY_TIMEOUT_S}s"}
        _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
        return result
    except Exception as exc:
        logger.exception("Sentinel-2 STAC search failed for land %s date %s", land_id, date_str)
        result = {"available": False, "source": "sentinel2", "reason": f"Sentinel-2 STAC search failed: {exc}"}
        _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
        return result

    if not items:
        result = {"available": False, "source": "sentinel2", "reason": "no Sentinel-2 scenes found for the exact date"}
        _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
        return result

    points_lonlat = [(lon, lat) for _, lon, lat in grid_points]
    last_error: str | None = None
    for candidate in sorted(items, key=_item_sort_key, reverse=True):
        cloud_cover = _extract_cloud_cover(candidate)
        if cloud_cover is None:
            continue
        if cloud_threshold_pct is not None and cloud_cover > cloud_threshold_pct:
            continue

        try:
            candidate_results = await asyncio.wait_for(asyncio.to_thread(_compute_indices_for_points, candidate, points_lonlat), timeout=120)
        except asyncio.TimeoutError:
            last_error = "raster sampling timed out"
            logger.error("Sentinel-2 availability sampling timed out land=%s item=%s", land_id, getattr(candidate, "id", None))
            continue
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("Sentinel-2 availability sampling failed for land %s item=%s", land_id, getattr(candidate, "id", None))
            continue

        usable_count = sum(1 for result in candidate_results if result.get("pixel_count", 0) > 0)
        if usable_count > 0:
            item_dt = _extract_item_datetime(candidate)
            result = {
                "available": True,
                "source": "sentinel2",
                "stac_item_id": getattr(candidate, "id", None),
                "acquisition_datetime": item_dt.isoformat() if item_dt else None,
                "tile_id": _extract_tile_id(candidate),
                "cloud_cover_pct": cloud_cover,
                "usable_grid_count": usable_count,
                "total_grid_count": len(points_lonlat),
                "reason": None,
            }
            _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
            _store_exact_context(land_id, date_str, cloud_threshold_pct, {"sentinel2": {"items": items, "selected_item": candidate}})
            return result

    result = {
        "available": False,
        "source": "sentinel2",
        "reason": f"Sentinel-2 reprojection/sampling failed: {last_error}" if last_error else "no usable Sentinel-2 pixels found for the exact date",
    }
    _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, {"sources": {"sentinel2": result}})
    _store_exact_context(land_id, date_str, cloud_threshold_pct, {"sentinel2": {"items": items, "selected_item": None}})
    return result


async def _check_modis_exact_availability(land_id: int, date_str: str) -> dict[str, Any]:
    cached_result = _get_cached_exact_availability(land_id, date_str, 60.0)
    if cached_result is not None and cached_result.get("sources", {}).get("modis") is not None:
        logger.info("availability cache hit source=modis land_id=%s date=%s", land_id, date_str)
        return cached_result["sources"]["modis"]

    land_geom, grid_points, utm_epsg = await _load_land_context(land_id, non_water_only=True)
    if not grid_points:
        result = {"available": False, "source": "modis", "reason": "no non-water grids available for this land"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        return result

    def _search_modis() -> list[Any]:
        from pystac_client import Client  # type: ignore

        target = datetime.fromisoformat(date_str).date()
        dt = f"{target:%Y-%m-%d}T00:00:00Z/{target:%Y-%m-%d}T23:59:59Z"
        client = Client.open(MODIS_PC_STAC_API)
        search = client.search(
            collections=[DEFAULT_MODIS_STAC_COLLECTION],
            intersects=land_geom.__geo_interface__,
            datetime=dt,
            max_items=200,
        )
        items = list(search.items())
        return [it for it in items if hasattr(it, "assets") and "LST_Day_1km" in it.assets]

    try:
        items = await asyncio.wait_for(asyncio.to_thread(_search_modis), timeout=MODIS_AVAILABILITY_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.warning("MODIS STAC availability search timed out land=%s date=%s", land_id, date_str)
        result = {"available": False, "source": "modis", "reason": f"MODIS STAC search timed out after {MODIS_AVAILABILITY_TIMEOUT_S}s"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        return result
    except Exception as exc:
        logger.warning("MODIS STAC search failed land=%s date=%s: %s", land_id, date_str, exc)
        result = {"available": False, "source": "modis", "reason": f"MODIS STAC search failed: {exc}"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        _store_exact_context(land_id, date_str, 60.0, {"modis": {"items": []}})
        return result

    if not items:
        result = {"available": False, "source": "modis", "reason": "no MODIS scenes found for the exact date"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        return result

    try:
        samples, _signed_items, valid_count = await _sample_modis_day(
            items=items,
            points_lonlat=[(lon, lat) for _, lon, lat in grid_points],
            utm_epsg=int(utm_epsg),
            land_geom=land_geom,
        )
    except Exception as exc:
        logger.warning("MODIS availability sampling failed land=%s date=%s: %s", land_id, date_str, exc)
        result = {"available": False, "source": "modis", "reason": f"MODIS sampling failed: {exc}"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        _store_exact_context(land_id, date_str, 60.0, {"modis": {"items": items}})
        return result

    if valid_count <= 0:
        result = {"available": False, "source": "modis", "reason": "no usable MODIS LST pixels found for the exact date"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
        _store_exact_context(land_id, date_str, 60.0, {"modis": {"items": items}})
        return result

    valid_samples = [sample for sample in samples if sample.get("lst_c") is not None]
    result = {
        "available": True,
        "source": "modis",
        "valid_grid_count": len(valid_samples),
        "total_grid_count": len(grid_points),
        "reason": None,
    }
    _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"modis": result}})
    _store_exact_context(land_id, date_str, 60.0, {"modis": {"items": items}})
    return result


async def _check_nasa_power_exact_availability(land_id: int, date_str: str) -> dict[str, Any]:
    cached_result = _get_cached_exact_availability(land_id, date_str, 60.0)
    if cached_result is not None and cached_result.get("sources", {}).get("nasa_power") is not None:
        logger.info("availability cache hit source=nasa_power land_id=%s date=%s", land_id, date_str)
        return cached_result["sources"]["nasa_power"]

    async with async_session() as session:
        res = await session.execute(
            text(
                "SELECT ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
                "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lat "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        row = res.first()

    if not row:
        result = {"available": False, "source": "nasa_power", "reason": "land not found"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"nasa_power": result}})
        return result

    lon, lat = float(row[0]), float(row[1])
    date_compact = date_str.replace("-", "")
    loop = asyncio.get_running_loop()

    try:
        data = await asyncio.wait_for(
            loop.run_in_executor(None, fetch_power_point, lat, lon, date_compact, date_compact),
            timeout=POWER_REQUEST_TIMEOUT_S + 10,
        )
    except asyncio.TimeoutError:
        logger.warning("NASA POWER availability check timed out land=%s date=%s", land_id, date_str)
        result = {"available": False, "source": "nasa_power", "reason": "NASA POWER timed out"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"nasa_power": result}})
        return result
    except Exception as exc:
        logger.exception("NASA POWER fetch failed for land %s date %s", land_id, date_str)
        result = {"available": False, "source": "nasa_power", "reason": f"NASA POWER fetch failed: {exc}"}
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"nasa_power": result}})
        return result

    day_data = data.get(date_str)
    if not day_data or all(day_data.get(k) is None for k in ("t2m", "rh2m", "prectotcorr")):
        result = {
            "available": False,
            "source": "nasa_power",
            "reason": "no NASA POWER data returned for the exact date" if not day_data else "NASA POWER returned only missing values",
        }
        _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"nasa_power": result}})
        _store_exact_context(land_id, date_str, 60.0, {"nasa_power": {"values": data}})
        return result

    result = {"available": True, "source": "nasa_power", "reason": None, "values": day_data}
    _store_cached_exact_availability(land_id, date_str, 60.0, {"sources": {"nasa_power": result}})
    _store_exact_context(land_id, date_str, 60.0, {"nasa_power": {"values": data}})
    return result


async def _check_exact_date_availability(land_id: int, date_str: str, cloud_threshold_pct: float = 60.0) -> dict[str, Any]:
    cached_result = _get_cached_exact_availability(land_id, date_str, cloud_threshold_pct)
    if cached_result is not None and cached_result.get("available") is not None:
        logger.info("availability cache hit land_id=%s date=%s", land_id, date_str)
        return cached_result

    sentinel_task = _check_sentinel2_exact_availability(land_id, date_str, cloud_threshold_pct=cloud_threshold_pct)
    modis_task = _check_modis_exact_availability(land_id, date_str)
    nasa_task = _check_nasa_power_exact_availability(land_id, date_str)

    sentinel, modis, nasa = await asyncio.gather(sentinel_task, modis_task, nasa_task)

    missing_gating = [label for label, result in (("Sentinel-2", sentinel), ("NASA POWER", nasa)) if not result.get("available")]
    missing_advisory = ["MODIS"] if not modis.get("available") else []

    available = len(missing_gating) == 0
    result = {
        "available": available,
        "selected_date": date_str,
        "future_date": False,
        "missing_sources": missing_gating,
        "missing_advisory": missing_advisory,
        "modis_available": modis.get("available", False),
        "sources": {
            "sentinel2": sentinel,
            "modis": modis,
            "nasa_power": nasa,
        },
        "title": "Data Not Available" if not available else None,
        "message": None if available else "Complete dataset not found for selected date.",
        "cloud_threshold_pct": cloud_threshold_pct,
    }
    _store_cached_exact_availability(land_id, date_str, cloud_threshold_pct, result)
    return result


async def _find_latest_exact_available_date(
    land_id: int,
    anchor_date: str,
    *,
    lookback_days: int = 14,
    cloud_threshold_pct: float = 60.0,
) -> str | None:
    async def _search() -> str | None:
        target = datetime.fromisoformat(anchor_date).date()
        for offset in range(0, lookback_days + 1):
            candidate = (target - timedelta(days=offset)).isoformat()
            logger.info("Checking availability for land=%s candidate_date=%s (offset=%d/%d)", land_id, candidate, offset, lookback_days)
            try:
                availability = await _check_exact_date_availability(
                    land_id,
                    candidate,
                    cloud_threshold_pct=cloud_threshold_pct,
                )
            except Exception as exc:
                logger.warning("Availability check failed for land=%s date=%s: %s - skipping", land_id, candidate, exc)
                continue

            if availability.get("available"):
                logger.info("Found available date land=%s date=%s modis_available=%s", land_id, candidate, availability.get("modis_available"))
                return candidate

        return None

    try:
        return await asyncio.wait_for(_search(), timeout=FIND_DATE_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.error("Date search timed out after %ss for land=%s anchor=%s", FIND_DATE_TIMEOUT_S, land_id, anchor_date)
        return None


async def _run_processing_pipeline(land_id: int, date_str: str) -> None:
    logger.info("Processing pipeline started land=%s anchor_date=%s", land_id, date_str)
    try:
        await _set_status(land_id, "running", "finding latest available date")
        analysis_date = await _find_latest_exact_available_date(land_id, date_str)
        if analysis_date is None:
            msg = (
                f"No usable Sentinel-2 + NASA POWER data found in the 14-day lookback window ending {date_str}. "
                "MODIS unavailability does not block analysis."
            )
            logger.error("Pipeline aborted land=%s: %s", land_id, msg)
            await _set_status(land_id, "error", "finding latest available date", msg)
            return

        logger.info("Pipeline using analysis_date=%s for land=%s", analysis_date, land_id)
        await _run_exact_processing_pipeline(
            land_id,
            analysis_date,
            exact_context=_get_exact_context(land_id, analysis_date, 60.0),
        )
        await _set_dashboard_state(land_id, "latest", None)
    except asyncio.CancelledError:
        logger.info("Processing pipeline cancelled land=%s", land_id)
        try:
            await _set_status(land_id, "error", "cancelled", "Processing cancelled (server shutdown).")
        except Exception:
            pass
        raise
    except Exception as exc:
        logger.exception("Processing pipeline failed land=%s", land_id)
        current = await _get_status(land_id)
        await _set_status(land_id, "error", current.get("step", "unknown"), str(exc))


async def _run_exact_processing_pipeline(
    land_id: int,
    date_str: str,
    *,
    exact_context: dict[str, Any] | None = None,
) -> None:
    logger.info("Exact-date pipeline started land=%s date=%s", land_id, date_str)

    try:
        source_errors: list[str] = []
        exact_context = exact_context or _get_exact_context(land_id, date_str, 60.0) or {}

        await _set_status(land_id, "running", "grids")
        async with async_session() as session:
            res = await session.execute(text("SELECT COUNT(*) FROM land_grid_cells WHERE land_id = :lid"), {"lid": land_id})
            grid_count = res.scalar()

        if not grid_count:
            await _set_status(land_id, "running", "generating grids")
            await generate_and_store_grids(land_id, cell_size_m=10.0)

        availability = _get_cached_exact_availability(land_id, date_str, 60.0)
        if availability is None:
            availability = await _check_exact_date_availability(land_id, date_str)

        if not availability.get("available"):
            missing = availability.get("missing_sources", [])
            raise RuntimeError(f"Gating data sources not available for {date_str}: {', '.join(missing)}")

        modis_available_for_date = availability.get("modis_available", False)

        await _set_status(land_id, "running", "sentinel2")
        sentinel_ctx = exact_context.get("sentinel2", {}) if isinstance(exact_context, dict) else {}
        try:
            s2 = await process_sentinel2_for_land_day(
                land_id,
                date_str,
                allow_fallback=False,
                cloud_threshold_pct=60.0,
                preloaded_items=sentinel_ctx.get("items"),
                preferred_item=sentinel_ctx.get("selected_item"),
            )
            if s2.get("processed", 0) == 0:
                raise RuntimeError(s2.get("reason", "Sentinel-2 exact-date processing failed"))
            logger.info("Sentinel-2 done land=%s processed=%d", land_id, s2.get("processed"))
        except Exception as exc:
            s2 = {"processed": 0, "reason": str(exc)}
            source_errors.append(f"Sentinel-2: {exc}")
            logger.error("Sentinel-2 failed land=%s date=%s: %s", land_id, date_str, exc)

        await _set_status(land_id, "running", "modis")
        modis_ctx = exact_context.get("modis", {}) if isinstance(exact_context, dict) else {}
        mod: dict[str, Any] = {"processed": 0, "reason": "MODIS not available for this date"}
        lst_available = False

        if modis_available_for_date:
            try:
                mod = await process_modis_for_land_day(
                    land_id,
                    date_str,
                    allow_fallback=False,
                    preloaded_items=modis_ctx.get("items"),
                )
                if mod.get("processed", 0) == 0:
                    logger.warning(
                        "MODIS returned 0 processed for land=%s date=%s reason=%s - continuing without LST",
                        land_id,
                        date_str,
                        mod.get("reason"),
                    )
                else:
                    lst_available = True
                    logger.info("MODIS done land=%s lst_mean=%s lst_date=%s", land_id, mod.get("lst_mean"), mod.get("lst_date"))
            except Exception as exc:
                logger.warning("MODIS failed land=%s date=%s: %s - continuing without LST", land_id, date_str, exc)
                mod = {"processed": 0, "reason": str(exc)}
        else:
            logger.info("MODIS skipped for land=%s date=%s (not available per availability check)", land_id, date_str)

        await _set_status(land_id, "running", "weather")
        nasa_ctx = exact_context.get("nasa_power", {}) if isinstance(exact_context, dict) else {}
        try:
            wea = await process_weather_for_land(
                land_id,
                date_str,
                date_str,
                preloaded_data=nasa_ctx.get("values"),
            )
            if wea.get("processed", 0) == 0:
                raise RuntimeError(wea.get("reason", "NASA POWER exact-date processing returned 0 rows"))
            logger.info("NASA POWER done land=%s processed=%d", land_id, wea.get("processed"))
        except Exception as exc:
            wea = {"processed": 0, "reason": str(exc)}
            source_errors.append(f"NASA POWER: {exc}")
            logger.error("NASA POWER failed land=%s date=%s: %s", land_id, date_str, exc)

        await _set_status(land_id, "running", "climatology")
        for variable_name in ("ndvi", "ndmi", "lst", "t2m", "rh2m", "prectotcorr"):
            if variable_name in VARIABLE_SOURCES:
                try:
                    await build_climatology_for_variable(land_id, variable_name)
                except Exception as exc:
                    logger.warning("Climatology build error for %s: %s", variable_name, exc)

        await _set_status(land_id, "running", "anomalies")
        s2_date = _safe_s2_date(s2, date_str) if s2.get("processed", 0) > 0 else None
        lst_date = mod.get("lst_date") if lst_available else None
        await _compute_anomalies_for_dates(land_id, _unique_dates(date_str, s2_date, lst_date))

        await _set_status(land_id, "running", "risk")
        try:
            await compute_risk_for_land_date(land_id, date_str)
            logger.info("Risk computed land=%s date=%s", land_id, date_str)
        except Exception as exc:
            source_errors.append(f"Risk: {exc}")
            logger.error("Risk computation failed land=%s date=%s: %s", land_id, date_str, exc)

        await _set_dashboard_state(land_id, "select", date_str)

        if source_errors:
            await _set_status(land_id, "error", "partial", "; ".join(source_errors))
            logger.warning(
                "Pipeline completed with errors land=%s date=%s errors=%s lst_available=%s",
                land_id,
                date_str,
                source_errors,
                lst_available,
            )
        else:
            await _set_status(land_id, "done", "complete" if lst_available else "complete_no_lst")
            logger.info("Pipeline complete land=%s date=%s lst_available=%s", land_id, date_str, lst_available)
    except asyncio.CancelledError:
        logger.info("Exact-date pipeline cancelled land=%s", land_id)
        try:
            await _set_status(land_id, "error", "cancelled", "Processing cancelled (server shutdown).")
        except Exception:
            pass
        raise
    except Exception as exc:
        logger.exception("Exact-date pipeline failed land=%s date=%s", land_id, date_str)
        current = await _get_status(land_id)
        await _set_status(land_id, "error", current.get("step", "unknown"), str(exc))


class ComputeDayRequest(BaseModel):
    land_id: int
    date: str


class ProcessSelectedRequest(BaseModel):
    date: str


@router.get("/risk")
async def get_risk(land_id: int, date: str):
    try:
        return await compute_risk_for_land_date(land_id, date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/compute_day")
async def compute_day(req: ComputeDayRequest):
    s2 = await process_sentinel2_for_land_day(req.land_id, req.date)
    mod = await process_modis_for_land_day(req.land_id, req.date)
    wea = await process_weather_for_land(req.land_id, req.date, req.date)

    for variable_name in ("ndvi", "ndmi", "lst", "t2m", "rh2m", "prectotcorr"):
        if variable_name in VARIABLE_SOURCES:
            try:
                await build_climatology_for_variable(req.land_id, variable_name)
            except Exception:
                pass

    s2_date = _safe_s2_date(s2, req.date)
    lst_date = mod.get("lst_date") or None
    all_dates = _unique_dates(req.date, s2_date, lst_date)
    await _compute_anomalies_for_dates(req.land_id, all_dates)

    risk = await compute_risk_for_land_date(req.land_id, req.date)

    return {
        "sentinel2": s2,
        "modis": mod,
        "weather": wea,
        "anomalies": {"dates_processed": all_dates},
        "risk": risk,
        "lst_available": lst_date is not None,
        "lst_missing_reason": None if lst_date else "MODIS did not return LST for this date",
    }


@router.post("/{land_id}/process")
async def process_land(land_id: int, background_tasks: BackgroundTasks):
    async with async_session() as session:
        res = await session.execute(text("SELECT land_id FROM lands WHERE land_id = :lid"), {"lid": land_id})
        if not res.first():
            raise HTTPException(status_code=404, detail="Land not found")

    target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    await _set_status(land_id, "queued", "pending")
    background_tasks.add_task(_run_processing_pipeline, land_id, target_date)
    return {
        "land_id": land_id,
        "date": target_date,
        "status": "processing",
        "message": "Pipeline started. Poll GET /dashboard/{land_id}/status for results.",
    }


@router.post("/{land_id}/process-selected")
async def process_selected(land_id: int, req: ProcessSelectedRequest, background_tasks: BackgroundTasks):
    async with async_session() as session:
        res = await session.execute(text("SELECT land_id FROM lands WHERE land_id = :lid"), {"lid": land_id})
        if not res.first():
            raise HTTPException(status_code=404, detail="Land not found")

    try:
        selected_date = datetime.fromisoformat(req.date).date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if selected_date > datetime.utcnow().date():
        raise HTTPException(status_code=400, detail="Future dates are not allowed")

    availability = await _check_exact_date_availability(land_id, req.date)
    if not availability.get("available"):
        raise HTTPException(status_code=409, detail=availability)

    await _set_status(land_id, "queued", "pending")
    background_tasks.add_task(_run_exact_processing_pipeline, land_id, req.date)
    return {
        "land_id": land_id,
        "date": req.date,
        "status": "processing",
        "mode": "select",
        "message": "Strict exact-date pipeline started.",
    }


@router.get("/{land_id}/availability")
async def get_availability(land_id: int, date: str, cloud_threshold_pct: float = 60.0):
    try:
        selected_date = datetime.fromisoformat(date).date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    if selected_date > datetime.utcnow().date():
        return {
            "available": False,
            "selected_date": date,
            "future_date": True,
            "missing_sources": ["Sentinel-2", "MODIS", "NASA POWER"],
            "sources": {},
            "title": "Data Not Available",
            "message": "Future dates are not available for analysis.",
            "cloud_threshold_pct": cloud_threshold_pct,
        }

    return await _check_exact_date_availability(land_id, date, cloud_threshold_pct=cloud_threshold_pct)


@router.get("/{land_id}/status")
async def get_processing_status(land_id: int):
    status = await _get_status(land_id)
    return {"land_id": land_id, **status}


def _select_active_date(mode: str, selected_date: str | None, latest_complete_date: str | None, latest_date: str | None) -> str | None:
    if mode == "select" and selected_date:
        return selected_date
    return latest_complete_date or latest_date or selected_date


def _build_summary(features: list[dict[str, Any]]) -> dict[str, Any]:
    non_water = [feature for feature in features if not feature["properties"].get("is_water")]
    return {
        "grid_count": len(features),
        "ndvi": _stats([feature["properties"].get("ndvi") for feature in non_water]),
        "ndmi": _stats([feature["properties"].get("ndmi") for feature in non_water]),
        "lst": _stats([feature["properties"].get("lst_c") for feature in non_water]),
        "risk": _stats([feature["properties"].get("risk") for feature in non_water]),
    }


def _build_feature(record: dict[str, Any], ndvi_bounds: tuple[float | None, float | None], ndmi_bounds: tuple[float | None, float | None], lst_bounds: tuple[float | None, float | None]) -> dict[str, Any]:
    idx_data = record["idx_data"]
    lst_data = record["lst_data"]
    risk_data = record["risk_data"]
    anomaly_data = record["anomaly_data"]

    ndvi = _to_float(idx_data.get("ndvi"))
    ndmi = _to_float(idx_data.get("ndmi"))
    lst_c = _to_float(lst_data.get("lst_c"))
    risk = _to_float(risk_data.get("probability"))

    return {
        "type": "Feature",
        "properties": {
            "grid_id": record["public_grid_id"],
            "internal_grid_key": record["internal_grid_id"],
            "row": record["row_idx"],
            "col": record["col_idx"],
            "is_water": bool(record["is_water"]),
            "b04": _to_float(idx_data.get("b04")),
            "b08": _to_float(idx_data.get("b08")),
            "b11": _to_float(idx_data.get("b11")),
            "stac_item_id": idx_data.get("stac_item_id"),
            "acquisition_datetime": _to_iso_string(idx_data.get("acquisition_datetime")),
            "tile_id": idx_data.get("tile_id"),
            "cloud_coverage_pct": _to_float(idx_data.get("cloud_cover_pct")),
            "ndvi": ndvi,
            "ndmi": ndmi,
            "lst_c": lst_c,
            "pixel_count": idx_data.get("pixel_count"),
            "ndvi_norm": _normalize(ndvi, *ndvi_bounds),
            "ndmi_norm": _normalize(ndmi, *ndmi_bounds),
            "lst_norm": _normalize(lst_c, *lst_bounds),
            "risk": risk,
            "color": {
                "ndvi": _ndvi_color(ndvi),
                "ndmi": _ndmi_color(ndmi),
                "lst": _lst_color(lst_c),
            },
            "anomalies": anomaly_data or None,
        },
        "geometry": record["geometry"],
    }


@router.get("/{land_id}")
async def get_dashboard(land_id: int):
    async with async_session() as session:
        land_res = await session.execute(
            text(
                "SELECT land_id, farmer_name, crop_type, ST_AsGeoJSON(geom) AS geojson, area_sqm, created_at "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        land_row = land_res.first()
        if not land_row:
            raise HTTPException(status_code=404, detail="Land not found")

        grid_res = await session.execute(
            text(
                "SELECT grid_id, grid_num, row_idx, col_idx, ST_AsGeoJSON(geom) AS geojson, COALESCE(is_water, FALSE) AS is_water "
                "FROM land_grid_cells WHERE land_id = :lid ORDER BY COALESCE(grid_num, 2147483647), grid_id"
            ),
            {"lid": land_id},
        )
        grid_rows = grid_res.fetchall()

        dashboard_state = await _get_dashboard_state(land_id)
        latest_complete_date = await _get_latest_complete_date(land_id)
        latest_date = await _get_latest_satellite_date(land_id)
        active_data_date = _select_active_date(dashboard_state["mode"], dashboard_state["selected_date"], latest_complete_date, latest_date)

        active_date_obj = datetime.fromisoformat(active_data_date).date() if active_data_date else None

        idx_rows: list[tuple[Any, ...]] = []
        lst_rows: list[tuple[Any, ...]] = []
        risk_rows: list[tuple[Any, ...]] = []
        anomaly_rows: list[tuple[Any, ...]] = []
        provenance_row = None

        if active_date_obj is not None:
            indices_res = await session.execute(
                text(
                    "SELECT grid_id, date, b04, b08, b11, ndvi, ndmi, pixel_count, stac_item_id, acquisition_datetime, tile_id, cloud_cover_pct "
                    "FROM land_daily_indices WHERE land_id = :lid AND date = :active_date ORDER BY grid_id"
                ),
                {"lid": land_id, "active_date": active_date_obj},
            )
            idx_rows = indices_res.fetchall()

            provenance_res = await session.execute(
                text(
                    "SELECT date, stac_item_id, acquisition_datetime, tile_id, cloud_cover_pct "
                    "FROM land_daily_indices WHERE land_id = :lid AND date = :active_date AND stac_item_id IS NOT NULL "
                    "ORDER BY grid_id LIMIT 1"
                ),
                {"lid": land_id, "active_date": active_date_obj},
            )
            provenance_row = provenance_res.first()

            lst_res = await session.execute(
                text("SELECT grid_id, date, lst_c FROM land_daily_lst WHERE land_id = :lid AND date = :active_date ORDER BY grid_id"),
                {"lid": land_id, "active_date": active_date_obj},
            )
            lst_rows = lst_res.fetchall()

            risk_res = await session.execute(
                text("SELECT grid_id, date, probability FROM stress_risk_forecast WHERE land_id = :lid AND date = :active_date ORDER BY grid_id"),
                {"lid": land_id, "active_date": active_date_obj},
            )
            risk_rows = risk_res.fetchall()

            anomaly_res = await session.execute(
                text(
                    "SELECT grid_id, variable, zscore, value FROM land_anomalies "
                    "WHERE land_id = :lid AND date = :active_date AND grid_id != '__land__'"
                ),
                {"lid": land_id, "active_date": active_date_obj},
            )
            anomaly_rows = anomaly_res.fetchall()

        weather_res = await session.execute(
            text("SELECT date, t2m, rh2m, prectotcorr FROM land_daily_weather WHERE land_id = :lid ORDER BY date DESC LIMIT 7"),
            {"lid": land_id},
        )
        weather_rows = list(reversed(weather_res.fetchall()))

    land_info = {
        "land_id": land_row[0],
        "farmer_name": land_row[1],
        "crop_type": land_row[2],
        "geometry": geometry_geojson_storage_to_api(json.loads(land_row[3])) if land_row[3] else None,
        "area_sqm": land_row[4],
        "created_at": _to_iso_string(land_row[5]),
    }

    idx_by_grid: dict[str, dict[str, Any]] = {}
    lst_by_grid: dict[str, dict[str, Any]] = {}
    risk_by_grid: dict[str, dict[str, Any]] = {}
    anomaly_by_grid: dict[str, dict[str, Any]] = {}

    for row in idx_rows:
        idx_by_grid[str(row[0])] = {
            "date": _to_iso_string(row[1]),
            "b04": row[2],
            "b08": row[3],
            "b11": row[4],
            "ndvi": row[5],
            "ndmi": row[6],
            "pixel_count": row[7],
            "stac_item_id": row[8],
            "acquisition_datetime": row[9],
            "tile_id": row[10],
            "cloud_cover_pct": row[11],
        }

    for row in lst_rows:
        lst_by_grid[str(row[0])] = {"date": _to_iso_string(row[1]), "lst_c": row[2]}

    for row in risk_rows:
        risk_by_grid[str(row[0])] = {"date": _to_iso_string(row[1]), "probability": row[2]}

    for row in anomaly_rows:
        anomaly_by_grid.setdefault(str(row[0]), {})[str(row[1])] = {"zscore": row[2], "value": row[3]}

    records: list[dict[str, Any]] = []
    for idx, row in enumerate(grid_rows, start=1):
        records.append(
            {
                "internal_grid_id": str(row[0]),
                "public_grid_id": int(row[1]) if row[1] is not None else idx,
                "row_idx": row[2],
                "col_idx": row[3],
                "geometry": geometry_geojson_storage_to_api(json.loads(row[4])),
                "is_water": row[5],
                "idx_data": idx_by_grid.get(str(row[0]), {}),
                "lst_data": lst_by_grid.get(str(row[0]), {}),
                "risk_data": risk_by_grid.get(str(row[0]), {}),
                "anomaly_data": anomaly_by_grid.get(str(row[0]), {}),
            }
        )

    ndvi_bounds = _bounds([_to_float(record["idx_data"].get("ndvi")) for record in records])
    ndmi_bounds = _bounds([_to_float(record["idx_data"].get("ndmi")) for record in records])
    lst_bounds = _bounds([_to_float(record["lst_data"].get("lst_c")) for record in records])

    features = [_build_feature(record, ndvi_bounds, ndmi_bounds, lst_bounds) for record in records]
    summary = _build_summary(features)
    weather = [{"date": _to_iso_string(row[0]), "t2m": row[1], "rh2m": row[2], "prectotcorr": row[3]} for row in weather_rows]

    provenance = None
    if provenance_row:
        provenance = {
            "satellite_source": "Sentinel-2 L2A",
            "acquisition_date": _to_iso_string(provenance_row[0]),
            "acquisition_datetime": _to_iso_string(provenance_row[2]),
            "stac_item_id": provenance_row[1],
            "tile_id": provenance_row[3],
            "cloud_coverage_pct": provenance_row[4],
        }

    processing = await _get_status(land_id)

    return {
        "land": land_info,
        "grids": {
            "type": "FeatureCollection",
            "features": features,
        },
        "latest_date": latest_date,
        "latest_complete_date": latest_complete_date,
        "mode": dashboard_state["mode"],
        "selected_date": dashboard_state["selected_date"],
        "active_data_date": active_data_date,
        "provenance": provenance,
        "summary": summary,
        "weather": weather,
        "processing": processing,
        "color_scales": COLOR_SCALES,
    }