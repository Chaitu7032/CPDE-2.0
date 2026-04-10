"""
dashboard.py — CPDE Dashboard API
===================================
FIXES APPLIED:
  Bug 1 — In-memory _processing_status replaced with DB-backed processing_jobs table
  Bug 2 — LST fallback date removed; missing LST no longer contaminates anomaly/risk
  Bug 3 — get_risk land_id type corrected to int (was str)
  Bug 4 — Silent LST None surfaced to frontend with lst_available flag + reason
  Bug 5 — Unsafe s2_date slicing made safe with length check
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from shapely.geometry import shape

from backend.pipelines.anomaly import build_climatology_for_variable, compute_anomalies_for_date, VARIABLE_SOURCES
from backend.pipelines.risk import compute_risk_for_land_date
from backend.pipelines.sentinel2 import (
    PC_STAC_API,
    _compute_indices_for_points,
    _extract_cloud_cover,
    _extract_item_datetime,
    _extract_tile_id,
    _item_sort_key,
    process_sentinel2_for_land_day,
)
from backend.pipelines.modis import (
    DEFAULT_MODIS_STAC_COLLECTION,
    _sample_modis_day,
    process_modis_for_land_day,
)
from backend.pipelines.nasa_power import fetch_power_point, process_weather_for_land
from backend.pipelines.grid_generation import generate_and_store_grids
from backend.utils.crs import STORAGE_CRS_EPSG, geometry_geojson_storage_to_api

from backend.db.connection import async_session
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


# ══════════════════════════════════════════════════════════════════════════
# DB-BACKED PROCESSING STATUS  (Fix for Bug 1)
# Replaces the old in-memory dict which was lost on every restart.
#
# Existing databases that predate this change should run the migration once.
# Fresh installs create the table automatically via backend.db.init_tables.
#
#   CREATE TABLE IF NOT EXISTS processing_jobs (
#       land_id     INTEGER PRIMARY KEY,
#       status      TEXT    NOT NULL DEFAULT 'unknown',
#       step        TEXT,
#       error       TEXT,
#       updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
#   );
#   CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs (status);
#
# ══════════════════════════════════════════════════════════════════════════

async def _set_status(land_id: int, status: str, step: str, error: str | None = None) -> None:
    """Upsert processing status for a land parcel into the DB."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO processing_jobs (land_id, status, step, error, updated_at)
                VALUES (:lid, :status, :step, :error, now())
                ON CONFLICT (land_id) DO UPDATE
                    SET status     = EXCLUDED.status,
                        step       = EXCLUDED.step,
                        error      = EXCLUDED.error,
                        updated_at = EXCLUDED.updated_at
            """),
            {"lid": land_id, "status": status, "step": step, "error": error},
        )
        await session.commit()


async def _get_status(land_id: int) -> dict:
    """Read processing status for a land parcel from the DB."""
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


async def _get_dashboard_state(land_id: int) -> dict:
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
    """Return the newest date that exists in all persisted source tables."""
    async with async_session() as session:
        res = await session.execute(
            text(
                "WITH grid_counts AS ("
                "  SELECT "
                "    COUNT(*) AS total_grids, "
                "    COUNT(*) FILTER (WHERE COALESCE(is_water, FALSE) = FALSE) AS non_water_grids "
                "  FROM land_grid_cells WHERE land_id = :lid"
                "), s2_dates AS ("
                "  SELECT date FROM land_daily_indices "
                "  WHERE land_id = :lid "
                "  GROUP BY date "
                "  HAVING COUNT(DISTINCT grid_id) = (SELECT total_grids FROM grid_counts)"
                "), modis_dates AS ("
                "  SELECT date FROM land_daily_lst "
                "  WHERE land_id = :lid "
                "  GROUP BY date "
                "  HAVING COUNT(DISTINCT grid_id) = (SELECT non_water_grids FROM grid_counts)"
                "), weather_dates AS ("
                "  SELECT date FROM land_daily_weather WHERE land_id = :lid GROUP BY date"
                ") "
                "SELECT MAX(date) FROM ("
                "  SELECT date FROM s2_dates "
                "  INTERSECT SELECT date FROM modis_dates "
                "  INTERSECT SELECT date FROM weather_dates"
                ") q"
            ),
            {"lid": land_id},
        )
        row = res.first()
    return str(row[0]) if row and row[0] else None


async def _load_land_context(land_id: int, *, non_water_only: bool = False) -> tuple[object, list[tuple[object, float, float]], int]:
    async with async_session() as session:
        land_res = await session.execute(
            text(
                "SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)), COALESCE(utm_epsg, :fallback_epsg) "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id, "fallback_epsg": int(STORAGE_CRS_EPSG)},
        )
        land_row = land_res.first()
        if not land_row or not land_row[0]:
            raise HTTPException(status_code=404, detail="Land not found")

        grid_sql = (
            "SELECT grid_id, "
            "ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) AS lon, "
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


async def _check_sentinel2_exact_availability(land_id: int, date_str: str, cloud_threshold_pct: float = 60.0) -> dict:
    try:
        land_geom, grid_points, _ = await _load_land_context(land_id)
    except HTTPException:
        raise

    if not grid_points:
        return {
            "available": False,
            "source": "sentinel2",
            "reason": "no grids available for this land",
        }

    from pystac_client import Client  # type: ignore

    client = Client.open(PC_STAC_API)
    target = datetime.fromisoformat(date_str).date()
    dt = f"{target.strftime('%Y-%m-%d')}T00:00:00Z/{target.strftime('%Y-%m-%d')}T23:59:59Z"
    search = client.search(
        collections=["sentinel-2-l2a"],
        intersects=land_geom.__geo_interface__,
        datetime=dt,
        max_items=50,
    )
    items = [it for it in search.items() if hasattr(it, "assets") and all(k in it.assets for k in ("B04", "B08", "B11", "SCL"))]
    if not items:
        return {
            "available": False,
            "source": "sentinel2",
            "reason": "no Sentinel-2 scenes found for the exact date",
        }

    points_lonlat = [(lon, lat) for _, lon, lat in grid_points]
    last_error: str | None = None
    for candidate in sorted(items, key=_item_sort_key, reverse=True):
        cloud_cover = _extract_cloud_cover(candidate)
        if cloud_cover is None:
            continue
        if cloud_threshold_pct is not None and cloud_cover > cloud_threshold_pct:
            continue

        try:
            candidate_results = await asyncio.to_thread(_compute_indices_for_points, candidate, points_lonlat)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("Sentinel-2 availability sampling failed for land %s item=%s", land_id, getattr(candidate, "id", None))
            continue

        usable_count = sum(1 for result in candidate_results if result.get("pixel_count", 0) > 0)
        if usable_count > 0:
            item_dt = _extract_item_datetime(candidate)
            return {
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

    result = {
        "available": False,
        "source": "sentinel2",
        "reason": "no usable Sentinel-2 pixels found for the exact date",
    }

    if last_error:
        result["reason"] = f"Sentinel-2 reprojection/sampling failed: {last_error}"
    return result


async def _check_modis_exact_availability(land_id: int, date_str: str) -> dict:
    try:
        land_geom, grid_points, utm_epsg = await _load_land_context(land_id, non_water_only=True)
    except HTTPException:
        raise

    if not grid_points:
        return {
            "available": False,
            "source": "modis",
            "reason": "no non-water grids available for this land",
        }

    from pystac_client import Client  # type: ignore

    client = Client.open(PC_STAC_API)
    target = datetime.fromisoformat(date_str).date()
    dt = f"{target.strftime('%Y-%m-%d')}T00:00:00Z/{target.strftime('%Y-%m-%d')}T23:59:59Z"
    search = client.search(
        collections=[DEFAULT_MODIS_STAC_COLLECTION],
        intersects=land_geom.__geo_interface__,
        datetime=dt,
        max_items=200,
    )
    items = list(search.items())
    items = [it for it in items if hasattr(it, "assets") and "LST_Day_1km" in it.assets]
    if not items:
        return {
            "available": False,
            "source": "modis",
            "reason": "no MODIS scenes found for the exact date",
        }

    try:
        samples, _signed_items, valid_count = await _sample_modis_day(
            items=items,
            points_lonlat=[(lon, lat) for _, lon, lat in grid_points],
            utm_epsg=int(utm_epsg),
            land_geom=land_geom,
        )
    except Exception as exc:
        logger.exception("MODIS availability sampling failed for land %s date %s", land_id, date_str)
        return {
            "available": False,
            "source": "modis",
            "reason": f"MODIS reprojection/sampling failed: {exc}",
        }
    if valid_count <= 0:
        return {
            "available": False,
            "source": "modis",
            "reason": "no usable MODIS LST pixels found for the exact date",
        }

    valid_samples = [sample for sample in samples if sample.get("lst_c") is not None]
    return {
        "available": True,
        "source": "modis",
        "valid_grid_count": len(valid_samples),
        "total_grid_count": len(grid_points),
        "reason": None,
    }


async def _check_nasa_power_exact_availability(land_id: int, date_str: str) -> dict:
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
        return {
            "available": False,
            "source": "nasa_power",
            "reason": "land not found",
        }

    lon, lat = float(row[0]), float(row[1])
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_power_point, lat, lon, date_str.replace("-", ""), date_str.replace("-", ""))
    day_data = data.get(date_str)
    if not day_data:
        return {
            "available": False,
            "source": "nasa_power",
            "reason": "no NASA POWER data returned for the exact date",
        }

    if all(day_data.get(key) is None for key in ("t2m", "rh2m", "prectotcorr")):
        return {
            "available": False,
            "source": "nasa_power",
            "reason": "NASA POWER returned only missing values for the exact date",
        }

    return {
        "available": True,
        "source": "nasa_power",
        "reason": None,
        "values": day_data,
    }


async def _check_exact_date_availability(land_id: int, date_str: str, cloud_threshold_pct: float = 60.0) -> dict:
    sentinel_task = _check_sentinel2_exact_availability(land_id, date_str, cloud_threshold_pct=cloud_threshold_pct)
    modis_task = _check_modis_exact_availability(land_id, date_str)
    nasa_task = _check_nasa_power_exact_availability(land_id, date_str)
    sentinel, modis, nasa = await asyncio.gather(sentinel_task, modis_task, nasa_task)

    missing_sources = []
    for label, result in (("Sentinel-2", sentinel), ("MODIS", modis), ("NASA POWER", nasa)):
        if not result.get("available"):
            missing_sources.append(label)

    available = len(missing_sources) == 0
    return {
        "available": available,
        "selected_date": date_str,
        "future_date": False,
        "missing_sources": missing_sources,
        "sources": {
            "sentinel2": sentinel,
            "modis": modis,
            "nasa_power": nasa,
        },
        "title": "Data Not Available" if not available else None,
        "message": None if available else "Complete dataset not found for selected date.",
        "cloud_threshold_pct": cloud_threshold_pct,
    }


async def _find_latest_exact_available_date(
    land_id: int,
    anchor_date: str,
    *,
    lookback_days: int = 14,
    cloud_threshold_pct: float = 60.0,
) -> str | None:
    """Find the most recent date before anchor_date that passes exact availability checks."""
    target = datetime.fromisoformat(anchor_date).date()
    for offset in range(0, lookback_days + 1):
        candidate = (target - timedelta(days=offset)).isoformat()
        availability = await _check_exact_date_availability(
            land_id,
            candidate,
            cloud_threshold_pct=cloud_threshold_pct,
        )
        if availability.get("available"):
            return candidate
    return None


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

NO_DATA_COLOR = "#808080"

# Scientifically defined color scales for crop-stress interpretation.
NDVI_COLOR_SCALE = [
    {"range": "< 0.2", "label": "Severe stress", "color": "#7f1d1d"},
    {"range": "0.2 - 0.4", "label": "Stressed", "color": "#f97316"},
    {"range": "0.4 - 0.6", "label": "Moderate", "color": "#facc15"},
    {"range": ">= 0.6", "label": "Healthy", "color": "#16a34a"},
]

NDMI_COLOR_SCALE = [
    {"range": "< -0.1", "label": "Dry", "color": "#dc2626"},
    {"range": "-0.1 - 0", "label": "Slightly dry", "color": "#f59e0b"},
    {"range": "0 - 0.2", "label": "Moderate", "color": "#7dd3fc"},
    {"range": ">= 0.2", "label": "Wet", "color": "#2563eb"},
]

LST_COLOR_SCALE = [
    {"range": "< 25", "label": "Cool", "color": "#2563eb"},
    {"range": "25 - 30", "label": "Normal", "color": "#16a34a"},
    {"range": "30 - 35", "label": "Warm", "color": "#f59e0b"},
    {"range": ">= 35", "label": "Hot stress", "color": "#dc2626"},
]


def _ndvi_color(v: float | None) -> str:
    if v is None:
        return NO_DATA_COLOR
    if v < 0.2:
        return "#7f1d1d"
    if v < 0.4:
        return "#f97316"
    if v < 0.6:
        return "#facc15"
    return "#16a34a"


def _ndmi_color(v: float | None) -> str:
    if v is None:
        return NO_DATA_COLOR
    if v < -0.1:
        return "#dc2626"
    if v < 0.0:
        return "#f59e0b"
    if v < 0.2:
        return "#7dd3fc"
    return "#2563eb"


def _lst_color(v: float | None) -> str:
    if v is None:
        return NO_DATA_COLOR
    if v < 25.0:
        return "#2563eb"
    if v < 30.0:
        return "#16a34a"
    if v < 35.0:
        return "#f59e0b"
    return "#dc2626"

def _safe_s2_date(s2: dict, fallback: str) -> str:
    """
    Extract YYYY-MM-DD from s2['datetime'].
    Fix for Bug 5 — old code did (s2.get("datetime") or "")[:10] which
    would silently return a truncated garbage string (e.g. "erro") if the
    value was unexpected.  Now we validate length before slicing.
    """
    raw = s2.get("datetime") or ""
    if len(raw) >= 10:
        return raw[:10]
    return fallback


async def _compute_anomalies_for_dates(land_id: int, dates: list[str]) -> None:
    """Compute anomalies for all unique non-None dates in the list."""
    seen: set[str] = set()
    for d in dates:
        if d and d not in seen:
            seen.add(d)
            try:
                await compute_anomalies_for_date(
                    land_id, d,
                    variables=["ndvi", "ndmi", "lst", "t2m", "prectotcorr", "rh2m"],
                )
            except Exception as e:
                logger.warning("Anomaly compute error for date %s: %s", d, e)


# ══════════════════════════════════════════════════════════════════════════
# ONE-SHOT DAILY PIPELINE  POST /dashboard/compute_day
# ══════════════════════════════════════════════════════════════════════════

class ComputeDayRequest(BaseModel):
    land_id: int
    date: str  # YYYY-MM-DD


@router.post("/compute_day")
async def compute_day(req: ComputeDayRequest):
    """One-shot daily pipeline: Sentinel-2 + MODIS + POWER + anomalies + risk."""

    # Step 1: Ingest authoritative sources so climatology sees new data.
    s2  = await process_sentinel2_for_land_day(req.land_id, req.date)
    mod = await process_modis_for_land_day(req.land_id, req.date)
    wea = await process_weather_for_land(req.land_id, req.date, req.date)

    # Step 2: Rebuild climatology after ingestion.
    for v in ("ndvi", "ndmi", "lst", "t2m", "prectotcorr"):
        if v in VARIABLE_SOURCES:
            try:
                await build_climatology_for_variable(req.land_id, v)
            except Exception:
                pass

    # Step 3: Compute anomalies only for dates where data actually exists.
    # Bug 2 fix — only add lst_date to the set when MODIS actually returned one.
    # Old code: lst_date = mod.get("lst_date") or req.date  ← always added req.date
    # New code: lst_date is None when MODIS failed, and None is filtered out below.
    s2_date  = _safe_s2_date(s2, req.date)           # Bug 5 fix
    lst_date = mod.get("lst_date") or None            # Bug 2 fix: no req.date fallback

    all_dates = list(filter(None, {req.date, s2_date, lst_date}))
    await _compute_anomalies_for_dates(req.land_id, all_dates)

    # Step 4: Compute risk.
    risk = await compute_risk_for_land_date(req.land_id, req.date)

    return {
        "sentinel2":  s2,
        "modis":      mod,
        "weather":    wea,
        "anomalies":  {"dates_processed": all_dates},
        "risk":       risk,
        # Surface LST availability so callers know whether the risk score is partial.
        "lst_available": lst_date is not None,
        "lst_missing_reason": None if lst_date else "MODIS did not return LST for this date",
    }


# ══════════════════════════════════════════════════════════════════════════
# RISK ENDPOINT  GET /dashboard/risk
# ══════════════════════════════════════════════════════════════════════════

@router.get("/risk")
async def get_risk(land_id: int, date: str):   # Bug 3 fix: was land_id: str
    """Return risk score for a land parcel on a specific date."""
    try:
        return await compute_risk_for_land_date(land_id, date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════
# FULL BACKGROUND PIPELINE  POST /dashboard/{land_id}/process
# ══════════════════════════════════════════════════════════════════════════

async def _run_processing_pipeline(land_id: int, date_str: str) -> None:
    """Background task: grids → Sentinel-2 → MODIS → NASA POWER → anomalies → risk."""
    try:
        await _set_status(land_id, "running", "finding latest available date")
        analysis_date = await _find_latest_exact_available_date(land_id, date_str)
        if analysis_date is None:
            raise RuntimeError("No exact-date dataset found within the lookback window")

        await _run_exact_processing_pipeline(land_id, analysis_date)
        await _set_dashboard_state(land_id, "latest", None)

    except Exception as e:
        logger.exception("Processing pipeline failed for land %s", land_id)
        current = await _get_status(land_id)
        await _set_status(land_id, "error", current.get("step", "unknown"), str(e))


async def _run_exact_processing_pipeline(land_id: int, date_str: str) -> None:
    """Strict background pipeline that only uses the exact selected date."""
    try:
        await _set_status(land_id, "running", "grids")

        async with async_session() as session:
            res = await session.execute(
                text("SELECT COUNT(*) FROM land_grid_cells WHERE land_id = :lid"),
                {"lid": land_id},
            )
            grid_count = res.scalar()

        if not grid_count:
            await _set_status(land_id, "running", "generating grids")
            await generate_and_store_grids(land_id, cell_size_m=10.0)

        availability = await _check_exact_date_availability(land_id, date_str)
        if not availability.get("available"):
            raise RuntimeError(
                f"Data not available for selected date: {', '.join(availability.get('missing_sources', []))}"
            )

        await _set_status(land_id, "running", "sentinel2")
        s2 = await process_sentinel2_for_land_day(land_id, date_str, allow_fallback=False, cloud_threshold_pct=60.0)
        if s2.get("processed", 0) == 0:
            raise RuntimeError(s2.get("reason", "Sentinel-2 exact-date processing failed"))

        await _set_status(land_id, "running", "modis")
        mod = await process_modis_for_land_day(land_id, date_str, allow_fallback=False)
        if mod.get("processed", 0) == 0:
            raise RuntimeError(mod.get("reason", "MODIS exact-date processing failed"))

        await _set_status(land_id, "running", "weather")
        wea = await process_weather_for_land(land_id, date_str, date_str)
        if wea.get("processed", 0) == 0:
            raise RuntimeError(wea.get("reason", "NASA POWER exact-date processing failed"))

        await _set_status(land_id, "running", "climatology")
        for v in ("ndvi", "ndmi", "lst", "t2m", "prectotcorr"):
            if v in VARIABLE_SOURCES:
                try:
                    await build_climatology_for_variable(land_id, v)
                except Exception as e:
                    logger.warning("Climatology build error for %s: %s", v, e)

        await _set_status(land_id, "running", "anomalies")
        s2_date = _safe_s2_date(s2, date_str)
        lst_date = mod.get("lst_date") or None
        all_dates = list(filter(None, {date_str, s2_date, lst_date}))
        await _compute_anomalies_for_dates(land_id, all_dates)

        await _set_status(land_id, "running", "risk")
        await compute_risk_for_land_date(land_id, date_str)

        await _set_dashboard_state(land_id, "select", date_str)
        await _set_status(land_id, "done", "complete")

    except Exception as e:
        logger.exception("Exact-date processing pipeline failed for land %s", land_id)
        current = await _get_status(land_id)
        await _set_status(land_id, "error", current.get("step", "unknown"), str(e))


@router.post("/{land_id}/process")
async def process_land(land_id: int, background_tasks: BackgroundTasks):
    """Trigger the full processing pipeline for a land parcel."""
    async with async_session() as session:
        res = await session.execute(
            text("SELECT land_id FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        if not res.first():
            raise HTTPException(status_code=404, detail="Land not found")

    target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    await _set_status(land_id, "queued", "pending")   # Bug 1 fix: DB write
    background_tasks.add_task(_run_processing_pipeline, land_id, target_date)

    return {
        "land_id": land_id,
        "date":    target_date,
        "status":  "processing",
        "message": "Pipeline started. Poll GET /dashboard/{land_id}/status for results.",
    }


class ProcessSelectedRequest(BaseModel):
    date: str


@router.post("/{land_id}/process-selected")
async def process_selected(land_id: int, req: ProcessSelectedRequest, background_tasks: BackgroundTasks):
    """Trigger the strict exact-date pipeline for user-selected analysis."""
    async with async_session() as session:
        res = await session.execute(
            text("SELECT land_id FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
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
    """Check exact-date availability for Sentinel-2, MODIS, and NASA POWER."""
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
    """Check the processing pipeline status for a land parcel."""
    status = await _get_status(land_id)   # Bug 1 fix: reads from DB
    return {"land_id": land_id, **status}


# ══════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD  GET /dashboard/{land_id}
# ══════════════════════════════════════════════════════════════════════════

@router.get("/{land_id}")
async def get_dashboard(land_id: int):
    """Return all dashboard data for a land parcel.

    Includes:
    - Land metadata + geometry (GeoJSON)
    - Grid geometries as FeatureCollection
    - Per-grid latest indices (NDVI, NDMI, LST)
    - Per-grid stress/risk scores (0–1 normalized)
    - Land-level weather data
    - Processing status
    - LST availability flag + reason (Bug 4 fix)
    """
    async with async_session() as session:

        # ── Land info ──────────────────────────────────────────────────────
        land_res = await session.execute(
            text(
                "SELECT land_id, farmer_name, crop_type, ST_AsGeoJSON(geom) as geojson, area_sqm, created_at "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        land_row = land_res.first()
        if not land_row:
            raise HTTPException(status_code=404, detail="Land not found")

        land_info = {
            "land_id":      land_row[0],
            "farmer_name":  land_row[1],
            "crop_type":    land_row[2],
            "geometry":     geometry_geojson_storage_to_api(json.loads(land_row[3])) if land_row[3] else None,
            "area_sqm":     land_row[4],
            "created_at":   str(land_row[5]) if land_row[5] else None,
        }

        # ── Grids ──────────────────────────────────────────────────────────
        grids_res = await session.execute(
            text(
                "SELECT grid_id, grid_num, row_idx, col_idx, ST_AsGeoJSON(geom) as geojson, COALESCE(is_water, FALSE) as is_water "
                "FROM land_grid_cells WHERE land_id = :lid "
                "ORDER BY COALESCE(grid_num, 2147483647), grid_id"
            ),
            {"lid": land_id},
        )
        grid_rows = grids_res.fetchall()

        # ── Latest indices per grid ────────────────────────────────────────
        indices_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, date, b04, b08, b11, ndvi, ndmi, pixel_count, stac_item_id, acquisition_datetime, tile_id, cloud_cover_pct "
                "FROM land_daily_indices "
                "WHERE land_id = :lid AND (ndvi IS NOT NULL OR ndmi IS NOT NULL) "
                "ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id},
        )
        idx_rows = indices_res.fetchall()
        idx_by_grid: dict[str, dict] = {}
        latest_date: str | None = None
        for r in idx_rows:
            idx_by_grid[str(r[0])] = {
                "date": str(r[1]),
                "b04": r[2],
                "b08": r[3],
                "b11": r[4],
                "ndvi": r[5],
                "ndmi": r[6],
                "pixel_count": r[7],
                "stac_item_id": r[8],
                "acquisition_datetime": str(r[9]) if r[9] else None,
                "tile_id": r[10],
                "cloud_cover_pct": r[11],
            }
            if latest_date is None or str(r[1]) > str(latest_date):
                latest_date = str(r[1])

        provenance_res = await session.execute(
            text(
                "SELECT date, stac_item_id, acquisition_datetime, tile_id, cloud_cover_pct "
                "FROM land_daily_indices "
                "WHERE land_id = :lid AND stac_item_id IS NOT NULL "
                "ORDER BY date DESC, grid_id LIMIT 1"
            ),
            {"lid": land_id},
        )
        provenance_row = provenance_res.first()

        # ── Latest LST per grid ────────────────────────────────────────────
        lst_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, date, lst_c "
                "FROM land_daily_lst "
                "WHERE land_id = :lid AND lst_c IS NOT NULL "
                "ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id},
        )
        lst_rows = lst_res.fetchall()
        lst_by_grid: dict[str, dict] = {}
        for r in lst_rows:
            lst_by_grid[str(r[0])] = {"date": str(r[1]), "lst_c": r[2]}

        # ── Latest risk scores per grid ────────────────────────────────────
        risk_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, date, probability "
                "FROM stress_risk_forecast "
                "WHERE land_id = :lid "
                "ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id},
        )
        risk_rows = risk_res.fetchall()
        risk_by_grid: dict[str, dict] = {}
        for r in risk_rows:
            risk_by_grid[str(r[0])] = {"date": str(r[1]), "probability": r[2]}

        # ── Latest anomalies per grid ──────────────────────────────────────
        anom_res = await session.execute(
            text(
                "SELECT grid_id, variable, zscore, value "
                "FROM land_anomalies "
                "WHERE land_id = :lid AND grid_id != '__land__' "
                "AND date = (SELECT MAX(date) FROM land_anomalies WHERE land_id = :lid AND grid_id != '__land__')"
            ),
            {"lid": land_id},
        )
        anom_rows = anom_res.fetchall()
        anom_by_grid: dict[str, dict] = {}
        for r in anom_rows:
            gid = str(r[0])
            anom_by_grid.setdefault(gid, {})[str(r[1])] = {"zscore": r[2], "value": r[3]}

        # ── Weather (land-level, last 7 days) ──────────────────────────────
        weather_res = await session.execute(
            text(
                "SELECT date, t2m, rh2m, prectotcorr "
                "FROM land_daily_weather "
                "WHERE land_id = :lid "
                "ORDER BY date DESC LIMIT 7"
            ),
            {"lid": land_id},
        )
        weather_rows = weather_res.fetchall()

    # ── Build GeoJSON FeatureCollection ───────────────────────────────────
    latest_complete_date = await _get_latest_complete_date(land_id)
    if latest_complete_date:
        latest_date = latest_complete_date

    features = []
    for idx, (internal_grid_id, grid_num, row_idx, col_idx, geojson_str, is_water) in enumerate(grid_rows, start=1):
        internal_gid = str(internal_grid_id)
        public_grid_id = int(grid_num) if grid_num is not None else idx
        grid_geometry = geometry_geojson_storage_to_api(json.loads(geojson_str))
        idx_data = idx_by_grid.get(internal_gid, {})
        lst_data = lst_by_grid.get(internal_gid, {})
        risk_data = risk_by_grid.get(internal_gid, {})
        anom_data = anom_by_grid.get(internal_gid, {})

        ndvi = idx_data.get("ndvi")
        ndvi_norm = max(0.0, min(1.0, (ndvi + 1.0) / 2.0)) if ndvi is not None else None

        ndmi = idx_data.get("ndmi")
        ndmi_norm = max(0.0, min(1.0, (ndmi + 1.0) / 2.0)) if ndmi is not None else None

        lst_c = lst_data.get("lst_c")
        lst_norm = max(0.0, min(1.0, lst_c / 50.0)) if lst_c is not None else None

        risk_prob = risk_data.get("probability")

        props = {
            "grid_id":   public_grid_id,
            "internal_grid_key": internal_gid,
            "row":       row_idx,
            "col":       col_idx,
            "is_water":  bool(is_water),
            "b04":       idx_data.get("b04"),
            "b08":       idx_data.get("b08"),
            "b11":       idx_data.get("b11"),
            "stac_item_id": idx_data.get("stac_item_id"),
            "acquisition_datetime": idx_data.get("acquisition_datetime"),
            "tile_id": idx_data.get("tile_id"),
            "cloud_coverage_pct": idx_data.get("cloud_cover_pct"),
            "ndvi":      ndvi,
            "ndmi":      ndmi,
            "lst_c":     lst_c,
            "pixel_count": idx_data.get("pixel_count"),
            "ndvi_norm": ndvi_norm,
            "ndmi_norm": ndmi_norm,
            "lst_norm":  lst_norm,
            "risk":      risk_prob,
            "color": {
                "ndvi": _ndvi_color(ndvi),
                "ndmi": _ndmi_color(ndmi),
                "lst": _lst_color(lst_c),
            },
            "anomalies": anom_data if anom_data else None,
        }
        features.append({
            "type":       "Feature",
            "properties": props,
            "geometry":   grid_geometry,
        })

    grids_fc = {"type": "FeatureCollection", "features": features}

    # ── Weather timeseries ─────────────────────────────────────────────────
    weather_ts = [
        {"date": str(r[0]), "t2m": r[1], "rh2m": r[2], "prectotcorr": r[3]}
        for r in reversed(weather_rows)
    ]

    # ── Summary stats ──────────────────────────────────────────────────────
    ndvi_vals = [f["properties"]["ndvi"]  for f in features if f["properties"]["ndvi"]  is not None]
    ndmi_vals = [f["properties"]["ndmi"]  for f in features if f["properties"]["ndmi"]  is not None]
    lst_vals  = [f["properties"]["lst_c"] for f in features if f["properties"]["lst_c"] is not None]
    risk_vals = [f["properties"]["risk"]  for f in features if f["properties"]["risk"]  is not None]

    def _stats(vals: list) -> dict | None:
        if not vals:
            return None
        return {"mean": sum(vals) / len(vals), "min": min(vals), "max": max(vals), "count": len(vals)}

    lst_stats = _stats(lst_vals)
    lst_mean: float | None = lst_stats["mean"] if lst_stats else None

    lst_date: str | None = (
        max(v["date"] for v in lst_by_grid.values()) if lst_by_grid else None
    )

    # Bug 4 fix — surface LST availability explicitly so frontend can show
    # a meaningful message instead of a silent "–".
    lst_available = lst_mean is not None
    lst_missing_reason: str | None = None
    if not lst_available:
        lst_missing_reason = (
            "MODIS LST has not been ingested for this land yet. "
            "Run the processing pipeline to fetch satellite data."
        )

    processing = await _get_status(land_id)   # Bug 1 fix: reads from DB
    dashboard_state = await _get_dashboard_state(land_id)
    mode = dashboard_state.get("mode", "latest")
    selected_date = dashboard_state.get("selected_date")
    active_data_date = selected_date if mode == "select" and selected_date else latest_date

    return {
        "land":         land_info,
        "grids":        grids_fc,
        "latest_date":  latest_date,
        "latest_complete_date": latest_complete_date,
        "mode":         mode,
        "selected_date": selected_date,
        "active_data_date": active_data_date,
        "processing": processing,
        "provenance": {
            "satellite_source": "Sentinel-2 L2A (ESA)",
            "acquisition_date": str(provenance_row[0]) if provenance_row and provenance_row[0] else None,
            "acquisition_datetime": str(provenance_row[2]) if provenance_row and provenance_row[2] else None,
            "stac_item_id": provenance_row[1] if provenance_row else None,
            "tile_id": provenance_row[3] if provenance_row else None,
            "cloud_coverage_pct": provenance_row[4] if provenance_row else None,
        },
        "lst_mean":     lst_mean,
        "lst_date":     lst_date,
        # Bug 4 fix — these two fields are new; frontend should use them
        "lst_available":      lst_available,
        "lst_missing_reason": lst_missing_reason,
        "summary": {
            "grid_count": len(grid_rows),
            "ndvi":  _stats(ndvi_vals),
            "ndmi":  _stats(ndmi_vals),
            "lst":   lst_stats,
            "risk":  _stats(risk_vals),
        },
        "color_scales": {
            "ndvi": NDVI_COLOR_SCALE,
            "ndmi": NDMI_COLOR_SCALE,
            "lst": LST_COLOR_SCALE,
            "no_data_color": NO_DATA_COLOR,
        },
        "weather":    weather_ts,
        "processing": processing,
    }