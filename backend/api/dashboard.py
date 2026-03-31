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

import json
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from backend.pipelines.anomaly import build_climatology_for_variable, compute_anomalies_for_date, VARIABLE_SOURCES
from backend.pipelines.risk import compute_risk_for_land_date
from backend.pipelines.sentinel2 import process_sentinel2_for_land_day
from backend.pipelines.modis import process_modis_for_land_day
from backend.pipelines.nasa_power import process_weather_for_land
from backend.pipelines.grid_generation import generate_and_store_grids

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


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

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
        await _set_status(land_id, "running", "grids")   # Bug 1 fix: DB write

        # Step 1: Ensure grids exist.
        async with async_session() as session:
            res = await session.execute(
                text("SELECT COUNT(*) FROM land_grid_cells WHERE land_id = :lid"),
                {"lid": land_id},
            )
            grid_count = res.scalar()

        if not grid_count:
            await _set_status(land_id, "running", "generating grids")
            await generate_and_store_grids(land_id, cell_size_m=10.0)

        # Step 2: Sentinel-2 NDVI/NDMI
        await _set_status(land_id, "running", "sentinel2")
        try:
            s2 = await process_sentinel2_for_land_day(land_id, date_str)
            logger.info("Sentinel-2 result for land %s: %s", land_id, s2)
        except Exception as e:
            logger.warning("Sentinel-2 pipeline error for land %s: %s", land_id, e)
            s2 = {"processed": 0, "reason": str(e)}

        # Step 3: MODIS LST
        await _set_status(land_id, "running", "modis")
        try:
            mod = await process_modis_for_land_day(land_id, date_str)
            logger.info("MODIS result for land %s: %s", land_id, mod)
        except Exception as e:
            logger.warning("MODIS pipeline error for land %s: %s", land_id, e)
            mod = {"processed": 0, "reason": str(e)}

        # Step 4: NASA POWER weather — 14-day window.
        await _set_status(land_id, "running", "weather")
        try:
            weather_start = (datetime.fromisoformat(date_str) - timedelta(days=14)).strftime("%Y-%m-%d")
            wea = await process_weather_for_land(land_id, weather_start, date_str)
            logger.info("Weather result for land %s: %s", land_id, wea)
        except Exception as e:
            logger.warning("Weather pipeline error for land %s: %s", land_id, e)
            wea = {"processed": 0, "reason": str(e)}

        # Step 5: Climatology — rebuild after every ingest.
        await _set_status(land_id, "running", "climatology")
        for v in ("ndvi", "ndmi", "lst", "t2m", "prectotcorr"):
            if v in VARIABLE_SOURCES:
                try:
                    await build_climatology_for_variable(land_id, v)
                except Exception as e:
                    logger.warning("Climatology build error for %s: %s", v, e)

        # Step 6: Anomalies — only for dates with real data.
        # Bug 2 fix — lst_date is None when MODIS failed; not added to the date set.
        await _set_status(land_id, "running", "anomalies")
        s2_date  = _safe_s2_date(s2, date_str)       # Bug 5 fix
        lst_date = mod.get("lst_date") or None        # Bug 2 fix

        all_dates = list(filter(None, {date_str, s2_date, lst_date}))
        await _compute_anomalies_for_dates(land_id, all_dates)

        # Step 7: Risk — uses latest-per-variable anomalies.
        await _set_status(land_id, "running", "risk")
        try:
            await compute_risk_for_land_date(land_id, date_str)
        except Exception as e:
            logger.warning("Risk computation error: %s", e)

        await _set_status(land_id, "done", "complete")   # Bug 1 fix: DB write

    except Exception as e:
        logger.exception("Processing pipeline failed for land %s", land_id)
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
            "geometry":     json.loads(land_row[3]) if land_row[3] else None,
            "area_sqm":     land_row[4],
            "created_at":   str(land_row[5]) if land_row[5] else None,
        }

        # ── Grids ──────────────────────────────────────────────────────────
        grids_res = await session.execute(
            text(
                "SELECT grid_id, ST_AsGeoJSON(geom) as geojson, COALESCE(is_water, FALSE) as is_water "
                "FROM land_grid_cells WHERE land_id = :lid ORDER BY grid_id"
            ),
            {"lid": land_id},
        )
        grid_rows = grids_res.fetchall()

        # ── Latest indices per grid ────────────────────────────────────────
        indices_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, date, ndvi, ndmi, pixel_count "
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
            idx_by_grid[str(r[0])] = {"date": str(r[1]), "ndvi": r[2], "ndmi": r[3], "pixel_count": r[4]}
            if latest_date is None or str(r[1]) > str(latest_date):
                latest_date = str(r[1])

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
    features = []
    for grid_id, geojson_str, is_water in grid_rows:
        gid      = str(grid_id)
        idx_data = idx_by_grid.get(gid, {})
        lst_data = lst_by_grid.get(gid, {})
        risk_data = risk_by_grid.get(gid, {})
        anom_data = anom_by_grid.get(gid, {})

        ndvi = idx_data.get("ndvi")
        ndvi_norm = max(0.0, min(1.0, (ndvi + 1.0) / 2.0)) if ndvi is not None else None

        ndmi = idx_data.get("ndmi")
        ndmi_norm = max(0.0, min(1.0, (ndmi + 1.0) / 2.0)) if ndmi is not None else None

        lst_c = lst_data.get("lst_c")
        lst_norm = max(0.0, min(1.0, lst_c / 50.0)) if lst_c is not None else None

        risk_prob = risk_data.get("probability")

        props = {
            "grid_id":   gid,
            "is_water":  bool(is_water),
            "ndvi":      ndvi,
            "ndmi":      ndmi,
            "lst_c":     lst_c,
            "ndvi_norm": ndvi_norm,
            "ndmi_norm": ndmi_norm,
            "lst_norm":  lst_norm,
            "risk":      risk_prob,
            "anomalies": anom_data if anom_data else None,
        }
        features.append({
            "type":       "Feature",
            "properties": props,
            "geometry":   json.loads(geojson_str),
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

    return {
        "land":         land_info,
        "grids":        grids_fc,
        "latest_date":  latest_date,
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
        "weather":    weather_ts,
        "processing": processing,
    }