import asyncio
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text

from backend.db.connection import async_session

logger = logging.getLogger(__name__)

# Configurable thermal parameters for Rice (Oryza sativa L. ssp. indica)
# 10.0°C is the conventional regional baseline in literature (Gao et al. 2021).
# Note: Cultivar and region-specific thermal base calibration is planned for field validation.
RICE_PHENOLOGY_CONFIG: Dict[str, Any] = {
    "default_base_temperature": 10.0,
    "t_opt": 30.0,
    "t_max": 35.0,
    "calibration_status": "CONVENTIONAL_BASELINE_CULTIVAR_CALIBRATION_PENDING",
}

RICE_T_BASE_C = RICE_PHENOLOGY_CONFIG["default_base_temperature"]
RICE_T_OPT_C = RICE_PHENOLOGY_CONFIG["t_opt"]
RICE_T_MAX_C = RICE_PHENOLOGY_CONFIG["t_max"]

# Cumulative GDD thresholds (°C-days above base temperature) for standard 120-day indica rice
RICE_GDD_STAGES = [
    (100.0, "Flooding / Land Preparation"),
    (250.0, "Transplanting / Early Establishment"),
    (750.0, "Vegetative / Active Tillering"),
    (1100.0, "Panicle Initiation / Stem Elongation"),
    (1450.0, "Heading / Flowering"),
    (1850.0, "Grain Filling / Ripening / Maturity"),
]


def calculate_daily_gdd(t_mean_c: Optional[float], t_base_c: float = RICE_T_BASE_C) -> float:
    """Calculate daily Growing Degree Days (°C-days) with base temperature."""
    if t_mean_c is None or not math.isfinite(t_mean_c):
        return 0.0
    effective_t = min(t_mean_c, RICE_T_MAX_C)
    return max(effective_t - t_base_c, 0.0)


def determine_phenology_stage(gdd_cumulative: float) -> Tuple[str, float]:
    """Map cumulative GDD to rice phenological stage and confidence."""
    if gdd_cumulative < 0:
        return ("Pre-Planting / Fallow", 0.7)

    for threshold, stage_name in RICE_GDD_STAGES:
        if gdd_cumulative < threshold:
            # High confidence if within the middle of a stage bracket
            confidence = 0.85
            return (stage_name, confidence)

    return ("Maturity / Harvest Ready", 0.90)


async def compute_phenology_for_land(
    land_id: int,
    target_date_str: str,
    season_start_date_str: Optional[str] = None,
    t_base_c: float = RICE_T_BASE_C,
) -> Dict[str, Any]:
    """Compute cumulative GDD, phenological stage, and days since planting for a land up to target date."""
    land_id = int(land_id)
    target_date = datetime.fromisoformat(target_date_str).date()

    # If season start date not provided, default to 90 days lookback or start of Kharif/Rabi
    if season_start_date_str:
        season_start = datetime.fromisoformat(season_start_date_str).date()
    else:
        season_start = target_date - timedelta(days=90)

    logger.info(
        "Computing phenology land=%s target=%s season_start=%s t_base=%.1f",
        land_id,
        target_date,
        season_start,
        t_base_c,
    )

    async with async_session() as session:
        # Fetch daily weather history for GDD accumulation
        weather_res = await session.execute(
            text(
                "SELECT date, t2m FROM land_daily_weather "
                "WHERE land_id = :lid AND date BETWEEN :start AND :target "
                "ORDER BY date"
            ),
            {"lid": land_id, "start": season_start, "target": target_date},
        )
        weather_rows = weather_res.fetchall()

        # Fetch optical & SAR progression for evidence validation
        opt_res = await session.execute(
            text(
                "SELECT date, AVG(ndvi) as ndvi, AVG(lswi) as lswi FROM land_daily_indices "
                "WHERE land_id = :lid AND date BETWEEN :start AND :target AND ndvi IS NOT NULL "
                "GROUP BY date ORDER BY date"
            ),
            {"lid": land_id, "start": season_start, "target": target_date},
        )
        opt_rows = opt_res.fetchall()

    gdd_cumulative = 0.0
    daily_gdds = []
    for row_date, t2m in weather_rows:
        dg = calculate_daily_gdd(float(t2m) if t2m is not None else None, t_base_c=t_base_c)
        gdd_cumulative += dg
        daily_gdds.append((row_date, dg, gdd_cumulative))

    stage, stage_confidence = determine_phenology_stage(gdd_cumulative)
    days_since_planting = (target_date - season_start).days if weather_rows else 0

    evidence = {
        "weather_days_used": len(weather_rows),
        "mean_temp_c": float(np.nanmean([r[1] for r in weather_rows if r[1] is not None])) if weather_rows else None,
        "optical_observations_used": len(opt_rows),
        "latest_ndvi": float(opt_rows[-1][1]) if opt_rows else None,
        "latest_lswi": float(opt_rows[-1][2]) if opt_rows and opt_rows[-1][2] is not None else None,
    }

    # Upsert into land_phenology
    import json
    upsert_sql = text(
        "INSERT INTO land_phenology "
        "(land_id, date, gdd_daily, gdd_cumulative, stage, stage_confidence, days_since_planting, phenological_evidence) "
        "VALUES (:land_id, :date, :gdd_daily, :gdd_cumulative, :stage, :stage_confidence, :dsp, :evidence) "
        "ON CONFLICT (land_id, date) DO UPDATE SET "
        "  gdd_daily = EXCLUDED.gdd_daily, "
        "  gdd_cumulative = EXCLUDED.gdd_cumulative, "
        "  stage = EXCLUDED.stage, "
        "  stage_confidence = EXCLUDED.stage_confidence, "
        "  days_since_planting = EXCLUDED.days_since_planting, "
        "  phenological_evidence = EXCLUDED.phenological_evidence"
    )

    last_daily_gdd = daily_gdds[-1][1] if daily_gdds else 0.0
    async with async_session() as session:
        await session.execute(
            upsert_sql,
            {
                "land_id": land_id,
                "date": target_date,
                "gdd_daily": last_daily_gdd,
                "gdd_cumulative": gdd_cumulative,
                "stage": stage,
                "stage_confidence": stage_confidence,
                "dsp": days_since_planting,
                "evidence": json.dumps(evidence),
            },
        )
        await session.commit()

    return {
        "land_id": land_id,
        "date": target_date_str,
        "stage": stage,
        "stage_confidence": stage_confidence,
        "gdd_cumulative": round(gdd_cumulative, 1),
        "days_since_planting": days_since_planting,
        "evidence": evidence,
    }
