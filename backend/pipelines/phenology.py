"""
CPDE v2 Phenology & Growing Degree Day (GDD) Engine
Enforces Rule 3: Crop-specific, literature-sourced GDD parameters for Bapatla / Andhra Pradesh crops.
Literature Sources: IRRI Rice Knowledge Bank, ICAR-CICR, ICAR-IIHR, IIMR, ICRISAT, ANGRAU AP.
"""

import asyncio
import json
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text

from backend.db.connection import async_session

logger = logging.getLogger(__name__)

# Crop-specific literature-sourced thermal baselines and stages
CROP_PHENOLOGY_CONFIG: Dict[str, Dict[str, Any]] = {
    "rice": {
        "name": "Paddy / Rice (Indica - BPT 5204 / Samba Mahsuri)",
        "t_base": 10.0,
        "t_opt": 30.0,
        "t_max": 40.0,
        "literature_source": "IRRI Rice Knowledge Bank; ANGRAU Rice Production Guidelines (AP)",
        "stages": [
            (100.0, "Flooding / Land Preparation"),
            (250.0, "Transplanting / Early Establishment"),
            (750.0, "Vegetative / Active Tillering"),
            (1100.0, "Panicle Initiation / Stem Elongation"),
            (1500.0, "Heading / Flowering"),
            (1900.0, "Grain Filling / Ripening"),
            (2200.0, "Maturity / Senescence"),
        ],
        "expected_curve": {
            "Flooding / Land Preparation": {"ndvi": 0.15, "ndmi": 0.35, "ndre": 0.12},
            "Transplanting / Early Establishment": {"ndvi": 0.28, "ndmi": 0.25, "ndre": 0.20},
            "Vegetative / Active Tillering": {"ndvi": 0.58, "ndmi": 0.20, "ndre": 0.38},
            "Panicle Initiation / Stem Elongation": {"ndvi": 0.72, "ndmi": 0.24, "ndre": 0.48},
            "Heading / Flowering": {"ndvi": 0.78, "ndmi": 0.22, "ndre": 0.52},
            "Grain Filling / Ripening": {"ndvi": 0.62, "ndmi": 0.10, "ndre": 0.38},
            "Maturity / Senescence": {"ndvi": 0.35, "ndmi": -0.05, "ndre": 0.22},
        },
    },
    "cotton": {
        "name": "Cotton (Bt Cotton)",
        "t_base": 15.5,
        "t_opt": 32.0,
        "t_max": 38.0,
        "literature_source": "ICAR-Central Institute for Cotton Research (CICR); FAO-56",
        "stages": [
            (150.0, "Emergence / Seedling"),
            (550.0, "Square Initiation / Vegetative"),
            (1100.0, "Early Bloom / Peak Flowering"),
            (1700.0, "Peak Boll Development / Boll Filling"),
            (2200.0, "Boll Opening / Maturity / Senescence"),
        ],
        "expected_curve": {
            "Emergence / Seedling": {"ndvi": 0.20, "ndmi": 0.05, "ndre": 0.15},
            "Square Initiation / Vegetative": {"ndvi": 0.48, "ndmi": 0.12, "ndre": 0.32},
            "Early Bloom / Peak Flowering": {"ndvi": 0.75, "ndmi": 0.22, "ndre": 0.50},
            "Peak Boll Development / Boll Filling": {"ndvi": 0.68, "ndmi": 0.16, "ndre": 0.44},
            "Boll Opening / Maturity / Senescence": {"ndvi": 0.38, "ndmi": -0.02, "ndre": 0.25},
        },
    },
    "chilli": {
        "name": "Chilli (Capsicum annuum L. - Guntur/Bapatla Varieties)",
        "t_base": 12.0,
        "t_opt": 28.0,
        "t_max": 35.0,
        "literature_source": "ICAR-IIHR; CRIDA Andhra Pradesh Horticulture Compendium",
        "stages": [
            (400.0, "Vegetative Establishment / Branching"),
            (800.0, "Flowering & Fruit Set"),
            (1400.0, "Fruit Development & Green Harvest"),
            (1900.0, "Ripening & Final Harvest"),
        ],
        "expected_curve": {
            "Vegetative Establishment / Branching": {"ndvi": 0.35, "ndmi": 0.10, "ndre": 0.24},
            "Flowering & Fruit Set": {"ndvi": 0.65, "ndmi": 0.18, "ndre": 0.42},
            "Fruit Development & Green Harvest": {"ndvi": 0.70, "ndmi": 0.20, "ndre": 0.46},
            "Ripening & Final Harvest": {"ndvi": 0.45, "ndmi": 0.02, "ndre": 0.30},
        },
    },
    "maize": {
        "name": "Maize (Zea mays L.)",
        "t_base": 10.0,
        "t_opt": 30.0,
        "t_max": 35.0,
        "literature_source": "ICAR-Indian Institute of Maize Research (IIMR); Ritchie et al.",
        "stages": [
            (350.0, "Emergence to V6"),
            (750.0, "Rapid Vegetative Growth (V6-VT)"),
            (950.0, "Silking & Pollination (R1)"),
            (1400.0, "Dough & Dent Stage (R4-R5)"),
            (1700.0, "Physiological Maturity (R6)"),
        ],
        "expected_curve": {
            "Emergence to V6": {"ndvi": 0.25, "ndmi": 0.05, "ndre": 0.18},
            "Rapid Vegetative Growth (V6-VT)": {"ndvi": 0.65, "ndmi": 0.18, "ndre": 0.44},
            "Silking & Pollination (R1)": {"ndvi": 0.82, "ndmi": 0.26, "ndre": 0.55},
            "Dough & Dent Stage (R4-R5)": {"ndvi": 0.68, "ndmi": 0.14, "ndre": 0.42},
            "Physiological Maturity (R6)": {"ndvi": 0.36, "ndmi": -0.04, "ndre": 0.24},
        },
    },
    "groundnut": {
        "name": "Groundnut / Peanut (Arachis hypogaea L.)",
        "t_base": 10.0,
        "t_opt": 28.0,
        "t_max": 36.0,
        "literature_source": "ICRISAT Groundnut Crop Guide; ICAR-DGR",
        "stages": [
            (200.0, "Emergence & Seedling"),
            (500.0, "Vegetative Branching"),
            (850.0, "Flowering & Pegging"),
            (1350.0, "Pod Development & Seed Filling"),
            (1700.0, "Maturity & Harvest"),
        ],
        "expected_curve": {
            "Emergence & Seedling": {"ndvi": 0.22, "ndmi": 0.04, "ndre": 0.16},
            "Vegetative Branching": {"ndvi": 0.52, "ndmi": 0.14, "ndre": 0.34},
            "Flowering & Pegging": {"ndvi": 0.72, "ndmi": 0.22, "ndre": 0.48},
            "Pod Development & Seed Filling": {"ndvi": 0.64, "ndmi": 0.15, "ndre": 0.40},
            "Maturity & Harvest": {"ndvi": 0.34, "ndmi": -0.05, "ndre": 0.22},
        },
    },
}

# Backward compatibility alias
RICE_PHENOLOGY_CONFIG = CROP_PHENOLOGY_CONFIG["rice"]
RICE_T_BASE_C = RICE_PHENOLOGY_CONFIG["t_base"]
RICE_T_OPT_C = RICE_PHENOLOGY_CONFIG["t_opt"]
RICE_T_MAX_C = RICE_PHENOLOGY_CONFIG["t_max"]
RICE_GDD_STAGES = RICE_PHENOLOGY_CONFIG["stages"]


def get_crop_config(crop_type: Optional[str]) -> Dict[str, Any]:
    """Retrieve normalized crop phenology configuration."""
    if not crop_type:
        return CROP_PHENOLOGY_CONFIG["rice"]
    key = str(crop_type).lower().strip()
    for k, v in CROP_PHENOLOGY_CONFIG.items():
        if k in key or key in k:
            return v
    return CROP_PHENOLOGY_CONFIG["rice"]


def calculate_daily_gdd(
    t_mean_c: Optional[float],
    crop_type: Optional[str] = "rice",
    t_base_c: Optional[float] = None,
) -> float:
    """Calculate daily Growing Degree Days (°C-days) using crop-specific baseline and cutoff."""
    if t_mean_c is None or not math.isfinite(t_mean_c):
        return 0.0
    cfg = get_crop_config(crop_type)
    t_base = t_base_c if t_base_c is not None else cfg["t_base"]
    t_max = cfg["t_max"]
    effective_t = min(t_mean_c, t_max)
    return max(effective_t - t_base, 0.0)


def determine_phenology_stage(gdd_cumulative: float, crop_type: Optional[str] = "rice") -> Tuple[str, float]:
    """Map cumulative GDD to crop-specific phenological stage and confidence."""
    cfg = get_crop_config(crop_type)
    stages = cfg["stages"]

    if gdd_cumulative < 0:
        return ("Pre-Planting / Fallow", 0.70)

    for threshold, stage_name in stages:
        if gdd_cumulative < threshold:
            confidence = 0.85
            return (stage_name, confidence)

    return (stages[-1][1], 0.90)


def get_expected_stage_indices(stage: str, crop_type: Optional[str] = "rice") -> Dict[str, float]:
    """Get expected baseline indices for crop and growth stage."""
    cfg = get_crop_config(crop_type)
    curve = cfg.get("expected_curve", {})
    if stage in curve:
        return curve[stage]
    # Default fallback
    return {"ndvi": 0.50, "ndmi": 0.15, "ndre": 0.35}


async def compute_phenology_for_land(
    land_id: int,
    target_date_str: str,
    season_start_date_str: Optional[str] = None,
    crop_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute cumulative GDD, phenological stage, expected baseline, and days since planting."""
    land_id = int(land_id)
    target_date = datetime.fromisoformat(target_date_str).date()

    async with async_session() as session:
        if not crop_type:
            res = await session.execute(text("SELECT crop_type FROM lands WHERE land_id = :lid"), {"lid": land_id})
            r = res.first()
            if r and r[0]:
                crop_type = str(r[0])
            else:
                crop_type = "rice"

    cfg = get_crop_config(crop_type)

    if season_start_date_str:
        season_start = datetime.fromisoformat(season_start_date_str).date()
    else:
        season_start = target_date - timedelta(days=90)

    logger.info(
        "Computing phenology land=%s crop=%s target=%s season_start=%s t_base=%.1f source='%s'",
        land_id,
        cfg["name"],
        target_date,
        season_start,
        cfg["t_base"],
        cfg["literature_source"],
    )

    async with async_session() as session:
        weather_res = await session.execute(
            text(
                "SELECT date, t2m FROM land_daily_weather "
                "WHERE land_id = :lid AND date BETWEEN :start AND :target "
                "ORDER BY date"
            ),
            {"lid": land_id, "start": season_start, "target": target_date},
        )
        weather_rows = weather_res.fetchall()

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
        dg = calculate_daily_gdd(float(t2m) if t2m is not None else None, crop_type=crop_type)
        gdd_cumulative += dg
        daily_gdds.append((row_date, dg, gdd_cumulative))

    stage, stage_confidence = determine_phenology_stage(gdd_cumulative, crop_type=crop_type)
    expected_indices = get_expected_stage_indices(stage, crop_type=crop_type)
    days_since_planting = (target_date - season_start).days if weather_rows else 0

    evidence = {
        "crop_profile": cfg["name"],
        "literature_source": cfg["literature_source"],
        "t_base_c": cfg["t_base"],
        "t_opt_c": cfg["t_opt"],
        "t_max_c": cfg["t_max"],
        "expected_stage_indices": expected_indices,
        "weather_days_used": len(weather_rows),
        "mean_temp_c": float(np.nanmean([r[1] for r in weather_rows if r[1] is not None])) if weather_rows else None,
        "optical_observations_used": len(opt_rows),
        "latest_ndvi": float(opt_rows[-1][1]) if opt_rows else None,
        "latest_lswi": float(opt_rows[-1][2]) if opt_rows and opt_rows[-1][2] is not None else None,
    }

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
        "crop_profile": cfg["name"],
        "literature_source": cfg["literature_source"],
        "date": target_date_str,
        "stage": stage,
        "stage_confidence": stage_confidence,
        "gdd_cumulative": round(gdd_cumulative, 1),
        "days_since_planting": days_since_planting,
        "expected_indices": expected_indices,
        "evidence": evidence,
    }
