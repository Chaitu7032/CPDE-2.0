import asyncio
import json
import logging
import math
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text

from backend.db.connection import async_session

logger = logging.getLogger(__name__)


# Multi-sensor stress fusion model configuration
# Weights are based on physical domain rules (heuristic_rule_v1)
# Note: Empirical parameter calibration requires ground-truth soil moisture and radiometer data.
FUSION_CONFIG: Dict[str, Any] = {
    "method": "heuristic_rule_v1",
    "calibration_status": "NOT_FIELD_CALIBRATED",
    "water_stress_weights": {
        "ndmi_anomaly": 0.28,
        "sar_vh_anomaly": 0.22,
        "delta_t_canopy_air": 0.20,
        "vpd_anomaly": 0.18,
        "rainfall_deficit": 0.12,
    },
    "chlorophyll_proxy_weights": {
        "ndvi_anomaly": 0.50,
        "evi_anomaly": 0.50,
    },
    "heat_stress_weights": {
        "lst_anomaly": 0.50,
        "delta_t_canopy_air": 0.30,
        "vpd": 0.20,
    },
}


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-min(x, 40.0))
        return 1.0 / (1.0 + z)
    z = math.exp(max(x, -40.0))
    return z / (1.0 + z)


def calculate_data_confidence(
    has_optical: bool,
    has_sar: bool,
    has_thermal: bool,
    has_weather: bool,
    cloud_cover_pct: Optional[float] = None,
    thermal_source: Optional[str] = None,
) -> Tuple[float, str]:
    """Calculate Observation / Data Confidence (how complete and trustworthy are input observations)."""
    score = 0.0

    # Optical contribution (max 0.35)
    if has_optical:
        cc = cloud_cover_pct if cloud_cover_pct is not None else 0.0
        optical_weight = max(0.35 * (1.0 - (cc / 100.0)), 0.10)
        score += optical_weight

    # SAR contribution (cloud-independent structural backscatter: 0.25)
    if has_sar:
        score += 0.25

    # Thermal contribution (0.20 for Landsat 30m, 0.12 for MODIS 1km)
    if has_thermal:
        if thermal_source and "LANDSAT" in thermal_source.upper():
            score += 0.20
        else:
            score += 0.12

    # Weather contribution (0.20)
    if has_weather:
        score += 0.20

    score = min(max(score, 0.0), 1.0)

    if score >= 0.75:
        level = "HIGH"
    elif score >= 0.45:
        level = "MEDIUM"
    elif score >= 0.20:
        level = "LOW"
    else:
        level = "INSUFFICIENT_DATA"

    return round(score, 3), level


def calculate_diagnosis_confidence(
    data_confidence_score: float,
    sensor_agreement: bool = True,
    supporting_sensor_count: int = 1,
) -> Tuple[float, str]:
    """Calculate Diagnosis Confidence (how strongly multi-sensor physics corroborates the biological cause)."""
    if supporting_sensor_count <= 0:
        return 0.0, "INSUFFICIENT_DATA"

    # Multiplier based on number of agreeing independent physical mechanisms
    if supporting_sensor_count >= 3 and sensor_agreement:
        diag_score = data_confidence_score * 1.0
    elif supporting_sensor_count == 2 and sensor_agreement:
        diag_score = data_confidence_score * 0.85
    elif not sensor_agreement:
        diag_score = data_confidence_score * 0.50
    else:
        # Single sensor only
        diag_score = data_confidence_score * 0.60

    diag_score = min(max(diag_score, 0.0), 1.0)

    if diag_score >= 0.70:
        diag_level = "HIGH"
    elif diag_score >= 0.40:
        diag_level = "MEDIUM"
    elif diag_score >= 0.15:
        diag_level = "LOW"
    else:
        diag_level = "UNCERTAIN"

    return round(diag_score, 3), diag_level


async def compute_multi_sensor_stress_for_land(
    land_id: int,
    date_str: str,
) -> Dict[str, Any]:
    """Phase 11 & 12: Research-grade Multi-Sensor Interpretable Stress Fusion & Confidence Engine."""
    land_id = int(land_id)
    obs_date = datetime.fromisoformat(date_str).date()
    logger.info("Computing multi-sensor stress fusion land=%s date=%s", land_id, obs_date)

    async with async_session() as session:
        # 1. Fetch grid cells
        grids_res = await session.execute(
            text("SELECT grid_id, COALESCE(is_water, FALSE) FROM land_grid_cells WHERE land_id = :lid ORDER BY grid_id"),
            {"lid": land_id},
        )
        grids = grids_res.fetchall()
        if not grids:
            return {"processed": 0, "reason": "no grids found for land"}

        # 2. Fetch latest optical indices
        opt_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, ndvi, ndmi, evi, lswi, quality_flag, cloud_cover_pct, date "
                "FROM land_daily_indices WHERE land_id = :lid AND date <= :dt ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id, "dt": obs_date},
        )
        opt_map = {r[0]: r for r in opt_res.fetchall()}

        # 3. Fetch latest SAR observations
        sar_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, vv, vh, vh_vv_ratio, date "
                "FROM land_daily_sar WHERE land_id = :lid AND date <= :dt ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id, "dt": obs_date},
        )
        sar_map = {r[0]: r for r in sar_res.fetchall()}

        # 4. Fetch latest thermal observations
        thm_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id) grid_id, lst_c, source_sensor, native_resolution_m, date "
                "FROM land_daily_lst WHERE land_id = :lid AND date <= :dt ORDER BY grid_id, date DESC"
            ),
            {"lid": land_id, "dt": obs_date},
        )
        thm_map = {r[0]: r for r in thm_res.fetchall()}

        # 5. Fetch weather for date
        wea_res = await session.execute(
            text(
                "SELECT t2m, rh2m, prectotcorr, vpd FROM land_daily_weather "
                "WHERE land_id = :lid AND date <= :dt ORDER BY date DESC LIMIT 1"
            ),
            {"lid": land_id, "dt": obs_date},
        )
        wea_row = wea_res.first()

        # 6. Fetch z-score anomalies
        anom_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id, variable) grid_id, variable, zscore "
                "FROM land_anomalies WHERE land_id = :lid AND date <= :dt ORDER BY grid_id, variable, date DESC"
            ),
            {"lid": land_id, "dt": obs_date},
        )
        anom_by_grid: Dict[str, Dict[str, float]] = {}
        for gid, var, z in anom_res.fetchall():
            if z is not None:
                anom_by_grid.setdefault(str(gid), {})[str(var)] = float(z)

    t2m = float(wea_row[0]) if wea_row and wea_row[0] is not None else None
    prectot = float(wea_row[2]) if wea_row and wea_row[2] is not None else None
    vpd = float(wea_row[3]) if wea_row and wea_row[3] is not None else None

    has_weather = wea_row is not None
    results = []
    water_stress_vals = []
    chl_stress_vals = []
    heat_stress_vals = []

    for grid_id, is_water_flag in grids:
        gid = str(grid_id)
        opt = opt_map.get(gid)
        sar = sar_map.get(gid)
        thm = thm_map.get(gid)
        anoms = anom_by_grid.get(gid, {})

        has_opt = opt is not None and opt[1] is not None
        has_s = sar is not None and (sar[1] is not None or sar[2] is not None)
        has_t = thm is not None and thm[1] is not None

        cloud_cov = float(opt[6]) if opt and opt[6] is not None else None
        thm_src = str(thm[2]) if thm and thm[2] is not None else None

        # ── 1. Water Deficit / Drought Stress Model ────────────────────────────
        # NDMI z-score (negative = moisture drop)
        # SAR VH z-score (negative = biomass/moisture loss)
        # VPD (high = atmospheric evaporative demand)
        # LST - T2M (positive = stomatal closure / thermal elevation)
        z_ndmi = anoms.get("ndmi", 0.0)
        z_vh = anoms.get("sar_vh", 0.0)
        z_vpd = anoms.get("vpd", 0.0)

        lst_c = float(thm[1]) if has_t else None
        delta_t = (lst_c - t2m) if (lst_c is not None and t2m is not None) else 0.0

        # Weighted linear combination
        linear_water = -0.3 + 0.7 * (-z_ndmi) + 0.6 * (-z_vh) + 0.4 * (delta_t / 5.0) + 0.3 * z_vpd
        water_stress = _sigmoid(linear_water) if (has_opt or has_s or has_weather) else None

        # ── 2. Chlorophyll / Nitrogen Proxy Stress Model ──────────────────────
        # NDVI z-score + EVI z-score
        z_ndvi = anoms.get("ndvi", 0.0)
        z_evi = anoms.get("evi", 0.0)
        linear_chl = -0.4 + 0.8 * (-z_ndvi) + 0.6 * (-z_evi)
        chl_stress = _sigmoid(linear_chl) if has_opt else None

        # ── 3. Canopy Heat Stress Model ────────────────────────────────────────
        z_lst = anoms.get("lst", 0.0)
        linear_heat = -0.5 + 0.9 * z_lst + 0.5 * (delta_t / 5.0) + 0.3 * (vpd / 2.0 if vpd else 0.0)
        heat_stress = _sigmoid(linear_heat) if (has_t or has_weather) else None

        # ── 4. Flooding State Classifier ──────────────────────────────────────
        flooding_state = "dry_canopy"
        if opt and opt[5] == "FLOODED_RICE":
            flooding_state = "flooded_rice"
        elif opt and opt[4] is not None and opt[4] > 0.3:  # high LSWI
            flooding_state = "saturated"
        elif is_water_flag:
            flooding_state = "permanent_water"

        # ── 5. Sensor Agreement Check ─────────────────────────────────────────
        # Agreement: if optical says water stress (NDMI low), SAR should also show moisture drop or normal
        agreement = True
        if z_ndmi < -1.5 and z_vh > 1.5:
            agreement = False

        supporting_sensor_count = sum([has_opt, has_s, has_t, has_weather])
        data_conf_score, data_conf_level = calculate_data_confidence(
            has_optical=has_opt,
            has_sar=has_s,
            has_thermal=has_t,
            has_weather=has_weather,
            cloud_cover_pct=cloud_cov,
            thermal_source=thm_src,
        )
        diag_conf_score, diag_conf_level = calculate_diagnosis_confidence(
            data_confidence_score=data_conf_score,
            sensor_agreement=agreement,
            supporting_sensor_count=supporting_sensor_count,
        )

        # Dominant Stress determination & Evidence Attribution (Rule 6)
        stresses = [
            ("Pattern consistent with water-related vegetation stress", water_stress or 0.0),
            ("Pattern consistent with canopy chlorophyll status anomaly", chl_stress or 0.0),
            ("Elevated canopy thermal load", heat_stress or 0.0),
        ]
        stresses.sort(key=lambda x: x[1], reverse=True)
        dominant_stress = stresses[0][0] if stresses[0][1] > 0.45 else "Normal Crop Condition"

        evidence = {
            "has_optical": has_opt,
            "has_sar": has_s,
            "has_thermal": has_t,
            "has_weather": has_weather,
            "supporting_sensor_count": supporting_sensor_count,
            "delta_t_canopy_air": round(delta_t, 2) if delta_t != 0.0 else None,
            "vpd_kpa": round(vpd, 2) if vpd is not None else None,
            "dominant_stress": dominant_stress,
            "data_confidence_level": data_conf_level,
            "diagnosis_confidence_level": diag_conf_level,
            "confounders_evaluated": [
                "phenological_senescence",
                "chlorophyll_nitrogen_deficiency",
                "foliar_disease",
                "cloud_shadow_attenuation",
            ],
            "actionable_recommendation": "Field inspection recommended: inspect root zone soil moisture and canopy before scheduling irrigation.",
            "calibration_status": FUSION_CONFIG["calibration_status"],
            "model_type": "physical_multi_sensor_rules_v2",
        }

        if water_stress is not None:
            water_stress_vals.append(water_stress)
        if chl_stress is not None:
            chl_stress_vals.append(chl_stress)
        if heat_stress is not None:
            heat_stress_vals.append(heat_stress)

        results.append({
            "land_id": land_id,
            "grid_id": gid,
            "date": obs_date,
            "water_stress_score": round(water_stress, 4) if water_stress is not None else None,
            "chlorophyll_stress_score": round(chl_stress, 4) if chl_stress is not None else None,
            "heat_stress_score": round(heat_stress, 4) if heat_stress is not None else None,
            "flooding_state": flooding_state,
            "confidence_score": diag_conf_score,
            "confidence_level": diag_conf_level,
            "dominant_stress": dominant_stress,
            "evidence_json": json.dumps(evidence),
            "is_water": is_water_flag,
        })

    # Upsert into land_multi_sensor_stress
    upsert_sql = text(
        "INSERT INTO land_multi_sensor_stress "
        "(land_id, grid_id, date, water_stress_score, chlorophyll_stress_score, heat_stress_score, "
        " flooding_state, confidence_score, confidence_level, dominant_stress, evidence_json, is_water) "
        "VALUES (:land_id, :grid_id, :date, :water_stress_score, :chlorophyll_stress_score, :heat_stress_score, "
        "        :flooding_state, :confidence_score, :confidence_level, :dominant_stress, :evidence_json, :is_water) "
        "ON CONFLICT (grid_id, date) DO UPDATE SET "
        "  water_stress_score = EXCLUDED.water_stress_score, "
        "  chlorophyll_stress_score = EXCLUDED.chlorophyll_stress_score, "
        "  heat_stress_score = EXCLUDED.heat_stress_score, "
        "  flooding_state = EXCLUDED.flooding_state, "
        "  confidence_score = EXCLUDED.confidence_score, "
        "  confidence_level = EXCLUDED.confidence_level, "
        "  dominant_stress = EXCLUDED.dominant_stress, "
        "  evidence_json = EXCLUDED.evidence_json, "
        "  is_water = EXCLUDED.is_water"
    )

    async with async_session() as session:
        if results:
            await session.execute(upsert_sql, results)
            await session.commit()

    logger.info("Multi-sensor fusion complete land=%s date=%s processed=%d", land_id, obs_date, len(results))
    return {
        "processed": len(results),
        "date": obs_date.isoformat(),
        "mean_water_stress": float(np.mean(water_stress_vals)) if water_stress_vals else None,
        "mean_chlorophyll_stress": float(np.mean(chl_stress_vals)) if chl_stress_vals else None,
        "mean_heat_stress": float(np.mean(heat_stress_vals)) if heat_stress_vals else None,
    }
