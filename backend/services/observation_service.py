"""
CPDE v2 Observation, Spectral Ingestion, and Temporal Analytics Engine
Orchestrates Sentinel-2 multispectral ingestion, NASA POWER weather,
6 scientific indices calculation, phenology stage gating, and temporal change.
"""

from __future__ import annotations
import json
import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Optional

import numpy as np
from sqlalchemy import text

from backend.db.connection import async_session
from backend.domain.phenology import determine_growth_stage
from backend.domain.quality_engine import compute_observation_quality
from backend.domain.recommendation_engine import generate_explainable_recommendation
from backend.domain.scientific_indices import (
    INDEX_REGISTRY,
    calculate_evi,
    calculate_gci,
    calculate_ndmi,
    calculate_ndre,
    calculate_ndvi,
    calculate_savi,
    calculate_vpd,
    classify_health,
)
from backend.pipelines.nasa_power import fetch_power_point
from backend.pipelines.sentinel2 import (
    PC_STAC_API,
    _compute_indices_for_points,
    _extract_cloud_cover,
    _extract_item_datetime,
    _extract_tile_id,
)

logger = logging.getLogger(__name__)


async def get_field_available_dates(field_id: int) -> list[dict[str, Any]]:
    """Retrieve all observation dates available for a field, including satellite quality and weather availability."""
    async with async_session() as session:
        q = text("""
            SELECT date, satellite_source, cloud_cover_pct, quality_score, confidence_score, growth_stage
            FROM field_observations
            WHERE field_id = :fid
            ORDER BY date DESC
        """)
        res = await session.execute(q, {"fid": field_id})
        rows = res.fetchall()

        return [
            {
                "date": r[0].isoformat() if isinstance(r[0], (date, datetime)) else str(r[0]),
                "satellite_source": r[1] or "Sentinel-2 L2A",
                "cloud_cover_pct": r[2],
                "quality_score": r[3],
                "confidence_score": r[4],
                "growth_stage": r[5] or "vegetative",
            }
            for r in rows
        ]


async def get_field_state_at_date(field_id: int, target_date: Optional[str] = None) -> dict[str, Any]:
    """Load the complete field state at a given observation date (or latest available).
    
    Returns:
      - Field metadata & GeoJSON geometry
      - GeoJSON FeatureCollection of all 10m grid cells with all 6 indices & health categories
      - Macro weather summary & VPD
      - Growth stage & phenological gating status
      - Explainable recommendation alerts with full scientific audit trail
    """
    async with async_session() as session:
        # 1. Fetch Field metadata and polygon (transformed to WGS84 GeoJSON for frontend display)
        field_q = text("""
            SELECT id, name, crop_type, area_sqm, area_ha, perimeter_m,
                   village, mandal, district, state, country,
                   geometry_quality_score, registration_confidence,
                   ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_geom,
                   ST_AsGeoJSON(ST_Transform(centroid, 4326)) AS geojson_centroid
            FROM fields WHERE id = :fid
        """)
        f_res = await session.execute(field_q, {"fid": field_id})
        field_row = f_res.fetchone()
        if not field_row:
            raise ValueError(f"Field #{field_id} not found.")

        field_geom = json.loads(field_row[13]) if field_row[13] else None
        centroid_geom = json.loads(field_row[14]) if field_row[14] else None

        # 2. Determine target observation date
        if target_date:
            obs_date_obj = datetime.fromisoformat(target_date).date()
        else:
            latest_date_q = text("""
                SELECT date FROM field_observations
                WHERE field_id = :fid ORDER BY date DESC LIMIT 1
            """)
            d_res = await session.execute(latest_date_q, {"fid": field_id})
            latest_row = d_res.fetchone()
            obs_date_obj = latest_row[0] if latest_row else date.today()

        # 3. Fetch field observation metadata
        obs_q = text("""
            SELECT satellite_source, stac_item_id, tile_id, cloud_cover_pct,
                   quality_score, confidence_score, growth_stage, weather_summary
            FROM field_observations
            WHERE field_id = :fid AND date = :dt
        """)
        obs_res = await session.execute(obs_q, {"fid": field_id, "dt": obs_date_obj})
        obs_meta = obs_res.fetchone()

        weather_data = json.loads(obs_meta[7]) if obs_meta and obs_meta[7] else {}
        growth_stage = obs_meta[6] if obs_meta and obs_meta[6] else "vegetative"
        quality_score = obs_meta[4] if obs_meta and obs_meta[4] is not None else 85.0
        confidence_score = obs_meta[5] if obs_meta and obs_meta[5] is not None else 82.0

        # 4. Fetch grid cells with their spectral indices for this date
        grid_q = text("""
            SELECT c.grid_id, c.row_idx, c.col_idx, c.area_sqm, c.is_water,
                   ST_AsGeoJSON(ST_Transform(c.geom, 4326)) AS geojson_geom,
                   o.b02, o.b03, o.b04, o.b05, o.b08, o.b11,
                   o.ndvi, o.ndmi, o.ndre, o.evi, o.savi, o.gci,
                   o.health_status, o.quality_score, o.confidence_score
            FROM field_grid_cells c
            LEFT JOIN grid_observations o
              ON c.grid_id = o.grid_id AND o.date = :dt
            WHERE c.field_id = :fid
            ORDER BY c.grid_num ASC
        """)
        g_res = await session.execute(grid_q, {"fid": field_id, "dt": obs_date_obj})
        grid_rows = g_res.fetchall()

        features = []
        ndvi_vals, ndmi_vals, ndre_vals, evi_vals, savi_vals, gci_vals = [], [], [], [], [], []

        for r in grid_rows:
            gid = r[0]
            geom = json.loads(r[5]) if r[5] else None

            ndvi = r[12]
            ndmi = r[13]
            ndre = r[14]
            evi = r[15]
            savi = r[16]
            gci = r[17]

            if ndvi is not None: ndvi_vals.append(ndvi)
            if ndmi is not None: ndmi_vals.append(ndmi)
            if ndre is not None: ndre_vals.append(ndre)
            if evi is not None: evi_vals.append(evi)
            if savi is not None: savi_vals.append(savi)
            if gci is not None: gci_vals.append(gci)

            # Classifications
            ndvi_class = classify_health("ndvi", ndvi)
            ndmi_class = classify_health("ndmi", ndmi)
            ndre_class = classify_health("ndre", ndre)
            evi_class = classify_health("evi", evi)
            savi_class = classify_health("savi", savi)
            gci_class = classify_health("gci", gci)

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "grid_id": gid,
                    "row": r[1],
                    "col": r[2],
                    "area_sqm": r[3],
                    "is_water": bool(r[4]),
                    "bands": {
                        "b02": r[6],
                        "b03": r[7],
                        "b04": r[8],
                        "b05": r[9],
                        "b08": r[10],
                        "b11": r[11],
                    },
                    "indices": {
                        "ndvi": ndvi,
                        "ndmi": ndmi,
                        "ndre": ndre,
                        "evi": evi,
                        "savi": savi,
                        "gci": gci,
                    },
                    "classifications": {
                        "ndvi": ndvi_class,
                        "ndmi": ndmi_class,
                        "ndre": ndre_class,
                        "evi": evi_class,
                        "savi": savi_class,
                        "gci": gci_class,
                    },
                    "primary_color": ndvi_class["color"],
                    "quality_score": r[19] or quality_score,
                    "confidence_score": r[20] or confidence_score,
                },
            })

        # Summary statistics
        summary = {
            "cell_count": len(grid_rows),
            "ndvi": {"mean": round(float(np.mean(ndvi_vals)), 3), "min": round(float(np.min(ndvi_vals)), 3), "max": round(float(np.max(ndvi_vals)), 3)} if ndvi_vals else None,
            "ndmi": {"mean": round(float(np.mean(ndmi_vals)), 3), "min": round(float(np.min(ndmi_vals)), 3), "max": round(float(np.max(ndmi_vals)), 3)} if ndmi_vals else None,
            "ndre": {"mean": round(float(np.mean(ndre_vals)), 3), "min": round(float(np.min(ndre_vals)), 3), "max": round(float(np.max(ndre_vals)), 3)} if ndre_vals else None,
            "evi": {"mean": round(float(np.mean(evi_vals)), 3), "min": round(float(np.min(evi_vals)), 3), "max": round(float(np.max(evi_vals)), 3)} if evi_vals else None,
            "savi": {"mean": round(float(np.mean(savi_vals)), 3), "min": round(float(np.min(savi_vals)), 3), "max": round(float(np.max(savi_vals)), 3)} if savi_vals else None,
            "gci": {"mean": round(float(np.mean(gci_vals)), 3), "min": round(float(np.min(gci_vals)), 3), "max": round(float(np.max(gci_vals)), 3)} if gci_vals else None,
        }

        # 5. Generate Explainable Recommendations
        t2m = weather_data.get("t2m")
        rh2m = weather_data.get("rh2m")
        vpd = calculate_vpd(t2m, rh2m) if t2m is not None and rh2m is not None else None
        precip_7d = weather_data.get("precip_7d")
        lst = weather_data.get("lst_c")

        mean_ndvi = summary["ndvi"]["mean"] if summary["ndvi"] else None
        mean_ndmi = summary["ndmi"]["mean"] if summary["ndmi"] else None
        mean_ndre = summary["ndre"]["mean"] if summary["ndre"] else None

        recommendation = generate_explainable_recommendation(
            growth_stage=growth_stage,
            stress_assessment_enabled=growth_stage not in ("bare_soil", "harvested"),
            ndvi=mean_ndvi,
            ndmi=mean_ndmi,
            ndre=mean_ndre,
            evi=summary["evi"]["mean"] if summary["evi"] else None,
            savi=summary["savi"]["mean"] if summary["savi"] else None,
            gci=summary["gci"]["mean"] if summary["gci"] else None,
            vpd_kpa=vpd,
            t2m_c=t2m,
            lst_c=lst,
            precip_7d_mm=precip_7d,
            observation_date=obs_date_obj.isoformat(),
        )

        return {
            "field": {
                "id": field_row[0],
                "name": field_row[1],
                "crop_type": field_row[2],
                "area_sqm": field_row[3],
                "area_ha": field_row[4],
                "perimeter_m": field_row[5],
                "location": {
                    "village": field_row[6],
                    "mandal": field_row[7],
                    "district": field_row[8],
                    "state": field_row[9],
                    "country": field_row[10],
                },
                "geometry": field_geom,
                "centroid": centroid_geom,
                "geometry_quality_score": field_row[11],
                "registration_confidence": field_row[12],
            },
            "observation": {
                "date": obs_date_obj.isoformat(),
                "satellite_source": obs_meta[0] if obs_meta else "Sentinel-2 L2A",
                "stac_item_id": obs_meta[1] if obs_meta else None,
                "tile_id": obs_meta[2] if obs_meta else None,
                "cloud_cover_pct": obs_meta[3] if obs_meta else None,
                "quality_score": quality_score,
                "confidence_score": confidence_score,
                "growth_stage": growth_stage,
                "weather": weather_data,
                "vpd_kpa": round(vpd, 2) if vpd is not None else None,
            },
            "summary": summary,
            "recommendation": {
                "id": recommendation.id,
                "severity": recommendation.severity,
                "headline": recommendation.headline,
                "summary": recommendation.pre_cause_summary,
                "confidence": recommendation.confidence,
                "evidence": [
                    {
                        "metric": e.metric,
                        "observed_value": e.observed_value,
                        "baseline_value": e.baseline_value,
                        "anomaly": e.anomaly_magnitude,
                        "source": e.sensor_or_source,
                        "date": e.observation_date,
                        "rationale": e.scientific_rationale,
                    }
                    for e in recommendation.evidence
                ],
                "actions": recommendation.actionable_steps,
                "traceability": recommendation.traceability,
            },
            "grids": {
                "type": "FeatureCollection",
                "features": features,
            },
            "scientific_registry": {
                k: {
                    "name": v.name,
                    "acronym": v.acronym,
                    "formula": v.formula,
                    "citation": v.citation,
                    "bands": v.bands_used,
                    "thresholds": v.thresholds,
                }
                for k, v in INDEX_REGISTRY.items()
            },
        }


async def compare_temporal_dates(
    field_id: int,
    date_a: str,
    date_b: str,
) -> dict[str, Any]:
    """Compare biophysical conditions between Date A (Reference) and Date B (Comparison).
    
    Calculates cell-level deltas for all 6 indices, classifying trajectories:
    Improved (delta >= +0.05), Stable (-0.05 to +0.05), Declined (delta <= -0.05).
    """
    state_a = await get_field_state_at_date(field_id, date_a)
    state_b = await get_field_state_at_date(field_id, date_b)

    features_a = {f["properties"]["grid_id"]: f for f in state_a["grids"]["features"]}
    features_b = {f["properties"]["grid_id"]: f for f in state_b["grids"]["features"]}

    diff_features = []
    improved_count = 0
    stable_count = 0
    declined_count = 0

    for gid, fb in features_b.items():
        fa = features_a.get(gid)
        if not fa:
            continue

        idx_a = fa["properties"]["indices"]
        idx_b = fb["properties"]["indices"]

        def _delta(kb: str) -> Optional[float]:
            va = idx_a.get(kb)
            vb = idx_b.get(kb)
            if va is not None and vb is not None and math.isfinite(va) and math.isfinite(vb):
                return round(vb - va, 3)
            return None

        ndvi_delta = _delta("ndvi")
        ndmi_delta = _delta("ndmi")
        ndre_delta = _delta("ndre")
        evi_delta = _delta("evi")
        savi_delta = _delta("savi")
        gci_delta = _delta("gci")

        # Classify trajectory based on NDVI & NDRE
        status = "stable"
        color = "#eab308"
        if ndvi_delta is not None:
            if ndvi_delta >= 0.05:
                status = "improved"
                color = "#22c55e"
                improved_count += 1
            elif ndvi_delta <= -0.05:
                status = "declined"
                color = "#ef4444"
                declined_count += 1
            else:
                stable_count += 1
        else:
            stable_count += 1

        diff_features.append({
            "type": "Feature",
            "geometry": fb["geometry"],
            "properties": {
                "grid_id": gid,
                "status": status,
                "color": color,
                "deltas": {
                    "ndvi": ndvi_delta,
                    "ndmi": ndmi_delta,
                    "ndre": ndre_delta,
                    "evi": evi_delta,
                    "savi": savi_delta,
                    "gci": gci_delta,
                },
                "reference": idx_a,
                "comparison": idx_b,
            },
        })

    days_between = abs((datetime.fromisoformat(date_b).date() - datetime.fromisoformat(date_a).date()).days)

    return {
        "field_id": field_id,
        "date_a": date_a,
        "date_b": date_b,
        "days_between": days_between,
        "trajectory_summary": {
            "improved_cells": improved_count,
            "stable_cells": stable_count,
            "declined_cells": declined_count,
            "total_cells": len(diff_features),
        },
        "mean_deltas": {
            "ndvi": round(state_b["summary"]["ndvi"]["mean"] - state_a["summary"]["ndvi"]["mean"], 3) if state_b["summary"]["ndvi"] and state_a["summary"]["ndvi"] else None,
            "ndmi": round(state_b["summary"]["ndmi"]["mean"] - state_a["summary"]["ndmi"]["mean"], 3) if state_b["summary"]["ndmi"] and state_a["summary"]["ndmi"] else None,
            "ndre": round(state_b["summary"]["ndre"]["mean"] - state_a["summary"]["ndre"]["mean"], 3) if state_b["summary"]["ndre"] and state_a["summary"]["ndre"] else None,
        },
        "diff_grids": {
            "type": "FeatureCollection",
            "features": diff_features,
        },
    }
