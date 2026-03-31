from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import math

from sqlalchemy import text

from backend.db.connection import async_session


LAND_LEVEL_GRID_ID = "__land__"


def _sigmoid(x: float) -> float:
    # numerically stable-ish for moderate magnitudes
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


@dataclass(frozen=True)
class RiskWeights:
    intercept: float = -0.2
    ndvi: float = 0.7
    ndmi: float = 0.9
    lst: float = 0.8
    t2m: float = 0.3
    prectotcorr: float = 0.4


async def compute_risk_for_land_date(
    land_id,
    date: str,
    *,
    weights: RiskWeights | None = None,
) -> dict[str, Any]:
    """Compute an interpretable, continuous risk score for each grid cell.

    Uses z-score anomalies (Phase 6 outputs). No hard thresholds:
    - Greenness/moisture stress increases as NDVI/NDMI z-scores decrease
    - Thermal stress increases as LST/T2M z-scores increase
    - Drought stress increases as PRECTOTCORR z-score decreases

    Returns:
      - land_summary: mean/p90 risk, counts
      - grid_risks: per grid_id risk + contributions
      - land_level_anomalies: weather z-scores used
    """
    w = weights or RiskWeights()
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    date_obj = datetime.fromisoformat(date).date()

    async with async_session() as session:
        grids_res = await session.execute(
            text("SELECT grid_id::text AS grid_id, COALESCE(is_water, FALSE) FROM land_grid_cells WHERE land_id = :lid ORDER BY grid_id"),
            {"lid": land_id},
        )
        grids = [(r[0], bool(r[1])) for r in grids_res.fetchall()]
        if not grids:
            return {"processed": 0, "reason": "no grids for land"}

        # Use the LATEST available anomaly for each variable+grid combination.
        # Satellite products (Sentinel-2, MODIS) have different acquisition dates
        # so a single-date join would miss data stored on earlier dates.
        anom_res = await session.execute(
            text(
                "SELECT DISTINCT ON (grid_id, variable) grid_id, variable, zscore, value "
                "FROM land_anomalies "
                "WHERE land_id = :lid AND variable IN ('ndvi','ndmi','lst') "
                "AND date <= :dt "
                "ORDER BY grid_id, variable, date DESC"
            ),
            {"lid": land_id, "dt": date_obj},
        )
        anom_rows = anom_res.fetchall()

        land_anom_res = await session.execute(
            text(
                "SELECT DISTINCT ON (variable) variable, zscore, value "
                "FROM land_anomalies "
                "WHERE land_id = :lid AND grid_id = :gid "
                "AND variable IN ('t2m','prectotcorr','rh2m') "
                "AND date <= :dt "
                "ORDER BY variable, date DESC"
            ),
            {"lid": land_id, "gid": LAND_LEVEL_GRID_ID, "dt": date_obj},
        )
        land_anom_rows = land_anom_res.fetchall()

    z_by_grid: dict[str, dict[str, float | None]] = {}
    val_by_grid: dict[str, dict[str, float | None]] = {}
    for gid, var, z, v in anom_rows:
        z_by_grid.setdefault(gid, {})[str(var)] = float(z) if z is not None else None
        val_by_grid.setdefault(gid, {})[str(var)] = float(v) if v is not None else None

    land_level_z: dict[str, float | None] = {}
    land_level_val: dict[str, float | None] = {}
    for var, z, v in land_anom_rows:
        land_level_z[str(var)] = float(z) if z is not None else None
        land_level_val[str(var)] = float(v) if v is not None else None

    t2m_z = land_level_z.get("t2m")
    prectot_z = land_level_z.get("prectotcorr")

    # ── Reference baselines for bootstrapping mode ─────────────────────────────
    # Used ONLY when real climatology is unavailable (z-score is NULL but raw
    # value exists). Values are defensible agricultural crop-science references.
    # As more data accumulates, real z-scores replace these.
    REF = {
        "ndvi": (0.50, 0.25),   # (mean, std) — healthy cropland baseline
        "ndmi": (0.10, 0.25),   # moisture baseline
        "lst":  (28.0,  8.0),   # comfortable land surface temp (°C)
        "t2m":  (28.0,  5.0),   # 2 m air temperature (°C)
        "prectotcorr": (2.0, 2.5),  # daily precipitation (mm)
    }

    def _effective_z(var: str, z: float | None, val: float | None) -> float | None:
        """Return real z if available, else synthetic z from reference baseline."""
        if z is not None:
            return z
        if val is None:
            return None
        ref_mean, ref_std = REF.get(var, (0.0, 1.0))
        return (val - ref_mean) / ref_std

    grid_risks: list[dict[str, Any]] = []
    risks: list[float] = []

    for grid_id, is_water in grids:
        z = z_by_grid.get(grid_id, {})
        val = val_by_grid.get(grid_id, {})
        ndvi_z = _effective_z("ndvi", z.get("ndvi"), val.get("ndvi"))
        ndmi_z = _effective_z("ndmi", z.get("ndmi"), val.get("ndmi"))
        lst_z  = _effective_z("lst", z.get("lst"), val.get("lst"))
        t2m_z_eff  = _effective_z("t2m", t2m_z, land_level_val.get("t2m"))
        prectot_z_eff = _effective_z("prectotcorr", prectot_z, land_level_val.get("prectotcorr"))

        # Partial scoring: only include variables that have data.
        # Missing variables are excluded from both numerator and
        # denominator so they do not inflate or dilute the score.
        terms: list[tuple[str, float]] = []
        total_weight = 0.0

        if ndvi_z is not None:
            terms.append(("ndvi", w.ndvi * (-float(ndvi_z))))
            total_weight += w.ndvi
        if ndmi_z is not None:
            terms.append(("ndmi", w.ndmi * (-float(ndmi_z))))
            total_weight += w.ndmi
        if lst_z is not None:
            terms.append(("lst", w.lst * float(lst_z)))
            total_weight += w.lst
        if t2m_z_eff is not None:
            terms.append(("t2m", w.t2m * float(t2m_z_eff)))
            total_weight += w.t2m
        if prectot_z_eff is not None:
            terms.append(("prectotcorr", w.prectotcorr * (-float(prectot_z_eff))))
            total_weight += w.prectotcorr

        contrib = {k: v for k, v in terms}
        # Fill missing keys so downstream consumers always see all variables.
        for key in ("ndvi", "ndmi", "lst", "t2m", "prectotcorr"):
            if key not in contrib:
                contrib[key] = 0.0

        # Re-scale the sum so that present variables fill the full weight
        # budget. This prevents a missing variable from artificially
        # lowering the risk (old behaviour: missing → 0 contribution).
        raw_sum = sum(v for _, v in terms)
        full_weight = w.ndvi + w.ndmi + w.lst + w.t2m + w.prectotcorr
        if total_weight > 0 and full_weight > 0:
            linear = w.intercept + raw_sum * (full_weight / total_weight)
        else:
            linear = w.intercept
        risk = _sigmoid(linear)

        # Optional: de-emphasize or null out water grids
        if is_water:
            risk_out: float | None = None
        else:
            risk_out = float(risk)
            risks.append(risk_out)

        drivers = sorted(contrib.items(), key=lambda kv: kv[1], reverse=True)
        top_drivers = [k for k, v in drivers[:3] if v > 0]

        grid_risks.append(
            {
                "grid_id": grid_id,
                "is_water": is_water,
                "risk": risk_out,
                "linear_score": float(linear),
                "z": {
                    "ndvi": z.get("ndvi"),
                    "ndmi": z.get("ndmi"),
                    "lst": z.get("lst"),
                    "t2m": t2m_z,
                    "prectotcorr": prectot_z,
                },
                "contributions": contrib,
                "top_drivers": top_drivers,
            }
        )

    if risks:
        risks_sorted = sorted(risks)
        p90 = risks_sorted[int(0.9 * (len(risks_sorted) - 1))]
        mean_risk = sum(risks) / len(risks)
    else:
        p90 = None
        mean_risk = None

    # ── Persist risk scores to stress_risk_forecast ───────────────────────────
    # This is the table the dashboard reads for per-grid risk display.
    if grid_risks:
        upsert_risk = text(
            "INSERT INTO stress_risk_forecast (land_id, grid_id, date, probability, model_version, created_at) "
            "VALUES (:land_id, :grid_id, :date, :probability, :model_version, CURRENT_DATE) "
            "ON CONFLICT (grid_id, date) DO UPDATE "
            "SET probability = EXCLUDED.probability, model_version = EXCLUDED.model_version, created_at = EXCLUDED.created_at"
        )
        async with async_session() as session:
            params = [
                {
                    "land_id": land_id,
                    "grid_id": gr["grid_id"],
                    "date": date_obj,
                    "probability": gr["risk"],
                    "model_version": "partial-weighted-v2",
                }
                for gr in grid_risks
                if not gr["is_water"]
            ]
            if params:
                await session.execute(upsert_risk, params)
                await session.commit()
    return {
        "processed": len(grid_risks),
        "date": date,
        "land_id": land_id,
        "land_level_anomalies": land_level_z,
        "land_summary": {
            "grid_count": len(grid_risks),
            "non_water_grid_count": len(risks),
            "mean_risk": mean_risk,
            "p90_risk": p90,
        },
        "grid_risks": grid_risks,
        "weights": {
            "intercept": w.intercept,
            "ndvi": w.ndvi,
            "ndmi": w.ndmi,
            "lst": w.lst,
            "t2m": w.t2m,
            "prectotcorr": w.prectotcorr,
        },
    }
