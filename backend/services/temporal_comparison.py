from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text

from backend.db.connection import async_session


DEFAULT_HISTORY_WINDOW_DAYS = 30
DEFAULT_COMPARISON_TOLERANCE_DAYS = 5


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _format_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _compute_vpd(t2m_c: float | None, rh_pct: float | None) -> float | None:
    if t2m_c is None or rh_pct is None:
        return None
    if not math.isfinite(t2m_c) or not math.isfinite(rh_pct):
        return None
    if rh_pct < 0 or rh_pct > 100:
        return None

    saturation_vapor_pressure = 0.6108 * math.exp((17.27 * t2m_c) / (t2m_c + 237.3))
    vapor_pressure_deficit = saturation_vapor_pressure * (1 - (rh_pct / 100.0))
    return max(vapor_pressure_deficit, 0.0)


def _pick_exact_point(points: list[dict[str, Any]], target_date: date) -> dict[str, Any] | None:
    for point in points:
        if point.get("date") == target_date:
            return dict(point)
    return None


def _pick_nearest_point(points: list[dict[str, Any]], target_date: date, tolerance_days: int) -> dict[str, Any] | None:
    best_point: dict[str, Any] | None = None
    best_distance: int | None = None

    for point in points:
        point_date = point.get("date")
        if not isinstance(point_date, date):
            continue

        distance = abs((point_date - target_date).days)
        if distance > tolerance_days:
            continue

        if best_point is None or best_distance is None or distance < best_distance or (
            distance == best_distance and point_date < best_point["date"]
        ):
            best_point = point
            best_distance = distance

    if best_point is None:
        return None

    picked = dict(best_point)
    picked["distance_days"] = best_distance
    return picked


def _serialize_point(point: dict[str, Any] | None) -> dict[str, Any] | None:
    if point is None:
        return None

    serialized: dict[str, Any] = {
        "date": _format_date(point.get("date")),
        "value": point.get("value"),
        "sample_count": point.get("sample_count"),
    }

    for key in ("t2m", "rh2m", "prectotcorr", "distance_days"):
        if key in point and point[key] is not None:
            serialized[key] = point[key]

    return serialized


def _build_trend(history: list[dict[str, Any]], reference_point: dict[str, Any] | None) -> dict[str, Any] | None:
    if reference_point is None or reference_point.get("value") is None:
        return None

    historical_points = [point for point in history if point.get("value") is not None and point.get("date")]
    if len(historical_points) < 2:
        return None

    baseline_point = historical_points[0]
    baseline_value = baseline_point.get("value")
    current_value = reference_point.get("value")
    if baseline_value is None or current_value is None:
        return None

    delta = current_value - baseline_value
    percent = None
    if baseline_value != 0:
        percent = (delta / abs(baseline_value)) * 100.0

    if percent is None:
        direction = None
        label = "Trend unavailable"
    elif percent > 2:
        direction = "up"
        label = "Rising"
    elif percent < -2:
        direction = "down"
        label = "Falling"
    else:
        direction = "flat"
        label = "Flat"

    return {
        "baseline_date": _format_date(baseline_point["date"]),
        "baseline_value": baseline_value,
        "delta": delta,
        "percent": percent,
        "direction": direction,
        "label": label,
    }


async def _fetch_ndvi_series(session, land_id: int, start_date: date, end_date: date) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            "SELECT date, AVG(ndvi) AS value, COUNT(ndvi) AS sample_count "
            "FROM land_daily_indices "
            "WHERE land_id = :lid AND date BETWEEN :start_date AND :end_date AND ndvi IS NOT NULL "
            "GROUP BY date ORDER BY date"
        ),
        {"lid": land_id, "start_date": start_date, "end_date": end_date},
    )
    return [
        {
            "date": row["date"],
            "value": _as_float(row["value"]),
            "sample_count": int(row["sample_count"] or 0),
        }
        for row in result.mappings().all()
    ]


async def _fetch_ndmi_series(session, land_id: int, start_date: date, end_date: date) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            "SELECT date, AVG(ndmi) AS value, COUNT(ndmi) AS sample_count "
            "FROM land_daily_indices "
            "WHERE land_id = :lid AND date BETWEEN :start_date AND :end_date AND ndmi IS NOT NULL "
            "GROUP BY date ORDER BY date"
        ),
        {"lid": land_id, "start_date": start_date, "end_date": end_date},
    )
    return [
        {
            "date": row["date"],
            "value": _as_float(row["value"]),
            "sample_count": int(row["sample_count"] or 0),
        }
        for row in result.mappings().all()
    ]


async def _fetch_lst_series(session, land_id: int, start_date: date, end_date: date) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            "SELECT date, AVG(lst_c) AS value, COUNT(lst_c) AS sample_count "
            "FROM land_daily_lst "
            "WHERE land_id = :lid AND date BETWEEN :start_date AND :end_date AND lst_c IS NOT NULL "
            "GROUP BY date ORDER BY date"
        ),
        {"lid": land_id, "start_date": start_date, "end_date": end_date},
    )
    return [
        {
            "date": row["date"],
            "value": _as_float(row["value"]),
            "sample_count": int(row["sample_count"] or 0),
        }
        for row in result.mappings().all()
    ]


async def _fetch_vpd_series(session, land_id: int, start_date: date, end_date: date) -> list[dict[str, Any]]:
    result = await session.execute(
        text(
            "SELECT date, AVG(t2m) AS t2m, AVG(rh2m) AS rh2m, AVG(prectotcorr) AS prectotcorr, COUNT(*) AS sample_count "
            "FROM land_daily_weather "
            "WHERE land_id = :lid AND date BETWEEN :start_date AND :end_date "
            "GROUP BY date ORDER BY date"
        ),
        {"lid": land_id, "start_date": start_date, "end_date": end_date},
    )

    series: list[dict[str, Any]] = []
    for row in result.mappings().all():
        t2m = _as_float(row["t2m"])
        rh2m = _as_float(row["rh2m"])
        series.append(
            {
                "date": row["date"],
                "value": _compute_vpd(t2m, rh2m),
                "sample_count": int(row["sample_count"] or 0),
                "t2m": t2m,
                "rh2m": rh2m,
                "prectotcorr": _as_float(row["prectotcorr"]),
            }
        )

    return series


def _confidence_label(available_metrics: int, total_metrics: int) -> str:
    if total_metrics <= 0 or available_metrics <= 0:
        return "None"
    ratio = available_metrics / total_metrics
    if ratio >= 0.75:
        return "High"
    if ratio >= 0.5:
        return "Medium"
    return "Low"


async def build_temporal_analysis(
    land_id: int,
    active_date: date,
    comparison_date: date | None = None,
    *,
    history_window_days: int = DEFAULT_HISTORY_WINDOW_DAYS,
    comparison_tolerance_days: int = DEFAULT_COMPARISON_TOLERANCE_DAYS,
) -> dict[str, Any]:
    history_window_days = max(1, min(int(history_window_days), 90))
    comparison_tolerance_days = max(1, min(int(comparison_tolerance_days), 30))

    history_start = active_date - timedelta(days=history_window_days)

    async with async_session() as session:
        land_res = await session.execute(text("SELECT 1 FROM lands WHERE land_id = :lid"), {"lid": land_id})
        if land_res.first() is None:
            raise HTTPException(status_code=404, detail="Land not found")

        ndvi_history = await _fetch_ndvi_series(session, land_id, history_start, active_date)
        ndmi_history = await _fetch_ndmi_series(session, land_id, history_start, active_date)
        lst_history = await _fetch_lst_series(session, land_id, history_start, active_date)
        vpd_history = await _fetch_vpd_series(session, land_id, history_start, active_date)

        metric_specs = [
            {
                "key": "ndvi",
                "label": "NDVI (Sentinel)",
                "source": "Sentinel-2",
                "unit": "index",
                "digits": 3,
                "history": ndvi_history,
            },
            {
                "key": "ndmi",
                "label": "NDMI (Sentinel)",
                "source": "Sentinel-2",
                "unit": "index",
                "digits": 3,
                "history": ndmi_history,
            },
            {
                "key": "lst",
                "label": "LST (MODIS)",
                "source": "MODIS",
                "unit": "deg C",
                "digits": 1,
                "history": lst_history,
            },
            {
                "key": "vpd",
                "label": "VPD (NASA POWER)",
                "source": "NASA POWER",
                "unit": "kPa",
                "digits": 3,
                "history": vpd_history,
            },
        ]

        comparison_history: dict[str, list[dict[str, Any]]] = {}
        if comparison_date is not None:
            comparison_window_start = comparison_date - timedelta(days=comparison_tolerance_days)
            comparison_window_end = comparison_date + timedelta(days=comparison_tolerance_days)
            comparison_history = {
                "ndvi": await _fetch_ndvi_series(session, land_id, comparison_window_start, comparison_window_end),
                "ndmi": await _fetch_ndmi_series(session, land_id, comparison_window_start, comparison_window_end),
                "lst": await _fetch_lst_series(session, land_id, comparison_window_start, comparison_window_end),
                "vpd": await _fetch_vpd_series(session, land_id, comparison_window_start, comparison_window_end),
            }

    metrics: list[dict[str, Any]] = []
    available_metrics = 0
    for spec in metric_specs:
        history = spec["history"]
        reference_point = _pick_exact_point(history, active_date)
        comparison_point = None
        status = "ready"
        message: str | None = None

        if reference_point is None:
            status = "missing_reference"
            message = "No data at the reference date in the stored series."
        elif comparison_date is not None:
            comparison_candidates = comparison_history.get(spec["key"], [])
            comparison_point = _pick_nearest_point(comparison_candidates, comparison_date, comparison_tolerance_days)
            if comparison_point is None:
                status = "missing_comparison"
                message = "No data near the selected comparison date."

        if status == "ready":
            available_metrics += 1

        change = None
        if reference_point is not None and comparison_point is not None and reference_point.get("value") is not None and comparison_point.get("value") is not None:
            absolute_change = reference_point["value"] - comparison_point["value"]
            percent_change = None
            if comparison_point["value"] != 0:
                percent_change = (absolute_change / abs(comparison_point["value"])) * 100.0
            change = {
                "absolute": absolute_change,
                "percent": percent_change,
            }

        trend = _build_trend(history, reference_point)

        metrics.append(
            {
                "key": spec["key"],
                "label": spec["label"],
                "source": spec["source"],
                "unit": spec["unit"],
                "digits": spec["digits"],
                "status": status,
                "message": message,
                "reference": _serialize_point(reference_point),
                "comparison": _serialize_point(comparison_point),
                "change": change,
                "trend": trend,
                "history": [_serialize_point(point) for point in history],
            }
        )

    confidence = {
        "available_metrics": available_metrics,
        "total_metrics": len(metric_specs),
        "label": _confidence_label(available_metrics, len(metric_specs)),
    }

    warnings: list[str] = []
    if comparison_date is not None:
        warnings.append(
            f"Nearest matches are selected independently per source within +/-{comparison_tolerance_days} days."
        )
    warnings.append("Historical analysis is read-only and does not modify dashboard state.")

    return {
        "land_id": land_id,
        "reference_date": _format_date(active_date),
        "comparison_date": _format_date(comparison_date),
        "analysis_mode": "comparison" if comparison_date is not None else "historical",
        "history_window_days": history_window_days,
        "comparison_tolerance_days": comparison_tolerance_days,
        "confidence": confidence,
        "metrics": metrics,
        "warnings": warnings,
    }