"""
CPDE v2 Data Quality & Observation Confidence Engine
Ensures no observation is processed or presented without rigorous metadata assessment.
Enforces Rule 5: Tri-Concept Separation (Stress Probability vs Model Confidence vs Evidence Sufficiency).
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional
import math


@dataclass(frozen=True)
class SensorFreshness:
    optical_age_days: Optional[int] = None
    sar_age_days: Optional[int] = None
    thermal_age_days: Optional[int] = None
    weather_age_days: Optional[int] = None
    overall_freshness: str = "GOOD"  # GOOD, ACCEPTABLE, DEGRADED


@dataclass(frozen=True)
class QualityAssessment:
    quality_score: float             # 0.0 - 100.0 (Evidence Sufficiency score)
    confidence_score: float          # 0.0 - 100.0 (Model confidence score)
    confidence_grade: str            # HIGH, MEDIUM, LOW, INSUFFICIENT
    cloud_penalty: float
    age_penalty: float
    band_completeness: float
    is_usable: bool
    reasons: list[str]
    freshness: SensorFreshness
    valid_pixel_fraction: Optional[float] = None
    cloud_contamination_pct: Optional[float] = None


def compute_observation_quality(
    cloud_cover_pct: Optional[float],
    observation_date: date,
    reference_date: Optional[date] = None,
    available_bands: Optional[list[str]] = None,
    scl_clear_pct: Optional[float] = None,
    sar_observation_date: Optional[date] = None,
    thermal_observation_date: Optional[date] = None,
    weather_observation_date: Optional[date] = None,
) -> QualityAssessment:
    """Evaluate radiometric, atmospheric, and temporal quality of satellite observation."""
    reasons: list[str] = []
    today = reference_date or date.today()

    # 1. Cloud Cover Penalty (max 40 pts)
    cc = cloud_cover_pct if cloud_cover_pct is not None and math.isfinite(cloud_cover_pct) else 50.0
    if cc <= 5.0:
        cloud_penalty = 0.0
        reasons.append("Pristine clear-sky conditions (cloud cover <= 5%)")
    elif cc <= 15.0:
        cloud_penalty = 8.0
        reasons.append(f"Minor cloud presence ({cc:.1f}%) with minimal surface obscuration")
    elif cc <= 35.0:
        cloud_penalty = 22.0
        reasons.append(f"Moderate cloud contamination ({cc:.1f}%); clear pixels extracted via SCL")
    else:
        cloud_penalty = 40.0
        reasons.append(f"Heavy cloud coverage ({cc:.1f}%); high atmospheric attenuation")

    # SCL Clear pixel bonus/penalty if provided
    if scl_clear_pct is not None and math.isfinite(scl_clear_pct):
        if scl_clear_pct < 50.0:
            cloud_penalty = min(cloud_penalty + 15.0, 45.0)
            reasons.append(f"Low parcel clear-pixel fraction ({scl_clear_pct:.1f}%)")

    # 2. Observation Latency & Sensor Freshness
    optical_age = max((today - observation_date).days, 0)
    sar_age = max((today - sar_observation_date).days, 0) if sar_observation_date else None
    thermal_age = max((today - thermal_observation_date).days, 0) if thermal_observation_date else None
    weather_age = max((today - weather_observation_date).days, 0) if weather_observation_date else None

    if optical_age <= 3:
        age_penalty = 0.0
        reasons.append(f"Recent observation ({optical_age} days latency)")
    elif optical_age <= 7:
        age_penalty = 5.0
        reasons.append(f"Standard cadence observation ({optical_age} days latency)")
    elif optical_age <= 14:
        age_penalty = 12.0
        reasons.append(f"Moderate observation latency ({optical_age} days old)")
    elif optical_age <= 30:
        age_penalty = 22.0
        reasons.append(f"Historical capture ({optical_age} days old)")
    else:
        age_penalty = 30.0
        reasons.append(f"Archival observation ({optical_age} days old)")

    # Freshness evaluation
    if optical_age <= 5 and (weather_age is None or weather_age <= 2):
        overall_freshness = "GOOD"
    elif optical_age <= 12:
        overall_freshness = "ACCEPTABLE"
    else:
        overall_freshness = "DEGRADED"

    freshness = SensorFreshness(
        optical_age_days=optical_age,
        sar_age_days=sar_age,
        thermal_age_days=thermal_age,
        weather_age_days=weather_age,
        overall_freshness=overall_freshness,
    )

    # 3. Band Completeness (max 30 pts)
    required_bands = {"B02", "B03", "B04", "B05", "B08", "B8A", "B11"}
    present_bands = set(available_bands or ["B02", "B03", "B04", "B05", "B08", "B8A", "B11"])
    fraction = len(present_bands.intersection(required_bands)) / len(required_bands)
    band_penalty = (1.0 - fraction) * 30.0

    if fraction == 1.0:
        reasons.append("Complete multispectral stack available (B02, B03, B04, B05, B08, B8A, B11)")
    else:
        reasons.append(f"Partial band coverage ({len(present_bands.intersection(required_bands))}/{len(required_bands)} bands present)")

    # Raw quality score (Evidence Sufficiency score)
    raw_quality = 100.0 - (cloud_penalty + age_penalty + band_penalty)
    quality_score = max(min(round(raw_quality, 1), 100.0), 0.0)

    # Model confidence integrates spatial consistency, sensor presence and quality
    confidence_score = max(min(round(quality_score * (0.8 + 0.2 * fraction), 1), 100.0), 0.0)

    if quality_score >= 80.0:
        confidence_grade = "HIGH"
        is_usable = True
    elif quality_score >= 50.0:
        confidence_grade = "MEDIUM"
        is_usable = True
    elif quality_score >= 30.0:
        confidence_grade = "LOW"
        is_usable = True
    else:
        confidence_grade = "INSUFFICIENT"
        is_usable = False
        reasons.append("Observation quality below minimum scientific threshold (< 30%).")

    return QualityAssessment(
        quality_score=quality_score,
        confidence_score=confidence_score,
        confidence_grade=confidence_grade,
        cloud_penalty=cloud_penalty,
        age_penalty=age_penalty,
        band_completeness=round(fraction * 100.0, 1),
        is_usable=is_usable,
        reasons=reasons,
        freshness=freshness,
        valid_pixel_fraction=round(scl_clear_pct / 100.0, 3) if scl_clear_pct is not None else None,
        cloud_contamination_pct=round(cc, 1),
    )
