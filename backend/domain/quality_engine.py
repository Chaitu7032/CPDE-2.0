"""
CPDE v2 Data Quality & Observation Confidence Engine
Ensures no observation is processed or presented without rigorous metadata assessment.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
import math


@dataclass(frozen=True)
class QualityAssessment:
    quality_score: float      # 0.0 - 100.0
    confidence_score: float   # 0.0 - 100.0
    grade: str                # Excellent, High, Moderate, Low, Reject
    cloud_penalty: float
    age_penalty: float
    band_completeness: float
    is_usable: bool
    reasons: list[str]


def compute_observation_quality(
    cloud_cover_pct: Optional[float],
    observation_date: date,
    reference_date: Optional[date] = None,
    available_bands: Optional[list[str]] = None,
    scl_clear_pct: Optional[float] = None,
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

    # 2. Observation Age Penalty (max 30 pts)
    age_days = max((today - observation_date).days, 0)
    if age_days <= 3:
        age_penalty = 0.0
        reasons.append(f"Near real-time capture ({age_days} days old)")
    elif age_days <= 7:
        age_penalty = 5.0
        reasons.append(f"Recent observation ({age_days} days old)")
    elif age_days <= 14:
        age_penalty = 12.0
        reasons.append(f"Moderate observation latency ({age_days} days old)")
    elif age_days <= 30:
        age_penalty = 22.0
        reasons.append(f"Historical capture ({age_days} days old)")
    else:
        age_penalty = 30.0
        reasons.append(f"Archival observation ({age_days} days old)")

    # 3. Band Completeness (max 30 pts)
    required_bands = {"B02", "B03", "B04", "B05", "B08", "B11"}
    present_bands = set(available_bands or ["B02", "B03", "B04", "B05", "B08", "B11"])
    fraction = len(present_bands.intersection(required_bands)) / len(required_bands)
    band_penalty = (1.0 - fraction) * 30.0

    if fraction == 1.0:
        reasons.append("Complete multispectral stack available (B02, B03, B04, B05, B08, B11)")
    else:
        reasons.append(f"Partial band coverage ({len(present_bands.intersection(required_bands))}/6 bands present)")

    # Raw quality score
    raw_quality = 100.0 - (cloud_penalty + age_penalty + band_penalty)
    quality_score = max(min(round(raw_quality, 1), 100.0), 0.0)

    # Confidence score integrates spatial consistency and data quality
    confidence_score = max(min(round(quality_score * (0.8 + 0.2 * fraction), 1), 100.0), 0.0)

    if quality_score >= 85.0:
        grade = "Excellent"
        is_usable = True
    elif quality_score >= 70.0:
        grade = "High"
        is_usable = True
    elif quality_score >= 50.0:
        grade = "Moderate"
        is_usable = True
    elif quality_score >= 30.0:
        grade = "Low"
        is_usable = True
    else:
        grade = "Reject"
        is_usable = False
        reasons.append("Observation quality below minimum scientific threshold (< 30%).")

    return QualityAssessment(
        quality_score=quality_score,
        confidence_score=confidence_score,
        grade=grade,
        cloud_penalty=cloud_penalty,
        age_penalty=age_penalty,
        band_completeness=round(fraction * 100.0, 1),
        is_usable=is_usable,
        reasons=reasons,
    )
