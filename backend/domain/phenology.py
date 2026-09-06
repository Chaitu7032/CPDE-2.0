"""
CPDE v2 Crop Growth Stage & Phenology Engine
Determines the biological growth stage and dynamically gates false stress alerts.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Literal
import math

GrowthStage = Literal[
    "bare_soil",
    "emergence",
    "vegetative",
    "flowering_reproductive",
    "maturity_senescence",
    "harvested",
]


@dataclass(frozen=True)
class PhenologyResult:
    stage: GrowthStage
    label: str
    stress_assessment_enabled: bool
    gdd_accumulated: Optional[float]
    confidence: float
    explanation: str


def calculate_daily_gdd(t_max: float, t_min: float, t_base: float = 10.0, t_cutoff: float = 30.0) -> float:
    """Calculate daily Growing Degree Days (GDD).
    Default base temp 10°C (standard warm season crops like maize/sorghum/cotton).
    """
    t_max_adj = min(max(t_max, t_base), t_cutoff)
    t_min_adj = min(max(t_min, t_base), t_cutoff)
    mean_t = (t_max_adj + t_min_adj) / 2.0
    return max(mean_t - t_base, 0.0)


def determine_growth_stage(
    ndvi_current: Optional[float],
    ndvi_historical: list[tuple[date, float]],
    sowing_date: Optional[date] = None,
    observation_date: Optional[date] = None,
    accumulated_gdd: Optional[float] = None,
) -> PhenologyResult:
    """Determine the crop growth stage and determine if stress monitoring should be active.
    
    If harvested or bare soil: disables stress assessment to prevent panic alerts.
    """
    if ndvi_current is None or not math.isfinite(ndvi_current):
        return PhenologyResult(
            stage="bare_soil",
            label="Bare Soil / Masked",
            stress_assessment_enabled=False,
            gdd_accumulated=accumulated_gdd,
            confidence=0.5,
            explanation="Insufficient spectral response to assess canopy.",
        )

    # Sort historical NDVI by date
    clean_history = sorted([(d, v) for d, v in ndvi_historical if v is not None and math.isfinite(v)], key=lambda x: x[0])

    peak_ndvi = max([v for _, v in clean_history], default=ndvi_current) if clean_history else ndvi_current

    # 1. Harvest detection: If field previously had high canopy (>0.50) and drops suddenly by >0.30 within <=15 days
    if clean_history and len(clean_history) >= 2:
        last_date, last_ndvi = clean_history[-1]
        days_since_last = (observation_date - last_date).days if observation_date and last_date else 10
        if days_since_last <= 20 and last_ndvi >= 0.50 and ndvi_current < 0.25:
            return PhenologyResult(
                stage="harvested",
                label="Harvested / Fallow",
                stress_assessment_enabled=False,
                gdd_accumulated=accumulated_gdd,
                confidence=0.92,
                explanation="Rapid canopy collapse from peak (>0.50 to <0.25) within observation window indicates crop harvest. Stress alerts disabled.",
            )

    # 2. Bare soil
    if ndvi_current < 0.18:
        return PhenologyResult(
            stage="bare_soil",
            label="Bare Soil / Pre-Planting",
            stress_assessment_enabled=False,
            gdd_accumulated=accumulated_gdd,
            confidence=0.88,
            explanation="NDVI < 0.18 corresponds to background soil or pre-emergence land preparation. Stress alerts suppressed.",
        )

    # 3. Emergence
    if ndvi_current < 0.32:
        return PhenologyResult(
            stage="emergence",
            label="Early Emergence / Seedling",
            stress_assessment_enabled=True,
            gdd_accumulated=accumulated_gdd,
            confidence=0.82,
            explanation="NDVI between 0.18 and 0.32 indicates early seedling canopy development.",
        )

    # 4. Vegetative growth
    if ndvi_current < 0.65:
        # Check if declining from higher peak
        if peak_ndvi > 0.70 and ndvi_current < 0.50:
            return PhenologyResult(
                stage="maturity_senescence",
                label="Maturation / Natural Senescence",
                stress_assessment_enabled=False,
                gdd_accumulated=accumulated_gdd,
                confidence=0.80,
                explanation="Declining vegetative index from peak maturity indicates expected physiological crop drying/senescence. Critical alarms muted.",
            )
        return PhenologyResult(
            stage="vegetative",
            label="Active Vegetative Growth",
            stress_assessment_enabled=True,
            gdd_accumulated=accumulated_gdd,
            confidence=0.90,
            explanation="Canopy expansion with rising NDVI (0.32 - 0.65). Prime window for pre-cause stress detection.",
        )

    # 5. Flowering / Peak Canopy
    return PhenologyResult(
        stage="flowering_reproductive",
        label="Peak Biomass / Reproductive",
        stress_assessment_enabled=True,
        gdd_accumulated=accumulated_gdd,
        confidence=0.94,
        explanation="High canopy closure (NDVI >= 0.65). Full photosynthetic potential; high vulnerability to moisture stress.",
    )
