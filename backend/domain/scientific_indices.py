"""
CPDE v2 Scientific Index Engine
Implements peer-reviewed, reproducible spectral indices and biophysical metrics.
Never use empirical approximations without citations.
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Optional
import math


@dataclass(frozen=True)
class IndexDefinition:
    key: str
    name: str
    acronym: str
    formula: str
    citation: str
    bands_used: list[str]
    description: str
    min_valid: float
    max_valid: float
    thresholds: list[dict[str, Any]]


INDEX_REGISTRY: dict[str, IndexDefinition] = {
    "ndvi": IndexDefinition(
        key="ndvi",
        name="Normalized Difference Vegetation Index",
        acronym="NDVI",
        formula="(B08 - B04) / (B08 + B04)",
        citation="Rouse, J. W., Haas, R. H., Schell, J. A., & Deering, D. W. (1974). Monitoring vegetation systems in the Great Plains with ERTS. NASA SP-351, 309-317.",
        bands_used=["B08 (NIR - 842nm)", "B04 (Red - 665nm)"],
        description="Standard indicator of active photosynthetic biomass, chlorophyll density, and canopy greenness.",
        min_valid=-1.0,
        max_valid=1.0,
        thresholds=[
            {"category": "critical", "label": "Severe Stress / Bare Soil", "range": "< 0.20", "color": "#dc2626", "bg": "#fef2f2"},
            {"category": "warning", "label": "Stressed / Sparse Vegetation", "range": "0.20 - 0.40", "color": "#f97316", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Vigor", "range": "0.40 - 0.60", "color": "#eab308", "bg": "#fefce8"},
            {"category": "healthy", "label": "Healthy Canopy", "range": "0.60 - 0.75", "color": "#22c55e", "bg": "#f0fdf4"},
            {"category": "excellent", "label": "Dense / Peak Biomass", "range": ">= 0.75", "color": "#15803d", "bg": "#ecfdf5"},
        ],
    ),
    "ndmi": IndexDefinition(
        key="ndmi",
        name="Normalized Difference Moisture Index",
        acronym="NDMI",
        formula="(B08 - B11) / (B08 + B11)",
        citation="Gao, B. C. (1996). NDWI—A normalized difference water index for remote sensing of vegetation liquid water from space. Remote Sensing of Environment, 58(3), 257-266.",
        bands_used=["B08 (NIR - 842nm)", "B11 (SWIR - 1610nm)"],
        description="Measures equivalent water thickness in the spongy mesophyll of leaf tissues. Directly tracks canopy hydration.",
        min_valid=-1.0,
        max_valid=1.0,
        thresholds=[
            {"category": "critical", "label": "Severe Moisture Deficit", "range": "< -0.15", "color": "#b91c1c", "bg": "#fef2f2"},
            {"category": "warning", "label": "Water Stressed / Dry", "range": "-0.15 - 0.00", "color": "#ea580c", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Moisture", "range": "0.00 - 0.20", "color": "#0284c7", "bg": "#f0f9ff"},
            {"category": "healthy", "label": "Adequate Hydration", "range": "0.20 - 0.40", "color": "#2563eb", "bg": "#eff6ff"},
            {"category": "excellent", "label": "High Canopy Moisture", "range": ">= 0.40", "color": "#1d4ed8", "bg": "#eef2ff"},
        ],
    ),
    "ndre": IndexDefinition(
        key="ndre",
        name="Normalized Difference Red Edge Index",
        acronym="NDRE",
        formula="(B08 - B05) / (B08 + B05)",
        citation="Barnes, E. M., Clarke, T. R., & Richards, P. J. (2000). Coincident detection of crop water and nitrogen stress in corn. Proceedings of the 5th International Conference on Precision Agriculture.",
        bands_used=["B08 (NIR - 842nm)", "B05 (Red Edge 1 - 705nm)"],
        description="Early indicator of canopy nitrogen content and leaf chlorophyll decline. Detects pre-visual physiological stress before NDVI saturates or declines.",
        min_valid=-1.0,
        max_valid=1.0,
        thresholds=[
            {"category": "critical", "label": "Acute Chlorophyll Deficit", "range": "< 0.15", "color": "#991b1b", "bg": "#fef2f2"},
            {"category": "warning", "label": "Early Nutrient/Chlorophyll Stress", "range": "0.15 - 0.28", "color": "#c2410c", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Vigor", "range": "0.28 - 0.42", "color": "#d97706", "bg": "#fefce8"},
            {"category": "healthy", "label": "Healthy Nitrogen Status", "range": "0.42 - 0.55", "color": "#16a34a", "bg": "#f0fdf4"},
            {"category": "excellent", "label": "Optimal Chlorophyll Density", "range": ">= 0.55", "color": "#166534", "bg": "#ecfdf5"},
        ],
    ),
    "evi": IndexDefinition(
        key="evi",
        name="Enhanced Vegetation Index",
        acronym="EVI",
        formula="2.5 * ((B08 - B04) / (B08 + 6.0 * B04 - 7.5 * B02 + 1.0))",
        citation="Huete, A., Didan, K., Miura, T., Rodriguez, E. P., Gao, X., & Ferreira, L. G. (2002). Overview of the radiometric and biophysical performance of the MODIS vegetation indices. Remote Sensing of Environment, 83(1-2), 195-213.",
        bands_used=["B08 (NIR - 842nm)", "B04 (Red - 665nm)", "B02 (Blue - 490nm)"],
        description="Optimized vegetation signal with improved sensitivity in high biomass regions and reduced atmospheric and soil background interference.",
        min_valid=-1.0,
        max_valid=2.0,
        thresholds=[
            {"category": "critical", "label": "Low Biomass / Stress", "range": "< 0.15", "color": "#dc2626", "bg": "#fef2f2"},
            {"category": "warning", "label": "Sub-optimal Canopy", "range": "0.15 - 0.30", "color": "#f97316", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Canopy Density", "range": "0.30 - 0.45", "color": "#eab308", "bg": "#fefce8"},
            {"category": "healthy", "label": "Dense Healthy Foliage", "range": "0.45 - 0.65", "color": "#22c55e", "bg": "#f0fdf4"},
            {"category": "excellent", "label": "Lush Vegetative Vigour", "range": ">= 0.65", "color": "#15803d", "bg": "#ecfdf5"},
        ],
    ),
    "savi": IndexDefinition(
        key="savi",
        name="Soil-Adjusted Vegetation Index",
        acronym="SAVI",
        formula="((B08 - B04) / (B08 + B04 + L)) * (1.0 + L), where L = 0.5",
        citation="Huete, A. R. (1988). A soil-adjusted vegetation index (SAVI). Remote Sensing of Environment, 25(3), 295-309.",
        bands_used=["B08 (NIR - 842nm)", "B04 (Red - 665nm)"],
        description="Incorporates soil brightness correction factor (L=0.5) to minimize ground optical noise during early growth and low canopy cover.",
        min_valid=-1.0,
        max_valid=1.5,
        thresholds=[
            {"category": "critical", "label": "Bare / Stressed Soil", "range": "< 0.15", "color": "#dc2626", "bg": "#fef2f2"},
            {"category": "warning", "label": "Early Germination / Sparse", "range": "0.15 - 0.25", "color": "#ea580c", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Ground Cover", "range": "0.25 - 0.40", "color": "#ca8a04", "bg": "#fefce8"},
            {"category": "healthy", "label": "Good Emergence / Vigour", "range": "0.40 - 0.55", "color": "#16a34a", "bg": "#f0fdf4"},
            {"category": "excellent", "label": "Closed Canopy", "range": ">= 0.55", "color": "#15803d", "bg": "#ecfdf5"},
        ],
    ),
    "gci": IndexDefinition(
        key="gci",
        name="Green Chlorophyll Index",
        acronym="GCI",
        formula="(B08 / B03) - 1.0",
        citation="Gitelson, A. A., Viña, A., Ciganda, V., Rundquist, D. C., & Arkebauer, T. J. (2005). Remote estimation of canopy chlorophyll content in crops. Geophysical Research Letters, 32(8).",
        bands_used=["B08 (NIR - 842nm)", "B03 (Green - 560nm)"],
        description="Linear proxy for total canopy chlorophyll mass. Highly responsive to nitrogen fertilization response and senescence.",
        min_valid=-1.0,
        max_valid=15.0,
        thresholds=[
            {"category": "critical", "label": "Severe Chlorosis", "range": "< 1.0", "color": "#dc2626", "bg": "#fef2f2"},
            {"category": "warning", "label": "Low Chlorophyll", "range": "1.0 - 2.5", "color": "#f97316", "bg": "#fff7ed"},
            {"category": "moderate", "label": "Moderate Chlorophyll", "range": "2.5 - 4.5", "color": "#ca8a04", "bg": "#fefce8"},
            {"category": "healthy", "label": "Strong Chlorophyll Level", "range": "4.5 - 6.5", "color": "#16a34a", "bg": "#f0fdf4"},
            {"category": "excellent", "label": "Optimum Chlorophyll Peak", "range": ">= 6.5", "color": "#15803d", "bg": "#ecfdf5"},
        ],
    ),
}


def _safe_div(num: float, den: float, default: Optional[float] = None) -> Optional[float]:
    if not math.isfinite(num) or not math.isfinite(den) or abs(den) < 1e-6:
        return default
    val = num / den
    return val if math.isfinite(val) else default


def calculate_ndvi(b08: float, b04: float) -> Optional[float]:
    """Calculate NDVI = (B08 - B04) / (B08 + B04)."""
    return _safe_div(b08 - b04, b08 + b04)


def calculate_ndmi(b08: float, b11: float) -> Optional[float]:
    """Calculate NDMI = (B08 - B11) / (B08 + B11)."""
    return _safe_div(b08 - b11, b08 + b11)


def calculate_ndre(b08: float, b05: float) -> Optional[float]:
    """Calculate NDRE = (B08 - B05) / (B08 + B05)."""
    return _safe_div(b08 - b05, b08 + b05)


def calculate_evi(b08: float, b04: float, b02: float) -> Optional[float]:
    """Calculate EVI = 2.5 * (B08 - B04) / (B08 + 6.0*B04 - 7.5*B02 + 1.0)."""
    denom = b08 + 6.0 * b04 - 7.5 * b02 + 1.0
    return _safe_div(2.5 * (b08 - b04), denom)


def calculate_savi(b08: float, b04: float, L: float = 0.5) -> Optional[float]:
    """Calculate SAVI = ((B08 - B04) / (B08 + B04 + L)) * (1.0 + L)."""
    val = _safe_div(b08 - b04, b08 + b04 + L)
    return val * (1.0 + L) if val is not None else None


def calculate_gci(b08: float, b03: float) -> Optional[float]:
    """Calculate GCI = (B08 / B03) - 1.0."""
    val = _safe_div(b08, b03)
    return val - 1.0 if val is not None else None


def calculate_vpd(t2m_c: float, rh2m_pct: float) -> Optional[float]:
    """Calculate Vapor Pressure Deficit in kPa using FAO-56 formulation.
    Allen, R. G., Pereira, L. S., Raes, D., & Smith, M. (1998).
    Crop evapotranspiration-Guidelines for computing crop water requirements-FAO Irrigation and drainage paper 56.
    """
    if t2m_c is None or rh2m_pct is None:
        return None
    if not math.isfinite(t2m_c) or not math.isfinite(rh2m_pct):
        return None
    if rh2m_pct < 0 or rh2m_pct > 100:
        return None
    sat_vp = 0.61078 * math.exp((17.27 * t2m_c) / (t2m_c + 237.3))
    act_vp = sat_vp * (rh2m_pct / 100.0)
    return max(sat_vp - act_vp, 0.0)


def classify_health(index_key: str, value: Optional[float]) -> dict[str, Any]:
    """Classify an index value into health category, label, and colors."""
    if value is None or not math.isfinite(value):
        return {
            "category": "no_data",
            "label": "No Data / Masked",
            "color": "#94a3b8",
            "bg": "#f8fafc",
        }

    defn = INDEX_REGISTRY.get(index_key)
    if not defn:
        return {"category": "unknown", "label": "Calculated", "color": "#64748b", "bg": "#f1f5f9"}

    if index_key == "ndvi":
        if value < 0.20:
            return defn.thresholds[0]
        if value < 0.40:
            return defn.thresholds[1]
        if value < 0.60:
            return defn.thresholds[2]
        if value < 0.75:
            return defn.thresholds[3]
        return defn.thresholds[4]

    elif index_key == "ndmi":
        if value < -0.15:
            return defn.thresholds[0]
        if value < 0.00:
            return defn.thresholds[1]
        if value < 0.20:
            return defn.thresholds[2]
        if value < 0.40:
            return defn.thresholds[3]
        return defn.thresholds[4]

    elif index_key == "ndre":
        if value < 0.15:
            return defn.thresholds[0]
        if value < 0.28:
            return defn.thresholds[1]
        if value < 0.42:
            return defn.thresholds[2]
        if value < 0.55:
            return defn.thresholds[3]
        return defn.thresholds[4]

    elif index_key == "evi":
        if value < 0.15:
            return defn.thresholds[0]
        if value < 0.30:
            return defn.thresholds[1]
        if value < 0.45:
            return defn.thresholds[2]
        if value < 0.65:
            return defn.thresholds[3]
        return defn.thresholds[4]

    elif index_key == "savi":
        if value < 0.15:
            return defn.thresholds[0]
        if value < 0.25:
            return defn.thresholds[1]
        if value < 0.40:
            return defn.thresholds[2]
        if value < 0.55:
            return defn.thresholds[3]
        return defn.thresholds[4]

    elif index_key == "gci":
        if value < 1.0:
            return defn.thresholds[0]
        if value < 2.5:
            return defn.thresholds[1]
        if value < 4.5:
            return defn.thresholds[2]
        if value < 6.5:
            return defn.thresholds[3]
        return defn.thresholds[4]

    return {"category": "moderate", "label": "Moderate", "color": "#eab308", "bg": "#fefce8"}
