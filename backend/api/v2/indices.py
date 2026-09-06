"""
CPDE v2 Scientific Indices & Traceability Router
"""

from fastapi import APIRouter
from backend.domain.scientific_indices import INDEX_REGISTRY

router = APIRouter(prefix="/indices", tags=["v2-indices"])


@router.get("/registry")
async def get_scientific_indices_registry():
    """Return complete peer-reviewed citations, formulas, bands, and threshold palettes."""
    return {
        "indices": {
            k: {
                "key": v.key,
                "name": v.name,
                "acronym": v.acronym,
                "formula": v.formula,
                "citation": v.citation,
                "bands_used": v.bands_used,
                "description": v.description,
                "min_valid": v.min_valid,
                "max_valid": v.max_valid,
                "thresholds": v.thresholds,
            }
            for k, v in INDEX_REGISTRY.items()
        }
    }
