"""
CPDE v2 Fields, Analytics, Temporal Comparison, Ground Truth & Export Router
"""

from fastapi import APIRouter, HTTPException, Response, Query
from pydantic import BaseModel
from typing import Any, Optional, Literal
from datetime import date

from backend.services.observation_service import (
    get_field_available_dates,
    get_field_state_at_date,
    compare_temporal_dates,
)
from backend.services.validation_service import add_ground_truth_sample, generate_validation_report
from backend.services.export_service import export_field_dataset

router = APIRouter(prefix="/fields", tags=["v2-fields"])


class GroundTruthRequest(BaseModel):
    date: date
    observed_condition: str
    grid_id: Optional[str] = None
    crop_stage: Optional[str] = None
    photo_url: Optional[str] = None
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@router.get("/{field_id}")
async def get_field_dashboard(field_id: int, date: Optional[str] = None):
    """Retrieve complete field state: 10m grid cells, 6 indices, weather, explainable alerts, and traceability."""
    try:
        data = await get_field_state_at_date(field_id, target_date=date)
        return data
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch field state: {e}")


@router.get("/{field_id}/available-dates")
async def get_available_dates(field_id: int):
    """List all available observation dates with cloud cover and quality score."""
    try:
        dates = await get_field_available_dates(field_id)
        return {"field_id": field_id, "observation_dates": dates}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{field_id}/temporal")
async def get_temporal_comparison(
    field_id: int,
    date_a: str = Query(..., description="Reference Date A (YYYY-MM-DD)"),
    date_b: str = Query(..., description="Comparison Date B (YYYY-MM-DD)"),
):
    """Compare biophysical conditions between Date A and Date B."""
    try:
        res = await compare_temporal_dates(field_id, date_a, date_b)
        return res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Temporal comparison failed: {e}")


@router.post("/{field_id}/ground-truth", status_code=201)
async def submit_ground_truth(field_id: int, req: GroundTruthRequest):
    """Record farmer or researcher ground-truth field condition."""
    try:
        res = await add_ground_truth_sample(
            field_id=field_id,
            sample_date=req.date,
            observed_condition=req.observed_condition,
            grid_id=req.grid_id,
            crop_stage=req.crop_stage,
            photo_url=req.photo_url,
            notes=req.notes,
            latitude=req.latitude,
            longitude=req.longitude,
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{field_id}/validation-report")
async def get_validation_report_endpoint(field_id: int):
    """Generate statistical ground-truth validation report with Confusion Matrix, Precision, Recall, F1."""
    try:
        report = await generate_validation_report(field_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{field_id}/export")
async def export_field_endpoint(
    field_id: int,
    format: Literal["csv", "geojson", "parquet"] = Query("csv", description="Export format"),
):
    """Download research-grade dataset in CSV, GeoJSON, or Apache Parquet."""
    try:
        content, media_type, filename = await export_field_dataset(field_id, format_type=format)
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")
