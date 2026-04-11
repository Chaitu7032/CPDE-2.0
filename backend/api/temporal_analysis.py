from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.services.temporal_comparison import get_temporal_data


router = APIRouter(prefix="/temporal-analysis", tags=["temporal-analysis"])


@router.get("/{land_id}")
async def get_temporal_analysis(
    land_id: int,
    active_date: date = Query(..., description="Reference date resolved by the dashboard"),
    comparison_date: date | None = Query(None, description="Optional comparison date"),
    mode: str = Query("strict", description="Temporal analysis mode: strict or smart"),
    window_days: int = Query(30, ge=1, le=90),
    tolerance_days: int = Query(5, ge=1, le=30),
):
    try:
        return await get_temporal_data(
            land_id,
            active_date,
            comparison_date,
            history_window_days=window_days,
            comparison_tolerance_days=tolerance_days,
            mode=mode,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))