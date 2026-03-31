from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from typing import Optional

from backend.pipelines.sentinel2 import process_sentinel2_for_land_day

router = APIRouter(prefix="/sentinel2", tags=["sentinel2"])


class ProcessRequest(BaseModel):
    land_id: str
    date: str  # YYYY-MM-DD


class ProcessResponse(BaseModel):
    processed: int
    stac_item_id: Optional[str] = None
    datetime: Optional[str] = None
    cloud_cover: Optional[float] = None


@router.post("/process")
async def process(req: ProcessRequest) -> ProcessResponse:
    try:
        result = await process_sentinel2_for_land_day(req.land_id, req.date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("processed", 0) == 0:
        raise HTTPException(status_code=404, detail=result.get("reason", "no data processed"))
    return ProcessResponse(**result)
