from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from typing import Optional

from backend.pipelines.modis import process_modis_for_land_day

router = APIRouter(prefix="/modis", tags=["modis"])


class ModisProcessRequest(BaseModel):
    land_id: str
    date: str  # YYYY-MM-DD
    collection: str | None = None


class ModisProcessResponse(BaseModel):
    processed: int
    short_name: Optional[str] = None
    version: Optional[str] = None
    granule_id: Optional[str] = None
    hdf_url: Optional[str] = None
    # Additive fields (do not break existing clients)
    lst_mean: Optional[float] = None
    lst_date: Optional[str] = None
    stac_collection: Optional[str] = None


@router.post("/process")
async def process(req: ModisProcessRequest) -> ModisProcessResponse:
    try:
        result = await process_modis_for_land_day(req.land_id, req.date, collection_concept_id=req.collection)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("processed", 0) == 0:
        raise HTTPException(status_code=404, detail=result.get("reason", "no data processed"))
    return ModisProcessResponse(**result)
