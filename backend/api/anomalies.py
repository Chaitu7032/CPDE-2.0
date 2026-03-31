from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

from backend.pipelines.anomaly import build_climatology_for_variable, compute_anomalies_for_date

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


class BuildClimRequest(BaseModel):
    land_id: str
    variable: str


class ComputeAnomRequest(BaseModel):
    land_id: str
    date: str  # YYYY-MM-DD
    variables: List[str] | None = None


@router.post("/build_climatology")
async def build_climatology(req: BuildClimRequest):
    try:
        res = await build_climatology_for_variable(req.land_id, req.variable)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return res


@router.post("/compute")
async def compute_anomalies(req: ComputeAnomRequest):
    try:
        res = await compute_anomalies_for_date(req.land_id, req.date, variables=req.variables)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return res
