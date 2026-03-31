from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from typing import Optional

from backend.pipelines.nasa_power import process_weather_for_land

router = APIRouter(prefix="/weather", tags=["weather"])


class WeatherRequest(BaseModel):
    land_id: str
    start_date: str  # YYYY-MM-DD or YYYYMMDD
    end_date: str


class WeatherResponse(BaseModel):
    processed: int
    reason: Optional[str] = None


@router.post("/fetch")
async def fetch_weather(req: WeatherRequest) -> WeatherResponse:
    try:
        result = await process_weather_for_land(req.land_id, req.start_date, req.end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if result.get("processed", 0) == 0:
        raise HTTPException(status_code=404, detail=result.get("reason", "no data processed"))
    return WeatherResponse(**result)
