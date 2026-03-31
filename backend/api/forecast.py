from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.pipelines.forecasting import train_logistic_model, predict_risk

router = APIRouter(prefix="/forecast", tags=["forecast"])


class TrainRequest(BaseModel):
    land_id: str
    grid_id: str
    lookback_days: int = 14
    predict_horizon: int = 14


class PredictRequest(BaseModel):
    land_id: str
    grid_id: str
    date: str  # YYYY-MM-DD
    model_path: str | None = None


@router.post("/train")
async def train(req: TrainRequest):
    try:
        model_path = await train_logistic_model(req.land_id, req.grid_id, lookback_days=req.lookback_days, predict_horizon=req.predict_horizon)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"model_path": model_path}


@router.post("/predict")
async def predict(req: PredictRequest):
    try:
        res = await predict_risk(req.land_id, req.grid_id, req.date, model_path=req.model_path)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return res
