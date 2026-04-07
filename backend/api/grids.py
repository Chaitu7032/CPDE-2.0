from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import json

from sqlalchemy import text

from backend.pipelines.grid_generation import generate_and_store_grids
from backend.db.connection import async_session
from backend.utils.crs import geometry_geojson_storage_to_api

router = APIRouter(prefix="/grids", tags=["grids"])


class GridRequest(BaseModel):
    land_id: str
    cell_size_m: float = 10.0


class GridGenerateResponse(BaseModel):
    count: int
    grid_ids: list[str]


@router.post("/generate")
async def generate_grids(req: GridRequest) -> GridGenerateResponse:
    if req.cell_size_m != 10.0:
        raise HTTPException(status_code=400, detail="CPDE uses fixed 10m x 10m grid cells (cell_size_m must be 10.0)")
    try:
        grid_ids = await generate_and_store_grids(req.land_id, cell_size_m=req.cell_size_m)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return GridGenerateResponse(count=len(grid_ids), grid_ids=grid_ids)


@router.get("/{land_id}")
async def get_grids(land_id: str):
    lid = int(land_id)
    async with async_session() as session:
        res = await session.execute(
            text(
                "SELECT grid_id, ST_AsGeoJSON(geom) as geojson FROM land_grid_cells WHERE land_id = :lid ORDER BY grid_id"
            ),
            {"lid": lid},
        )
        rows = res.fetchall()

    features = []
    for grid_id, geojson in rows:
        storage_geometry = json.loads(geojson)
        api_geometry = geometry_geojson_storage_to_api(storage_geometry)
        features.append(
            {
                "type": "Feature",
                "properties": {"grid_id": grid_id},
                "geometry": api_geometry,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }
