"""
CPDE v2 Farm & Land Registration Router
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
from datetime import date
from sqlalchemy import select

from backend.db.connection import async_session
from backend.db.models import Farm, Field
from backend.services.farm_service import validate_field_polygon, register_field

router = APIRouter(prefix="/farms", tags=["v2-farms"])


class CreateFarmRequest(BaseModel):
    user_id: int = 1  # Default demo / single-user or extracted from JWT
    name: str
    description: Optional[str] = None
    state: Optional[str] = None
    district: Optional[str] = None
    mandal: Optional[str] = None
    village: Optional[str] = None


class ValidatePolygonRequest(BaseModel):
    coordinates: list[list[float]]


class RegisterFieldRequest(BaseModel):
    name: str
    coordinates: list[list[float]]
    crop_type: Optional[str] = None
    sowing_date: Optional[date] = None


@router.post("", status_code=201)
async def create_farm(req: CreateFarmRequest):
    async with async_session() as session:
        farm = Farm(
            user_id=req.user_id,
            name=req.name.strip(),
            description=req.description,
            state=req.state,
            district=req.district,
            mandal=req.mandal,
            village=req.village,
        )
        session.add(farm)
        await session.commit()
        await session.refresh(farm)
        return {
            "id": farm.id,
            "name": farm.name,
            "user_id": farm.user_id,
            "village": farm.village,
            "district": farm.district,
            "state": farm.state,
        }


@router.get("")
async def list_farms(user_id: Optional[int] = None):
    async with async_session() as session:
        stmt = select(Farm)
        if user_id:
            stmt = stmt.where(Farm.user_id == user_id)
        res = await session.execute(stmt)
        farms = res.scalars().all()
        return [
            {
                "id": f.id,
                "name": f.name,
                "description": f.description,
                "village": f.village,
                "district": f.district,
                "state": f.state,
            }
            for f in farms
        ]


@router.post("/validate-geometry")
async def validate_geometry_endpoint(req: ValidatePolygonRequest):
    """Instant topological and geometric validation of user-drawn polygon."""
    val = validate_field_polygon(req.coordinates)
    return {
        "valid": val["valid"],
        "errors": val["errors"],
        "warnings": val["warnings"],
        "area_sqm": val.get("area_sqm"),
        "area_ha": val.get("area_ha"),
        "perimeter_m": val.get("perimeter_m"),
        "centroid_wgs": val.get("centroid_wgs"),
        "bbox_wgs": val.get("bbox_wgs"),
        "quality_score": val.get("quality_score"),
        "registration_confidence": val.get("registration_confidence"),
        "processing_crs": "EPSG:32644 (UTM Zone 44N)",
    }


@router.post("/{farm_id}/fields", status_code=201)
async def register_field_endpoint(farm_id: int, req: RegisterFieldRequest):
    """Register agricultural field with 10m adaptive grid generation."""
    try:
        res = await register_field(
            farm_id=farm_id,
            name=req.name,
            coordinates=req.coordinates,
            crop_type=req.crop_type,
            sowing_date=req.sowing_date,
        )
        return res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {e}")
