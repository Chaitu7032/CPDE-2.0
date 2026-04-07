from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from backend.services.field_technical_details import compute_field_technical_details

router = APIRouter(prefix="/field", tags=["field-technical-details"])


class FieldTechnicalDetailsRequest(BaseModel):
    geometry: dict[str, Any]


@router.post("/technical-details")
async def field_technical_details(payload: FieldTechnicalDetailsRequest):
    # Defensive behavior: this endpoint always returns a structured object with warnings.
    return compute_field_technical_details(payload.geometry)
