"""
CPDE v2 Auth Router
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from typing import Optional
from backend.services.auth_service import register_user, authenticate_user, refresh_user_token

router = APIRouter(prefix="/auth", tags=["v2-auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str
    phone: Optional[str] = None
    role: str = "farmer"
    state: Optional[str] = None
    district: Optional[str] = None
    mandal: Optional[str] = None
    village: Optional[str] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


@router.post("/register", status_code=201)
async def register_endpoint(req: RegisterRequest):
    try:
        res = await register_user(
            email=req.email,
            password=req.password,
            name=req.name,
            phone=req.phone,
            role=req.role,
            state=req.state,
            district=req.district,
            mandal=req.mandal,
            village=req.village,
        )
        return res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/login")
async def login_endpoint(req: LoginRequest):
    try:
        res = await authenticate_user(email=req.email, password=req.password)
        return res
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))


@router.post("/refresh")
async def refresh_endpoint(req: RefreshRequest):
    try:
        res = await refresh_user_token(req.refresh_token)
        return res
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
