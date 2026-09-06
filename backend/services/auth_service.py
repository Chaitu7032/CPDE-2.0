"""
CPDE v2 Authentication Service
Manages farmer and researcher registration, JWT lifecycle, and RBAC.
"""

from __future__ import annotations
from typing import Any, Optional
from sqlalchemy import select
from backend.db.connection import async_session
from backend.db.models import User
from backend.core.security import hash_password, verify_password, create_access_token, create_refresh_token, decode_token


async def register_user(
    email: str,
    password: str,
    name: str,
    phone: Optional[str] = None,
    role: str = "farmer",
    state: Optional[str] = None,
    district: Optional[str] = None,
    mandal: Optional[str] = None,
    village: Optional[str] = None,
) -> dict[str, Any]:
    """Register a new farmer or agricultural scientist."""
    email_clean = email.strip().lower()
    async with async_session() as session:
        # Check existing
        res = await session.execute(select(User).where(User.email == email_clean))
        if res.scalar_one_or_none():
            raise ValueError("An account with this email already exists.")

        hashed = hash_password(password)
        new_user = User(
            email=email_clean,
            hashed_password=hashed,
            name=name.strip(),
            phone=phone.strip() if phone else None,
            role=role if role in ("farmer", "researcher", "admin") else "farmer",
            state=state.strip() if state else None,
            district=district.strip() if district else None,
            mandal=mandal.strip() if mandal else None,
            village=village.strip() if village else None,
        )
        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)

        user_data = {
            "id": new_user.id,
            "email": new_user.email,
            "name": new_user.name,
            "role": new_user.role,
        }
        access_token = create_access_token(user_data)
        refresh_token = create_refresh_token(user_data)

        return {
            "user": {
                "id": new_user.id,
                "email": new_user.email,
                "name": new_user.name,
                "phone": new_user.phone,
                "role": new_user.role,
                "state": new_user.state,
                "district": new_user.district,
                "mandal": new_user.mandal,
                "village": new_user.village,
            },
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }


async def authenticate_user(email: str, password: str) -> dict[str, Any]:
    """Authenticate a user and return fresh JWT tokens."""
    email_clean = email.strip().lower()
    async with async_session() as session:
        res = await session.execute(select(User).where(User.email == email_clean))
        user = res.scalar_one_or_none()
        if not user or not verify_password(password, user.hashed_password):
            raise ValueError("Invalid email or password.")

        user_data = {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.role,
        }
        access_token = create_access_token(user_data)
        refresh_token = create_refresh_token(user_data)

        return {
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "phone": user.phone,
                "role": user.role,
                "state": user.state,
                "district": user.district,
                "mandal": user.mandal,
                "village": user.village,
            },
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }


async def refresh_user_token(refresh_token: str) -> dict[str, Any]:
    """Validate refresh token and issue a new access token."""
    payload = decode_token(refresh_token)
    if not payload or payload.get("type") != "refresh":
        raise ValueError("Invalid or expired refresh token.")

    user_id = payload.get("id")
    async with async_session() as session:
        res = await session.execute(select(User).where(User.id == user_id))
        user = res.scalar_one_or_none()
        if not user:
            raise ValueError("User not found.")

        user_data = {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.role,
        }
        access_token = create_access_token(user_data)
        return {
            "access_token": access_token,
            "token_type": "bearer",
        }
