import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Load .env values (if present)
# Be robust to launching uvicorn from either repo root or subfolders (e.g. ./backend)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

if not os.getenv("DATABASE_URL"):
    raise RuntimeError("DATABASE_URL is not set. Create .env with DATABASE_URL=postgresql+asyncpg://...")

# Re-export the project-wide async engine/sessionmaker (final form in db/database.py)
from backend.db.database import engine, AsyncSessionLocal, get_db  # noqa: E402

async_session = AsyncSessionLocal


async def init_db():
    """Validate DB connectivity. Returns True if a connection can be established."""
    try:
        async with engine.connect() as conn:
            result = await conn.scalar(text("SELECT 1"))
            logger.info("DB connection verified (SELECT 1 = %s)", result)
        return True
    except SQLAlchemyError as e:
        logger.error("DB connection failed: %s", e)
        return False


async def db_health() -> dict:
    """Return DB health details (including error message on failure)."""
    try:
        async with engine.connect() as conn:
            await conn.scalar(text("SELECT 1"))
            postgis_ver = await conn.scalar(text("SELECT PostGIS_Version()"))
        return {"ok": True, "postgis": postgis_ver}
    except Exception as e:
        logger.error("DB health check failed: %s", e)
        return {"ok": False, "error": str(e)}
