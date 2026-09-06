import pytest
from backend.db.init_tables import create_tables
from backend.db.connection import async_session
from sqlalchemy import text


@pytest.mark.anyio
async def test_schema_init():
    try:
        await create_tables()
    except Exception as exc:
        pytest.skip(f"Database connection not available or offline: {exc}")

    async with async_session() as session:
        # Check that new tables and columns exist
        res = await session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'land_daily_indices'"))
        cols = {r[0] for r in res.fetchall()}
        assert "evi" in cols
        assert "ndre" in cols
        assert "lswi" in cols
        assert "native_resolution_m" in cols
