"""Inspect actual DB column types."""
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def check():
    engine = create_async_engine(os.getenv("DATABASE_URL"), echo=False)
    async with engine.connect() as conn:
        for tbl in ("lands", "land_grid_cells", "land_daily_indices", "land_daily_lst", "land_daily_weather", "land_climatology", "land_anomalies"):
            r = await conn.execute(text(
                "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = :t ORDER BY ordinal_position"
            ), {"t": tbl})
            rows = r.fetchall()
            if rows:
                print(f"\n=== {tbl} ===")
                for row in rows:
                    print(f"  {row[0]:20s} {row[1]:30s} {row[2]}")
            else:
                print(f"\n=== {tbl} === (table not found)")
    await engine.dispose()

asyncio.run(check())
