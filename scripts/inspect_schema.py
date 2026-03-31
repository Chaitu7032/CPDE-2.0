import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from backend.db.connection import engine


async def main() -> None:
    async with engine.connect() as conn:
        tables = [
            "lands",
            "land_grid_cells",
            "land_daily_indices",
            "land_daily_lst",
            "land_daily_weather",
            "land_climatology",
            "land_anomalies",
        ]

        for t in tables:
            r = await conn.execute(
                text(
                    "SELECT data_type, udt_name FROM information_schema.columns WHERE table_name=:t AND column_name='land_id'"
                ),
                {"t": t},
            )
            print(f"{t}.land_id:", r.first())

        r2 = await conn.execute(text("SELECT land_id, pg_typeof(land_id) FROM lands ORDER BY land_id LIMIT 5"))
        print("lands sample:", r2.fetchall())

        cols = await conn.execute(
            text(
                "SELECT column_name, data_type, udt_name, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_name='lands' ORDER BY ordinal_position"
            )
        )
        print("lands columns:")
        for row in cols.fetchall():
            print("  ", row)


if __name__ == "__main__":
    asyncio.run(main())
