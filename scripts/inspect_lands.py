import asyncio
from backend.db.connection import async_session
from sqlalchemy import text


async def main():
    async with async_session() as session:
        res = await session.execute(
            text(
                "SELECT l.land_id, l.farmer_name, l.crop_type, l.area_sqm, count(g.id) as grid_count "
                "FROM lands l "
                "LEFT JOIN land_grid_cells g ON g.land_id = l.land_id "
                "GROUP BY l.land_id, l.farmer_name, l.crop_type, l.area_sqm "
                "ORDER BY l.land_id"
            )
        )
        rows = res.fetchall()
        print(f"Total lands found: {len(rows)}")
        for r in rows:
            print(f"Land #{r[0]}: Name='{r[1]}', Crop='{r[2]}', Area={r[3]} sqm, Grids={r[4]}")


if __name__ == "__main__":
    asyncio.run(main())
