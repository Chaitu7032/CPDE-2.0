import asyncio
import json
from datetime import datetime, timedelta

from shapely.geometry import shape
from pystac_client import Client
from sqlalchemy import text

from backend.db.connection import async_session
from backend.pipelines.modis import (
    DEFAULT_MODIS_STAC_COLLECTION,
    PC_STAC_API,
    _lonlat_to_utm_epsg,
    _sample_modis_lst_from_cogs,
)
import planetary_computer


async def main() -> None:
    land_id = 19
    async with async_session() as session:
        land_res = await session.execute(
            text(
                "SELECT ST_AsGeoJSON(geom), ST_X(COALESCE(centroid, ST_Centroid(geom))), ST_Y(COALESCE(centroid, ST_Centroid(geom))), utm_epsg "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        geom_json, lon, lat, utm_epsg = land_res.first()
        geom = shape(json.loads(geom_json))
        if not utm_epsg:
            utm_epsg = _lonlat_to_utm_epsg(float(lon), float(lat))
        grid_res = await session.execute(
            text(
                "SELECT ST_X(COALESCE(centroid, ST_Centroid(geom))) AS lon, ST_Y(COALESCE(centroid, ST_Centroid(geom))) AS lat "
                "FROM land_grid_cells WHERE land_id = :lid AND COALESCE(is_water, FALSE) = FALSE ORDER BY grid_id"
            ),
            {"lid": land_id},
        )
        points = [(float(row[0]), float(row[1])) for row in grid_res.fetchall()]

    target = datetime(2026, 3, 30).date()
    client = Client.open(PC_STAC_API)
    rows = []
    for day_offset in range(0, 15):
        day = target - timedelta(days=day_offset)
        dt = f"{day:%Y-%m-%d}T00:00:00Z/{day:%Y-%m-%d}T23:59:59Z"
        items = list(
            client.search(
                collections=[DEFAULT_MODIS_STAC_COLLECTION],
                intersects=geom.__geo_interface__,
                datetime=dt,
                max_items=200,
            ).items()
        )
        items = [it for it in items if hasattr(it, "assets") and "LST_Day_1km" in it.assets]
        if not items:
            continue
        item = planetary_computer.sign(items[0])
        samples = await asyncio.to_thread(
            _sample_modis_lst_from_cogs,
            lst_href=item.assets["LST_Day_1km"].href,
            qc_href=item.assets["QC_Day"].href if "QC_Day" in item.assets else None,
            utm_epsg=int(utm_epsg),
            points_lonlat=points,
        )
        valid = sum(1 for sample in samples if sample["lst_c"] is not None)
        rows.append(
            {
                "day": day.isoformat(),
                "valid": valid,
                "item": item.id,
                "qc": samples[0]["qc"] if samples else None,
            }
        )

    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
