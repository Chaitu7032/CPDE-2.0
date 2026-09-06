import asyncio
import json
from datetime import datetime, timedelta
from pystac_client import Client
from shapely.geometry import shape
from sqlalchemy import text
from backend.db.connection import async_session

PC_STAC_API = "https://planetarycomputer.microsoft.com/api/stac/v1"


async def main():
    async with async_session() as session:
        res = await session.execute(
            text("SELECT ST_AsGeoJSON(ST_Transform(geom, 4326)) FROM lands WHERE land_id = 106")
        )
        row = res.first()
        geom = shape(json.loads(row[0]))

    client = Client.open(PC_STAC_API)

    # Let's search across 2024, 2025, and 2026 Kharif periods to see STAC coverage
    windows = [
        ("2026-07-01", "2026-08-30", "2026 Kharif"),
        ("2024-07-01", "2024-08-30", "2024 Kharif"),
        ("2024-01-01", "2024-03-30", "2024 Rabi"),
    ]

    for start, end, label in windows:
        dt = f"{start}T00:00:00Z/{end}T23:59:59Z"
        print(f"\n=== Searching STAC for {label} ({dt}) ===")
        
        # Sentinel-2
        s2_items = list(client.search(collections=["sentinel-2-l2a"], intersects=geom.__geo_interface__, datetime=dt).items())
        print(f"  Sentinel-2 scenes: {len(s2_items)}")
        for it in s2_items[:3]:
            print(f"    S2: {it.id}, datetime={it.properties.get('datetime')}, cloud={it.properties.get('eo:cloud_cover')}%")

        # Sentinel-1
        s1_items = list(client.search(collections=["sentinel-1-grd"], intersects=geom.__geo_interface__, datetime=dt).items())
        print(f"  Sentinel-1 scenes: {len(s1_items)}")
        for it in s1_items[:3]:
            print(f"    S1: {it.id}, datetime={it.properties.get('datetime')}, orbit={it.properties.get('sat:orbit_state')}")

        # Landsat
        ls_items = list(client.search(collections=["landsat-c2-l2"], intersects=geom.__geo_interface__, datetime=dt).items())
        print(f"  Landsat scenes: {len(ls_items)}")
        for it in ls_items[:3]:
            print(f"    Landsat: {it.id}, datetime={it.properties.get('datetime')}, cloud={it.properties.get('eo:cloud_cover')}%")


if __name__ == "__main__":
    asyncio.run(main())
