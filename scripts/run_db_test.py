import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure project root is on sys.path so `backend` package is importable when running the script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text

load_dotenv()

from backend.db.init_tables import create_tables
from backend.db.connection import async_session
from shapely.geometry import Polygon
from urllib.parse import urlparse
import psycopg2
import psycopg2.errors


async def main():
    print("Using DATABASE_URL from .env")
    # Ensure target database exists; if not, attempt to create it using postgres DB
    db_url = os.getenv("DATABASE_URL")
    parsed = urlparse(db_url)
    target_db = parsed.path.lstrip("/")

    try:
        print(f"Ensuring database '{target_db}' exists...")
        conn = psycopg2.connect(dbname="postgres", user=parsed.username, password=parsed.password, host=parsed.hostname, port=parsed.port)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE \"{target_db}\"")
            print(f"Created database '{target_db}'")
        else:
            print(f"Database '{target_db}' already exists")
        cur.close()
        conn.close()
    except Exception as e:
        print("Warning: could not create database automatically:", e)

    # Create tables
    print("Creating tables...")
    await create_tables()
    print("Tables created (if not existing).")

    # Insert test polygon
    geom = Polygon([(30.0, 10.0), (40.0, 40.0), (20.0, 40.0), (10.0, 20.0), (30.0, 10.0)])
    wkt = geom.wkt
    land_id = "test-land-1"

    async with async_session() as session:
        print("Inserting test land...")
        await session.execute(
            text("INSERT INTO lands (land_id, owner, geom) VALUES (:land_id, :owner, ST_SetSRID(ST_GeomFromText(:wkt),4326)) ON CONFLICT (land_id) DO NOTHING"),
            {"land_id": land_id, "owner": "tester", "wkt": wkt},
        )
        await session.commit()

        # Read back
        res = await session.execute(text("SELECT land_id, ST_AsGeoJSON(geom) as geojson FROM lands WHERE land_id = :land_id"), {"land_id": land_id})
        row = res.first()
        if row:
            print("Read back:", row[0])
            print("GeoJSON:", row[1])
        else:
            print("No row returned for test-land-1")


if __name__ == "__main__":
    asyncio.run(main())
