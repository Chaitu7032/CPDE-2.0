from datetime import datetime
from typing import Dict
import requests
import asyncio

from sqlalchemy import text

from backend.db.connection import async_session


POWER_API = "https://power.larc.nasa.gov/api/temporal/daily/point"


def fetch_power_point(lat: float, lon: float, start: str, end: str, parameters: str = "T2M,RH2M,PRECTOTCORR") -> Dict[str, Dict]:
    """Fetch NASA POWER daily data for a point between start/end (YYYYMMDD).

    Returns dict mapping date (YYYY-MM-DD) -> {param: value}
    """
    params = {
        "start": start,
        "end": end,
        "latitude": lat,
        "longitude": lon,
        "parameters": parameters,
        "community": "ag",
        "format": "JSON",
    }
    r = requests.get(POWER_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = {}
    # daily data at data['properties']['parameter'][PARAM][date]
    props = data.get("properties", {})
    param_block = props.get("parameter", {})
    if not param_block:
        return results

    # get list of dates from one parameter
    sample_param = next(iter(param_block.keys()))
    dates = list(param_block[sample_param].keys())
    for date in dates:
        out = {}
        for p, vals in param_block.items():
            v = vals.get(date)
            # NASA POWER uses -999 (and variants) as fill/missing value
            if v is not None and (v == -999 or v == -999.0 or v <= -998):
                v = None
            out[p.lower()] = v
        # ISO date format (YYYY-MM-DD)
        iso = datetime.strptime(date, "%Y%m%d").date().isoformat()
        results[iso] = out

    return results


async def process_weather_for_land(land_id, start_date: str, end_date: str) -> dict:
    """Fetch NASA POWER for the centroid of the land and store into DB for the date range.

    start_date/end_date format: YYYYMMDD or YYYY-MM-DD (function will normalize).
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    # normalize dates to YYYYMMDD
    def norm(d: str) -> str:
        if "-" in d:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%Y%m%d")
        return d

    start = norm(start_date)
    end = norm(end_date)

    # fetch land centroid
    async with async_session() as session:
        res = await session.execute(
            text(
                "SELECT "
                "ST_X(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) as lon, "
                "ST_Y(ST_Transform(COALESCE(centroid, ST_Centroid(geom)), 4326)) as lat "
                "FROM lands WHERE land_id = :lid"
            ),
            {"lid": land_id},
        )
        row = res.first()
        if not row:
            return {"processed": 0, "reason": "land not found"}
        lon, lat = row[0], row[1]

    # fetch power data (blocking network call) in thread
    loop = asyncio.get_running_loop()
    data = await loop.run_in_executor(None, fetch_power_point, lat, lon, start, end)

    if not data:
        return {"processed": 0, "reason": "no data from NASA POWER"}

    # store into DB (skip rows where ALL values are None / -999)
    async with async_session() as session:
        processed = 0
        for iso_date, vals in data.items():
            date_obj = datetime.fromisoformat(iso_date).date()
            t2m = vals.get("t2m")
            rh2m = vals.get("rh2m")
            prectot = vals.get("prectotcorr")
            # Skip dates where all parameters are missing
            if t2m is None and rh2m is None and prectot is None:
                continue
            await session.execute(
                text(
                    "INSERT INTO land_daily_weather (land_id, date, t2m, rh2m, prectotcorr, source) VALUES (:land_id, :date, :t2m, :rh2m, :prectot, :source) ON CONFLICT (land_id, date) DO UPDATE SET t2m = EXCLUDED.t2m, rh2m = EXCLUDED.rh2m, prectotcorr = EXCLUDED.prectotcorr, source = EXCLUDED.source"
                ),
                {"land_id": land_id, "date": date_obj, "t2m": t2m, "rh2m": rh2m, "prectot": prectot, "source": "NASA_POWER"},
            )
            processed += 1
        await session.commit()

    return {"processed": processed}
