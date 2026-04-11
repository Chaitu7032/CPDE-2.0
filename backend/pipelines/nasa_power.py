from datetime import datetime
from typing import Dict, Optional
import requests
import asyncio
import logging

from sqlalchemy import text

from backend.db.connection import async_session

logger = logging.getLogger(__name__)

POWER_API = "https://power.larc.nasa.gov/api/temporal/daily/point"
POWER_REQUEST_TIMEOUT_S = 45  # NASA POWER can be slow; 45s is generous but bounded


def fetch_power_point(
    lat: float,
    lon: float,
    start: str,
    end: str,
    parameters: str = "T2M,RH2M,PRECTOTCORR",
) -> Dict[str, Dict]:
    """Fetch NASA POWER daily data for a point between start/end (YYYYMMDD).

    Returns dict mapping date (YYYY-MM-DD) -> {param: value}.
    Returns empty dict (never raises) on network/parse errors.
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
    try:
        r = requests.get(POWER_API, params=params, timeout=POWER_REQUEST_TIMEOUT_S)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.Timeout:
        logger.error(
            "NASA POWER request timed out after %ss for lat=%s lon=%s",
            POWER_REQUEST_TIMEOUT_S,
            lat,
            lon,
        )
        return {}
    except requests.exceptions.RequestException as exc:
        logger.error("NASA POWER request failed for lat=%s lon=%s: %s", lat, lon, exc)
        return {}
    except Exception as exc:
        logger.exception(
            "NASA POWER unexpected error for lat=%s lon=%s: %s", lat, lon, exc
        )
        return {}

    results = {}
    try:
        props = data.get("properties", {})
        param_block = props.get("parameter", {})
        if not param_block:
            logger.warning(
                "NASA POWER returned empty parameter block for lat=%s lon=%s", lat, lon
            )
            return results

        sample_param = next(iter(param_block.keys()))
        dates = list(param_block[sample_param].keys())

        for date in dates:
            out = {}
            for p, vals in param_block.items():
                v = vals.get(date)
                # NASA POWER uses -999 (and variants) as fill/missing value
                if v is not None and v <= -998:
                    v = None
                out[p.lower()] = v
            iso = datetime.strptime(date, "%Y%m%d").date().isoformat()
            results[iso] = out
    except Exception as exc:
        logger.exception(
            "NASA POWER response parse error for lat=%s lon=%s: %s", lat, lon, exc
        )
        return {}

    logger.info(
        "NASA POWER fetched %d days for lat=%s lon=%s start=%s end=%s",
        len(results),
        lat,
        lon,
        start,
        end,
    )
    return results


async def process_weather_for_land(
    land_id,
    start_date: str,
    end_date: str,
    preloaded_data: Optional[Dict[str, Dict]] = None,
) -> dict:
    """Fetch NASA POWER for the centroid of the land and store into DB for the date range.

    start_date/end_date format: YYYYMMDD or YYYY-MM-DD (function will normalize).
    Never raises — returns {"processed": 0, "reason": ...} on any failure.
    """
    land_id = int(land_id)

    def norm(d: str) -> str:
        if "-" in d:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%Y%m%d")
        return d

    start = norm(start_date)
    end = norm(end_date)

    if preloaded_data is None:
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
            lon, lat = float(row[0]), float(row[1])

        loop = asyncio.get_running_loop()
        try:
            data = await asyncio.wait_for(
                loop.run_in_executor(None, fetch_power_point, lat, lon, start, end),
                timeout=POWER_REQUEST_TIMEOUT_S + 10,  # slightly above requests timeout
            )
        except asyncio.TimeoutError:
            logger.error(
                "NASA POWER executor timed out for land=%s start=%s end=%s",
                land_id,
                start,
                end,
            )
            return {
                "processed": 0,
                "reason": f"NASA POWER timed out after {POWER_REQUEST_TIMEOUT_S}s",
            }
        except Exception as exc:
            logger.exception(
                "NASA POWER fetch failed for land=%s start=%s end=%s", land_id, start, end
            )
            return {"processed": 0, "reason": f"NASA POWER fetch error: {exc}"}
    else:
        data = preloaded_data

    if not data:
        return {"processed": 0, "reason": "no data from NASA POWER"}

    processed = 0
    try:
        async with async_session() as session:
            for iso_date, vals in data.items():
                try:
                    date_obj = datetime.fromisoformat(iso_date).date()
                except ValueError:
                    logger.warning(
                        "NASA POWER skipping unparseable date %s for land=%s",
                        iso_date,
                        land_id,
                    )
                    continue

                t2m = vals.get("t2m")
                rh2m = vals.get("rh2m")
                prectot = vals.get("prectotcorr")

                # Skip dates where all parameters are missing
                if t2m is None and rh2m is None and prectot is None:
                    continue

                await session.execute(
                    text(
                        "INSERT INTO land_daily_weather "
                        "(land_id, date, t2m, rh2m, prectotcorr, source) "
                        "VALUES (:land_id, :date, :t2m, :rh2m, :prectot, :source) "
                        "ON CONFLICT (land_id, date) DO UPDATE SET "
                        "  t2m         = EXCLUDED.t2m, "
                        "  rh2m        = EXCLUDED.rh2m, "
                        "  prectotcorr = EXCLUDED.prectotcorr, "
                        "  source      = EXCLUDED.source"
                    ),
                    {
                        "land_id": land_id,
                        "date": date_obj,
                        "t2m": t2m,
                        "rh2m": rh2m,
                        "prectot": prectot,
                        "source": "NASA_POWER",
                    },
                )
                processed += 1
            await session.commit()
    except Exception as exc:
        logger.exception(
            "NASA POWER DB write failed for land=%s start=%s end=%s",
            land_id,
            start,
            end,
        )
        return {"processed": 0, "reason": f"NASA POWER DB write error: {exc}"}

    logger.info(
        "NASA POWER processing complete land=%s processed=%d start=%s end=%s",
        land_id,
        processed,
        start,
        end,
    )
    return {"processed": processed}