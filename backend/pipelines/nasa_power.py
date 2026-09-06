import asyncio
from datetime import datetime
import logging
import math
from typing import Dict, Optional
import requests

from sqlalchemy import text

from backend.db.connection import async_session

logger = logging.getLogger(__name__)

POWER_API = "https://power.larc.nasa.gov/api/temporal/daily/point"
POWER_REQUEST_TIMEOUT_S = 45  # NASA POWER can be slow; 45s is generous but bounded


def _normalize_power_date(date_str: str) -> str:
    """Ensure date is YYYYMMDD (compact) regardless of input format."""
    if "-" in date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
    return date_str


def fetch_power_point(
    lat: float,
    lon: float,
    start: str,
    end: str,
    parameters: str = "T2M,RH2M,PRECTOTCORR",
) -> Dict[str, Dict]:
    """Fetch NASA POWER daily data for a point between start/end (YYYYMMDD or YYYY-MM-DD).

    Returns dict mapping date (YYYY-MM-DD) -> {param: value}.
    Returns empty dict (never raises) on network/parse errors.

    Scientific note
    ---------------
    NASA POWER uses -999 (and variants) as fill/missing values for days with
    insufficient observation coverage. These are masked out and returned as None.
    Data latency is typically 1-3 days behind real time.
    """
    params = {
        "start": _normalize_power_date(start),
        "end": _normalize_power_date(end),
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
            # Always emit ISO date key (YYYY-MM-DD) regardless of API response format
            try:
                iso = datetime.strptime(date, "%Y%m%d").date().isoformat()
            except ValueError:
                # Defensive: pass through if already ISO
                iso = date[:10] if len(date) >= 10 else date
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

    When start_date == end_date (single-day request), automatically widens the
    fetch window by ±3 days so that NASA POWER fill-value gaps on individual
    dates don't block the pipeline.  The extra rows are stored in the DB and
    the caller's target date will always be found in the result.
    """
    from datetime import timedelta as _td

    land_id = int(land_id)

    def norm_iso(d: str) -> str:
        """Return YYYY-MM-DD regardless of input format."""
        if "-" in d:
            return d[:10]
        return datetime.strptime(d, "%Y%m%d").date().isoformat()

    start_iso = norm_iso(start_date)
    end_iso   = norm_iso(end_date)

    # Widen to a 7-day window when fetching a single target date.
    # NASA POWER individual-day requests can return fill values (-999) even
    # when the surrounding days have valid data.  Fetching ±3 days ensures
    # the target date has neighbours that confirm data availability.
    if start_iso == end_iso:
        from datetime import date as _date
        anchor = datetime.fromisoformat(start_iso).date()
        start_iso = (anchor - _td(days=3)).isoformat()
        end_iso   = (anchor + _td(days=3)).isoformat()
        logger.info(
            "NASA POWER widening single-day request to 7-day window: %s to %s for land=%s",
            start_iso, end_iso, land_id,
        )

    start_compact = _normalize_power_date(start_iso)
    end_compact   = _normalize_power_date(end_iso)

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
                loop.run_in_executor(None, fetch_power_point, lat, lon, start_compact, end_compact),
                timeout=POWER_REQUEST_TIMEOUT_S + 10,  # slightly above requests timeout
            )
        except asyncio.TimeoutError:
            logger.error(
                "NASA POWER executor timed out for land=%s start=%s end=%s",
                land_id,
                start_compact,
                end_compact,
            )
            return {
                "processed": 0,
                "reason": f"NASA POWER timed out after {POWER_REQUEST_TIMEOUT_S}s",
            }
        except Exception as exc:
            logger.exception(
                "NASA POWER fetch failed for land=%s start=%s end=%s", land_id, start_compact, end_compact
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

                vpd = None
                if t2m is not None and rh2m is not None:
                    try:
                        t_val = float(t2m)
                        rh_val = float(rh2m)
                        if 0 <= rh_val <= 100:
                            es = 0.6108 * math.exp((17.27 * t_val) / (t_val + 237.3))
                            vpd = max(float(es * (1.0 - (rh_val / 100.0))), 0.0)
                    except Exception:
                        vpd = None

                await session.execute(
                    text(
                        "INSERT INTO land_daily_weather "
                        "(land_id, date, t2m, rh2m, prectotcorr, vpd, source) "
                        "VALUES (:land_id, :date, :t2m, :rh2m, :prectot, :vpd, :source) "
                        "ON CONFLICT (land_id, date) DO UPDATE SET "
                        "  t2m         = EXCLUDED.t2m, "
                        "  rh2m        = EXCLUDED.rh2m, "
                        "  prectotcorr = EXCLUDED.prectotcorr, "
                        "  vpd         = EXCLUDED.vpd, "
                        "  source      = EXCLUDED.source"
                    ),
                    {
                        "land_id": land_id,
                        "date": date_obj,
                        "t2m": t2m,
                        "rh2m": rh2m,
                        "prectot": prectot,
                        "vpd": vpd,
                        "source": "NASA_POWER",
                    },
                )
                processed += 1
            await session.commit()
    except Exception as exc:
        logger.exception(
            "NASA POWER DB write failed for land=%s start=%s end=%s",
            land_id,
            start_iso,
            end_iso,
        )
        return {"processed": 0, "reason": f"NASA POWER DB write error: {exc}"}

    logger.info(
        "NASA POWER processing complete land=%s processed=%d start=%s end=%s",
        land_id,
        processed,
        start_iso,
        end_iso,
    )
    return {"processed": processed}