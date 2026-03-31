from datetime import datetime
from typing import Dict, List

from sqlalchemy import text

from backend.db.connection import async_session


VARIABLE_SOURCES: dict[str, dict] = {
    # Grid-level (per grid_id)
    "ndvi": {"table": "land_daily_indices", "value_col": "ndvi", "pixel_count_col": "pixel_count", "level": "grid"},
    "ndmi": {"table": "land_daily_indices", "value_col": "ndmi", "pixel_count_col": "pixel_count", "level": "grid"},
    "lst": {"table": "land_daily_lst", "value_col": "lst_c", "pixel_count_col": None, "level": "grid"},
    # Land-level (one value per land_id/day). Stored in climatology/anomalies under grid_id='__land__'
    "t2m": {"table": "land_daily_weather", "value_col": "t2m", "pixel_count_col": None, "level": "land"},
    "rh2m": {"table": "land_daily_weather", "value_col": "rh2m", "pixel_count_col": None, "level": "land"},
    "prectotcorr": {"table": "land_daily_weather", "value_col": "prectotcorr", "pixel_count_col": None, "level": "land"},
}

LAND_LEVEL_GRID_ID = "__land__"


async def build_climatology_for_variable(land_id, variable: str) -> Dict[str, int]:
    """Compute day-of-year climatology (mean/std/count) per `grid_id` for given `variable` and store in `land_climatology`.

    Returns summary {"grids_processed": N}.
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    if variable not in VARIABLE_SOURCES:
        raise ValueError("unsupported variable")

    src = VARIABLE_SOURCES[variable]
    table = src["table"]
    col = src["value_col"]
    level = src.get("level", "grid")

    if level == "grid":
        sql = text(
            f"""
            INSERT INTO land_climatology (land_id, grid_id, variable, day_of_year, mean, std, count)
                        SELECT CAST(:land_id AS INTEGER) as land_id, grid_id, CAST(:variable AS VARCHAR(32)) as variable, EXTRACT(DOY FROM date)::int as day_of_year,
                   AVG({col}) as mean, STDDEV_POP({col}) as std, COUNT({col}) as count
            FROM {table}
                        WHERE land_id = CAST(:land_id AS INTEGER) AND {col} IS NOT NULL
            GROUP BY grid_id, EXTRACT(DOY FROM date)::int
            ON CONFLICT (land_id, grid_id, variable, day_of_year) DO UPDATE
              SET mean = EXCLUDED.mean, std = EXCLUDED.std, count = EXCLUDED.count;
            """
        )
    else:
        # land-level variable stored under a sentinel grid_id
        sql = text(
            f"""
            INSERT INTO land_climatology (land_id, grid_id, variable, day_of_year, mean, std, count)
                        SELECT CAST(:land_id AS INTEGER) as land_id, CAST(:grid_id AS VARCHAR(128)) as grid_id, CAST(:variable AS VARCHAR(32)) as variable, EXTRACT(DOY FROM date)::int as day_of_year,
                   AVG({col}) as mean, STDDEV_POP({col}) as std, COUNT({col}) as count
            FROM {table}
                        WHERE land_id = CAST(:land_id AS INTEGER) AND {col} IS NOT NULL
            GROUP BY EXTRACT(DOY FROM date)::int
            ON CONFLICT (land_id, grid_id, variable, day_of_year) DO UPDATE
              SET mean = EXCLUDED.mean, std = EXCLUDED.std, count = EXCLUDED.count;
            """
        )

    async with async_session() as session:
        await session.execute(sql, {"land_id": land_id, "variable": variable, "grid_id": LAND_LEVEL_GRID_ID})
        await session.commit()

    return {"climatology_built": 1}


async def compute_anomalies_for_date(land_id, date_str: str, variables: List[str] | None = None) -> Dict[str, int]:
    """For a given date, compute z-score anomalies for requested variables across all grids in the land.

    If `variables` is None, compute for all supported variables.
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    if variables is None:
        variables = list(VARIABLE_SOURCES.keys())

    date_obj = datetime.fromisoformat(date_str).date()
    doy = int(date_obj.timetuple().tm_yday)

    processed = 0
    async with async_session() as session:
        for var in variables:
            if var not in VARIABLE_SOURCES:
                continue

            src = VARIABLE_SOURCES[var]
            table = src["table"]
            col = src["value_col"]
            px_col = src.get("pixel_count_col")
            level = src.get("level", "grid")

            if level == "grid":
                px_select = f"COALESCE({px_col}, 0)" if px_col else "NULL::int"
                sql = text(
                    f"""
                    INSERT INTO land_anomalies (land_id, grid_id, date, variable, value, mean, std, zscore, pixel_count)
                    SELECT
                        obs.land_id,
                        obs.grid_id,
                        obs.date,
                        :variable as variable,
                        obs.value,
                        clim.mean,
                        clim.std,
                        CASE
                            WHEN clim.std IS NULL OR clim.std = 0 THEN NULL
                            ELSE (obs.value - clim.mean) / clim.std
                        END as zscore,
                        obs.pixel_count
                    FROM (
                        SELECT land_id, grid_id, date, {col} as value, {px_select} as pixel_count
                        FROM {table}
                        WHERE land_id = :land_id AND date = :date AND {col} IS NOT NULL
                    ) obs
                    LEFT JOIN land_climatology clim
                        ON clim.land_id = obs.land_id
                        AND clim.grid_id = obs.grid_id
                        AND clim.variable = :variable
                        AND clim.day_of_year = :doy
                    ON CONFLICT (land_id, grid_id, date, variable) DO UPDATE
                      SET value=EXCLUDED.value, mean=EXCLUDED.mean, std=EXCLUDED.std, zscore=EXCLUDED.zscore, pixel_count=EXCLUDED.pixel_count;
                    """
                )

                res = await session.execute(sql, {"land_id": land_id, "date": date_obj, "variable": var, "doy": doy})
                processed += int(getattr(res, "rowcount", 0) or 0)

            else:
                # land-level variable stored as a single anomaly row
                sql = text(
                    f"""
                    INSERT INTO land_anomalies (land_id, grid_id, date, variable, value, mean, std, zscore, pixel_count)
                    SELECT
                        :land_id as land_id,
                        :grid_id as grid_id,
                        :date as date,
                        :variable as variable,
                        obs.value,
                        clim.mean,
                        clim.std,
                        CASE
                            WHEN clim.std IS NULL OR clim.std = 0 THEN NULL
                            ELSE (obs.value - clim.mean) / clim.std
                        END as zscore,
                        NULL::int as pixel_count
                    FROM (
                        SELECT {col} as value
                        FROM {table}
                        WHERE land_id = :land_id AND date = :date
                        LIMIT 1
                    ) obs
                    LEFT JOIN land_climatology clim
                        ON clim.land_id = :land_id
                        AND clim.grid_id = :grid_id
                        AND clim.variable = :variable
                        AND clim.day_of_year = :doy
                    ON CONFLICT (land_id, grid_id, date, variable) DO UPDATE
                      SET value=EXCLUDED.value, mean=EXCLUDED.mean, std=EXCLUDED.std, zscore=EXCLUDED.zscore, pixel_count=EXCLUDED.pixel_count;
                    """
                )
                res = await session.execute(
                    sql,
                    {"land_id": land_id, "grid_id": LAND_LEVEL_GRID_ID, "date": date_obj, "variable": var, "doy": doy},
                )
                processed += int(getattr(res, "rowcount", 0) or 0)

        await session.commit()

    return {"anomalies_upserted": processed}
