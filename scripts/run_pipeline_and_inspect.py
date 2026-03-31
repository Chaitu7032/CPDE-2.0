import asyncio
import traceback

from datetime import datetime


async def main():
    land_id = 15
    date = '2026-02-24'
    print(f"Starting pipeline for land {land_id} date {date}")

    try:
        from backend.pipelines.sentinel2 import process_sentinel2_for_land_day
        from backend.pipelines.modis import process_modis_for_land_day
        from backend.pipelines.nasa_power import process_weather_for_land
        from backend.pipelines.anomaly import compute_anomalies_for_date
        from backend.pipelines.risk import compute_risk_for_land_date
        from backend.db.connection import async_session
        from sqlalchemy import text
    except Exception as e:
        print("Import error:")
        traceback.print_exc()
        return

    # Run ingestion steps
    try:
        print('\n--- Running Sentinel-2 pipeline ---')
        s2 = await process_sentinel2_for_land_day(land_id, date)
        print('Sentinel-2 result:', s2)
    except Exception:
        print('Sentinel-2 failed:')
        traceback.print_exc()

    try:
        print('\n--- Running MODIS pipeline ---')
        mod = await process_modis_for_land_day(land_id, date)
        print('MODIS result:', mod)
    except Exception:
        print('MODIS failed:')
        traceback.print_exc()

    try:
        print('\n--- Running NASA POWER weather pipeline ---')
        wea = await process_weather_for_land(land_id, date, date)
        print('Weather result:', wea)
    except Exception:
        print('Weather failed:')
        traceback.print_exc()

    # Compute anomalies and risk
    try:
        print('\n--- Computing anomalies for date ---')
        anom = await compute_anomalies_for_date(land_id, date, variables=["ndvi","ndmi","lst","t2m","prectotcorr","rh2m"])
        print('Anomalies result:', anom)
    except Exception:
        print('Anomalies failed:')
        traceback.print_exc()

    try:
        print('\n--- Computing risk for date ---')
        risk = await compute_risk_for_land_date(land_id, date)
        print('Risk result:', risk)
    except Exception:
        print('Risk failed:')
        traceback.print_exc()

    # Inspect DB tables
    try:
        print('\n--- DB inspection: land_daily_lst ---')
        async with async_session() as session:
            res = await session.execute(text("SELECT grid_id, date, lst_c, qc FROM land_daily_lst WHERE land_id = :lid ORDER BY date DESC, grid_id LIMIT 200"), {"lid": land_id})
            rows = res.fetchall()
            for r in rows:
                print(r)

        print('\n--- DB inspection: land_daily_indices (NDVI/NDMI) ---')
        async with async_session() as session:
            res = await session.execute(text("SELECT grid_id, date, ndvi, ndmi FROM land_daily_indices WHERE land_id = :lid ORDER BY date DESC, grid_id LIMIT 200"), {"lid": land_id})
            rows = res.fetchall()
            for r in rows:
                print(r)

        print('\n--- DB inspection: land_anomalies (z-scores) ---')
        async with async_session() as session:
            res = await session.execute(text("SELECT grid_id, variable, zscore, value FROM land_anomalies WHERE land_id = :lid AND date = :dt ORDER BY grid_id, variable"), {"lid": land_id, "dt": datetime.fromisoformat(date).date()})
            rows = res.fetchall()
            for r in rows:
                print(r)

        print('\n--- DB inspection: stress_risk_forecast ---')
        async with async_session() as session:
            res = await session.execute(text("SELECT grid_id, date, probability FROM stress_risk_forecast WHERE land_id = :lid ORDER BY date DESC, grid_id LIMIT 200"), {"lid": land_id})
            rows = res.fetchall()
            for r in rows:
                print(r)

    except Exception:
        print('DB inspection failed:')
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
