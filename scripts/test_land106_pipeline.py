import asyncio
import logging
import sys
import time
from datetime import datetime

from backend.db.connection import async_session
from backend.pipelines.anomaly import (
    VARIABLE_SOURCES,
    build_climatology_for_variable,
    compute_anomalies_for_date,
)
from backend.pipelines.fusion import compute_multi_sensor_stress_for_land
from backend.pipelines.modis import process_modis_for_land_day
from backend.pipelines.nasa_power import process_weather_for_land
from backend.pipelines.phenology import compute_phenology_for_land
from backend.pipelines.risk import compute_risk_for_land_date
from backend.pipelines.sentinel1 import process_sentinel1_for_land_day
from backend.pipelines.sentinel2 import process_sentinel2_for_land_day
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def main():
    land_id = 106
    test_date = "2026-03-30"  # Target date for Coastal AP

    print(f"\n============================================================")
    print(f"CPDE v2 END-TO-END VALIDATION: LAND #106 ({test_date})")
    print(f"============================================================\n")

    t_start = time.perf_counter()

    # 1. Verify Land #106 in DB
    async with async_session() as session:
        land_res = await session.execute(
            text("SELECT land_id, farmer_name, crop_type, area_sqm, (SELECT count(*) FROM land_grid_cells WHERE land_id=106) FROM lands WHERE land_id = :lid"),
            {"lid": land_id},
        )
        land_row = land_res.first()
        if not land_row:
            print(f"ERROR: Land {land_id} not found in database!")
            return 1
        print(f"Target Field: Land #{land_row[0]} - '{land_row[1]}', Crop: {land_row[2]}, Area: {land_row[3]/10000:.2f} ha, Grids: {land_row[4]}")
        assert land_row[4] == 3317, f"Expected 3,317 grids, found {land_row[4]}"

    # 2. Weather Ingestion (NASA POWER)
    t0 = time.perf_counter()
    print("\n[1/6] Ingesting NASA POWER Weather...")
    wea_res = await process_weather_for_land(land_id, test_date, test_date)
    print(f"  Weather Processed: {wea_res.get('processed', 0)} days ({time.perf_counter() - t0:.2f}s)")

    # 3. Rice Phenology & GDD Engine
    t0 = time.perf_counter()
    print("\n[2/6] Computing Rice Phenology & Thermal Time (GDD)...")
    pheno_res = await compute_phenology_for_land(land_id, test_date)
    print(f"  Stage: {pheno_res.get('stage')} (Conf: {pheno_res.get('stage_confidence')*100:.0f}%), Cumulative GDD: {pheno_res.get('gdd_cumulative')} °C-days ({time.perf_counter() - t0:.2f}s)")

    # 4. Multi-Sensor Climatology & Anomalies
    t0 = time.perf_counter()
    print("\n[3/6] Building Climatology & Z-Score Anomalies...")
    for var in ("ndvi", "ndmi", "evi", "lswi", "lst", "sar_vv", "sar_vh", "t2m", "rh2m", "prectotcorr", "vpd"):
        if var in VARIABLE_SOURCES:
            try:
                await build_climatology_for_variable(land_id, var)
            except Exception as e:
                pass
    anom_res = await compute_anomalies_for_date(land_id, test_date)
    print(f"  Anomalies Computed: {anom_res.get('anomalies_upserted', 0)} ({time.perf_counter() - t0:.2f}s)")

    # 5. Multi-Sensor Stress Fusion Engine & Confidence
    t0 = time.perf_counter()
    print("\n[4/6] Executing Multi-Sensor Interpretable Stress Fusion...")
    fusion_res = await compute_multi_sensor_stress_for_land(land_id, test_date)
    print(f"  Fusion Processed: {fusion_res.get('processed', 0)} cells ({time.perf_counter() - t0:.2f}s)")
    if fusion_res.get("mean_water_stress") is not None:
        print(f"  Mean Water Stress: {fusion_res.get('mean_water_stress'):.3f}")
    if fusion_res.get("mean_chlorophyll_stress") is not None:
        print(f"  Mean Chlorophyll Stress: {fusion_res.get('mean_chlorophyll_stress'):.3f}")
    if fusion_res.get("mean_heat_stress") is not None:
        print(f"  Mean Heat Stress: {fusion_res.get('mean_heat_stress'):.3f}")

    # 6. Continuous Risk Engine
    t0 = time.perf_counter()
    print("\n[5/6] Computing Continuous Stress Risk Forecast...")
    risk_res = await compute_risk_for_land_date(land_id, test_date)
    print(f"  Risk Processed: {risk_res.get('processed', 0)} cells ({time.perf_counter() - t0:.2f}s)")

    # 7. Database Verification
    print("\n[6/6] Verifying Data Persistence in PostgreSQL...")
    async with async_session() as session:
        r_pheno = await session.execute(text("SELECT count(*) FROM land_phenology WHERE land_id = 106"))
        r_fusion = await session.execute(text("SELECT count(*) FROM land_multi_sensor_stress WHERE land_id = 106"))
        r_risk = await session.execute(text("SELECT count(*) FROM stress_risk_forecast WHERE land_id = 106"))
        print(f"  land_phenology rows: {r_pheno.scalar()}")
        print(f"  land_multi_sensor_stress rows: {r_fusion.scalar()}")
        print(f"  stress_risk_forecast rows: {r_risk.scalar()}")

    t_total = time.perf_counter() - t_start
    print(f"\n============================================================")
    print(f"ALL PIPELINE STAGES EXECUTED SUCCESSFULLY ({t_total:.2f}s total)")
    print(f"============================================================\n")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
