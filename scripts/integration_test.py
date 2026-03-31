"""Integration test: exercises all API phases sequentially."""
import requests
import json
import sys

BASE = "http://localhost:8000"

def main():
    errors = []

    # Phase 1: Health & DB
    print("=== PHASE 1: Health & DB ===")
    r = requests.get(f"{BASE}/health")
    print(f"  /health: {r.status_code} {r.json()}")
    if r.status_code != 200 or not r.json().get("db_connected"):
        errors.append("Health check failed")

    r = requests.get(f"{BASE}/db-health")
    print(f"  /db-health: {r.status_code} {r.json()}")
    if r.status_code != 200 or not r.json().get("ok"):
        errors.append("DB health check failed")

    # Phase 2: Land Registration
    print("\n=== PHASE 2: Land Registration ===")
    payload = {
        "farmer_name": "Integration Test",
        "crop_type": "maize",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[36.8, -1.28], [36.801, -1.28], [36.801, -1.281], [36.8, -1.281], [36.8, -1.28]]]
        }
    }
    r = requests.post(f"{BASE}/lands/", json=payload)
    print(f"  POST /lands/: {r.status_code} {r.json()}")
    if r.status_code != 201:
        errors.append(f"Land registration failed: {r.status_code} {r.text}")
        print(f"\nFATAL: Cannot proceed without land_id. Errors: {errors}")
        return 1

    land_data = r.json()
    land_id = land_data["land_id"]
    print(f"  land_id={land_id}, utm_epsg={land_data.get('utm_epsg')}, area_sqm={land_data.get('area_sqm')}")

    # Fetch land back
    r = requests.get(f"{BASE}/lands/{land_id}")
    resp = r.json()
    geom_ok = resp.get("geometry") is not None
    print(f"  GET /lands/{land_id}: {r.status_code}, geometry_present={geom_ok}")
    if r.status_code != 200 or not geom_ok:
        errors.append("Land fetch failed or missing geometry")

    # Phase 6: Grid Generation
    print("\n=== PHASE 6: Grid Generation ===")
    r = requests.post(f"{BASE}/grids/generate", json={"land_id": land_id, "cell_size_m": 10.0})
    print(f"  POST /grids/generate: {r.status_code}")
    if r.status_code == 200:
        grid_data = r.json()
        grid_count = grid_data.get("count", 0)
        print(f"  Grid count: {grid_count}")
        if grid_count == 0:
            errors.append("Grid generation returned 0 grids")
    else:
        errors.append(f"Grid generation failed: {r.status_code} {r.text}")
        grid_count = 0

    # Fetch grids
    r = requests.get(f"{BASE}/grids/{land_id}")
    features = r.json().get("features", [])
    print(f"  GET /grids/{land_id}: {r.status_code}, features={len(features)}")

    # Phase 5: NASA POWER Weather
    print("\n=== PHASE 5: NASA POWER Weather ===")
    r = requests.post(f"{BASE}/weather/fetch", json={
        "land_id": land_id,
        "start_date": "2024-06-01",
        "end_date": "2024-06-07",
    })
    print(f"  POST /weather/fetch: {r.status_code} {r.json()}")
    if r.status_code in (200,):
        print(f"  Weather processed: {r.json().get('processed', 0)} days")
    elif r.status_code == 404:
        print(f"  No weather data (expected for some dates): {r.json()}")
    else:
        errors.append(f"Weather fetch error: {r.status_code} {r.text}")

    # Phase 3: Sentinel-2 (may 404 if no imagery for date/area)
    print("\n=== PHASE 3: Sentinel-2 ===")
    r = requests.post(f"{BASE}/sentinel2/process", json={
        "land_id": land_id,
        "date": "2024-06-01",
    })
    print(f"  POST /sentinel2/process: {r.status_code}")
    if r.status_code == 200:
        print(f"  S2 processed: {r.json()}")
    elif r.status_code == 404:
        print(f"  No S2 data for date (OK for test): {r.json().get('detail','')}")
    else:
        resp_text = r.text[:200]
        print(f"  S2 error: {resp_text}")
        errors.append(f"Sentinel-2 processing error: {r.status_code}")

    # Phase 4: MODIS LST (requires Earthdata credentials)
    print("\n=== PHASE 4: MODIS LST ===")
    r = requests.post(f"{BASE}/modis/process", json={
        "land_id": land_id,
        "date": "2024-06-01",
    })
    print(f"  POST /modis/process: {r.status_code}")
    if r.status_code == 200:
        print(f"  MODIS processed: {r.json()}")
    elif r.status_code == 404:
        print(f"  No MODIS data / credentials (OK for test): {r.json().get('detail','')}")
    else:
        resp_text = r.text[:200]
        print(f"  MODIS error: {resp_text}")
        # MODIS failure due to missing credentials is acceptable
        if "EARTHDATA" in r.text:
            print("  (Expected: Missing Earthdata credentials)")
        else:
            errors.append(f"MODIS processing error: {r.status_code}")

    # Summary
    print("\n" + "=" * 50)
    if errors:
        print(f"ISSUES FOUND ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
        return 1
    else:
        print("ALL PHASES PASSED SUCCESSFULLY")
        return 0

if __name__ == "__main__":
    sys.exit(main())
