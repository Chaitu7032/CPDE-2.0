# CPDE Architecture

High-level flow:

- Farmer registers land → stored in PostGIS (lat/lon polygon)
- Land converted into fixed-size spatial grids (10m x 10m)
- Ingest Sentinel-2 (B04, B08, B11), MODIS LST, NASA POWER
- Compute indices: NDVI, NDMI, LST-based metrics
- Detect anomalies, run interpretable forecasting
- Present via dashboard + alerts

Phase 0 created project scaffold and DB config placeholder. Provide PostGIS credentials next to validate connectivity.
