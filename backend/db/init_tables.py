import logging

from backend.db.connection import engine
from backend.db.models import Base
from sqlalchemy import text

logger = logging.getLogger(__name__)


async def create_tables():
    """Create DB tables (runs SQLAlchemy create_all in sync context via engine.run_sync).

    Note: This requires a reachable Postgres/PostGIS instance. Provide DB credentials
    in `.env` (DATABASE_URL) before running.
    """
    async with engine.begin() as conn:
        # Ensure PostGIS is available
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))

        # ── Fix legacy land_grid_cells schema ─────────────────────────────
        # Older DB versions had grid_id as INTEGER (auto-increment PK) instead
        # of VARCHAR(128). The ORM model requires VARCHAR(128) grid_id + separate
        # INTEGER id PK. Detect and rebuild if necessary.
        check = await conn.execute(text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'land_grid_cells' AND column_name = 'grid_id'"
        ))
        row = check.first()
        if row and row[0] == "integer":
            logger.warning(
                "land_grid_cells.grid_id is INTEGER — rebuilding table with correct VARCHAR(128) schema"
            )
            await conn.execute(text("DROP TABLE IF EXISTS land_grid_cells CASCADE"))

        # Create all tables from ORM models (idempotent — skips already-existing)
        await conn.run_sync(Base.metadata.create_all)

        # Lightweight local schema evolution (use Alembic for production).
        # Add `crop_type` if the database was created before this column existed.
        await conn.execute(text("ALTER TABLE IF EXISTS lands ADD COLUMN IF NOT EXISTS crop_type VARCHAR(64)"))

        # Phase 1/2 cached geometry + CRS metadata
        await conn.execute(text("ALTER TABLE IF EXISTS lands ADD COLUMN IF NOT EXISTS centroid geometry(POINT, 32644)"))
        await conn.execute(text("ALTER TABLE IF EXISTS lands ADD COLUMN IF NOT EXISTS utm_epsg INTEGER"))
        await conn.execute(text("ALTER TABLE IF EXISTS lands ADD COLUMN IF NOT EXISTS area_sqm DOUBLE PRECISION"))
        await conn.execute(text("ALTER TABLE IF EXISTS lands ADD COLUMN IF NOT EXISTS created_at TIMESTAMP"))

        # Phase 2/3 grid helpers
        await conn.execute(text("ALTER TABLE IF EXISTS land_grid_cells ADD COLUMN IF NOT EXISTS centroid geometry(POINT, 32644)"))
        await conn.execute(text("ALTER TABLE IF EXISTS land_grid_cells ADD COLUMN IF NOT EXISTS is_water BOOLEAN"))

        # Canonical CRS enforcement: all persisted geometries must be UTM 44N (EPSG:32644).
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS lands "
                "ALTER COLUMN geom TYPE geometry(POLYGON, 32644) "
                "USING ST_Transform(geom, 32644)"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS lands "
                "ALTER COLUMN centroid TYPE geometry(POINT, 32644) "
                "USING CASE WHEN centroid IS NULL THEN NULL ELSE ST_Transform(centroid, 32644) END"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_grid_cells "
                "ALTER COLUMN geom TYPE geometry(POLYGON, 32644) "
                "USING ST_Transform(geom, 32644)"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_grid_cells "
                "ALTER COLUMN centroid TYPE geometry(POINT, 32644) "
                "USING CASE WHEN centroid IS NULL THEN NULL ELSE ST_Transform(centroid, 32644) END"
            )
        )
        await conn.execute(text("UPDATE lands SET utm_epsg = 32644 WHERE utm_epsg IS DISTINCT FROM 32644"))

        # Helpful indexes for sampling and timeseries
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_land_grid_cells_land_id ON land_grid_cells (land_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lands_geom_gist ON lands USING GIST (geom)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_land_grid_cells_geom_gist ON land_grid_cells USING GIST (geom)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_land_daily_indices_land_date ON land_daily_indices (land_id, date)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_land_daily_lst_land_date ON land_daily_lst (land_id, date)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_land_daily_weather_land_date ON land_daily_weather (land_id, date)"))

        # Phase 5/6 climatology/anomaly keys
        # Older schemas used a uniqueness constraint that omitted land_id, which breaks multi-land usage
        # (notably the land-level sentinel grid_id='__land__').
        await conn.execute(text("ALTER TABLE IF EXISTS land_climatology DROP CONSTRAINT IF EXISTS uq_land_climatology_grid_var_doy"))
        await conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'uq_land_climatology_land_grid_var_doy'
                    ) THEN
                        ALTER TABLE land_climatology
                        ADD CONSTRAINT uq_land_climatology_land_grid_var_doy
                        UNIQUE (land_id, grid_id, variable, day_of_year);
                    END IF;
                END $$;
                """
            )
        )

        # Ensure land_id type consistency with existing `lands.land_id` (INTEGER).
        # These tables were previously created with VARCHAR land_id in some local setups.
        # Cast is safe as long as stored values are numeric strings (e.g., '1').
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_daily_indices ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_daily_lst ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_daily_weather ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_climatology ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS land_anomalies ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE IF EXISTS stress_risk_forecast ALTER COLUMN land_id TYPE INTEGER USING land_id::integer"
            )
        )
