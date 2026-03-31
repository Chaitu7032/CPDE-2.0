import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.db.connection import init_db, db_health
from backend.db.init_tables import create_tables
from backend.api.lands import router as lands_router
from backend.api.grids import router as grids_router
from backend.api.sentinel2 import router as sentinel2_router
from backend.api.modis import router as modis_router
from backend.api.weather import router as weather_router
from backend.api.anomalies import router as anomalies_router
from backend.api.forecast import router as forecast_router
from backend.api.dashboard import router as dashboard_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: verify DB and create/migrate tables. Shutdown: dispose engine."""
    logger.info("CPDE API starting up — verifying database...")
    db_ok = await init_db()
    if db_ok:
        logger.info("DB connection OK. Creating/migrating tables...")
        try:
            await create_tables()
            logger.info("Tables ready.")
        except Exception as e:
            logger.error("Table creation failed: %s", e)
    else:
        logger.error("Cannot connect to database! Check DATABASE_URL in .env")
    yield
    # Shutdown
    from backend.db.database import engine
    await engine.dispose()
    logger.info("CPDE API shut down.")


app = FastAPI(title="CPDE API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all for unhandled exceptions — return 500 with detail instead of crashing."""
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
    )


@app.get("/")
async def root():
    return {
        "name": "CPDE API",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health():
    db_ok = await init_db()
    return {"status": "ok" if db_ok else "degraded", "db_connected": db_ok}


@app.get("/db-health")
async def db_health_endpoint():
    """Detailed DB connectivity check (returns error string if connection fails)."""
    return await db_health()


app.include_router(lands_router)
app.include_router(grids_router)
app.include_router(sentinel2_router)
app.include_router(modis_router)
app.include_router(weather_router)
app.include_router(anomalies_router)
app.include_router(forecast_router)
app.include_router(dashboard_router)
