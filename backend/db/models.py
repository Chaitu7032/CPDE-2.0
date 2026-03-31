from sqlalchemy import Boolean, Column, Date, DateTime, Float, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import declarative_base

from geoalchemy2 import Geometry


Base = declarative_base()


class Land(Base):
	__tablename__ = "lands"

	# IMPORTANT: This model maps the existing DB schema (see scripts/inspect_schema.py).
	# Primary key used everywhere in the pipelines.
	land_id = Column(Integer, primary_key=True, autoincrement=True)
	farmer_name = Column(String, nullable=False)
	crop_type = Column(String(64), nullable=True)
	# geometry stored in PostGIS (SRID 4326, POLYGON)
	geom = Column(Geometry(geometry_type="POLYGON", srid=4326), nullable=False)
	# cached centroid for point-based queries (weather, UTM selection)
	centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=True)
	# cached UTM EPSG for this land (Phase 2+)
	utm_epsg = Column(Integer, nullable=True)
	# optional metadata
	area_sqm = Column(Float, nullable=True)
	created_at = Column(DateTime, nullable=True)


class LandGrid(Base):
	__tablename__ = "land_grid_cells"

	id = Column(Integer, primary_key=True, autoincrement=True)
	grid_id = Column(String(128), unique=True, index=True, nullable=False)
	land_id = Column(Integer, nullable=False, index=True)
	# store geometry in WGS84 so all stored geometries are consistent
	geom = Column(Geometry(geometry_type="POLYGON", srid=4326), nullable=False)
	centroid = Column(Geometry(geometry_type="POINT", srid=4326), nullable=True)
	# derived from Sentinel-2 SCL (value==6); used as water mask for MODIS
	is_water = Column(Boolean, nullable=True)

	__table_args__ = (
		# create GIST spatial index for fast spatial queries
		{
			"info": {"comment": "Spatial index created separately if needed"}
		},
	)

	def __repr__(self) -> str:  # pragma: no cover - simple repr
		return f"<LandGrid grid_id={self.grid_id} land_id={self.land_id}>"


class LandDailyIndex(Base):
	__tablename__ = "land_daily_indices"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_land_daily_indices_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	ndvi = Column(Float, nullable=True)
	ndmi = Column(Float, nullable=True)
	pixel_count = Column(Integer, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover - simple repr
		return f"<LandDailyIndex {self.land_id} {self.grid_id} {self.date} NDVI={self.ndvi}>"


class LandDailyLST(Base):
	__tablename__ = "land_daily_lst"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_land_daily_lst_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	lst_c = Column(Float, nullable=True)
	qc = Column(Integer, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover - simple repr
		return f"<LandDailyLST {self.land_id} {self.grid_id} {self.date} LST={self.lst_c}>"


class LandLSTClimatology(Base):
	__tablename__ = "land_lst_climatology"

	__table_args__ = (
		UniqueConstraint("grid_id", "day_of_year", name="uq_land_lst_climatology_grid_doy"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	day_of_year = Column(Integer, nullable=False, index=True)
	lst_mean = Column(Float, nullable=True)
	lst_std = Column(Float, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandLSTClimatology {self.land_id} {self.grid_id} DOY={self.day_of_year}>"


class LandClimatology(Base):
	__tablename__ = "land_climatology"

	__table_args__ = (
		UniqueConstraint("land_id", "grid_id", "variable", "day_of_year", name="uq_land_climatology_land_grid_var_doy"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	variable = Column(String(32), nullable=False, index=True)  # e.g., ndvi, ndmi, lst, prectotcorr
	day_of_year = Column(Integer, nullable=False, index=True)
	mean = Column(Float, nullable=True)
	std = Column(Float, nullable=True)
	count = Column(Integer, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandClimatology {self.land_id} {self.grid_id} {self.variable} DOY={self.day_of_year}>"


class LandAnomaly(Base):
	__tablename__ = "land_anomalies"

	__table_args__ = (
		UniqueConstraint("land_id", "grid_id", "date", "variable", name="uq_land_anomalies_key"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	variable = Column(String(32), nullable=False)
	value = Column(Float, nullable=True)
	mean = Column(Float, nullable=True)
	std = Column(Float, nullable=True)
	zscore = Column(Float, nullable=True)
	pixel_count = Column(Integer, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandAnomaly {self.land_id} {self.grid_id} {self.variable} {self.date} z={self.zscore}>"


class StressRiskForecast(Base):
	__tablename__ = "stress_risk_forecast"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_stress_risk_forecast_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	probability = Column(Float, nullable=True)
	expected_lead_time = Column(Float, nullable=True)
	model_version = Column(String(64), nullable=True)
	created_at = Column(Date, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<StressRiskForecast {self.land_id} {self.grid_id} {self.date} p={self.probability}>"


class LandDailyWeather(Base):
	__tablename__ = "land_daily_weather"

	__table_args__ = (
		UniqueConstraint("land_id", "date", name="uq_land_daily_weather_land_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	t2m = Column(Float, nullable=True)
	rh2m = Column(Float, nullable=True)
	prectotcorr = Column(Float, nullable=True)
	source = Column(String(64), nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandDailyWeather {self.land_id} {self.date} T2M={self.t2m}>"


class ProcessingJob(Base):
	__tablename__ = "processing_jobs"

	__table_args__ = (
		Index("idx_processing_jobs_status", "status"),
	)

	land_id = Column(Integer, primary_key=True)
	status = Column(Text, nullable=False, server_default=text("'unknown'"))
	step = Column(Text, nullable=True)
	error = Column(Text, nullable=True)
	updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
