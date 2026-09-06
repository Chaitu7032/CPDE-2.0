from sqlalchemy import Boolean, Column, Date, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
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
	# geometry stored in PostGIS in the canonical processing CRS (UTM zone 44N)
	geom = Column(Geometry(geometry_type="POLYGON", srid=32644), nullable=False)
	# centroid stored in the same canonical CRS as geom
	centroid = Column(Geometry(geometry_type="POINT", srid=32644), nullable=True)
	# cached CRS marker for downstream pipelines
	utm_epsg = Column(Integer, nullable=True)
	# optional metadata
	area_sqm = Column(Float, nullable=True)
	created_at = Column(DateTime, nullable=True)


class LandGrid(Base):
	__tablename__ = "land_grid_cells"

	id = Column(Integer, primary_key=True, autoincrement=True)
	grid_id = Column(String(128), unique=True, index=True, nullable=False)
	land_id = Column(Integer, nullable=False, index=True)
	grid_num = Column(Integer, nullable=True, index=True)
	row_idx = Column(Integer, nullable=True)
	col_idx = Column(Integer, nullable=True)
	# store geometry in canonical UTM processing CRS
	geom = Column(Geometry(geometry_type="POLYGON", srid=32644), nullable=False)
	centroid = Column(Geometry(geometry_type="POINT", srid=32644), nullable=True)
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
	stac_item_id = Column(String(256), nullable=True)
	acquisition_datetime = Column(DateTime(timezone=True), nullable=True)
	tile_id = Column(String(64), nullable=True)
	cloud_cover_pct = Column(Float, nullable=True)
	b04 = Column(Float, nullable=True)
	b08 = Column(Float, nullable=True)
	b11 = Column(Float, nullable=True)
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


class LandDailySAR(Base):
	__tablename__ = "land_daily_sar"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_land_daily_sar_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	stac_item_id = Column(String(256), nullable=True)
	acquisition_datetime = Column(DateTime(timezone=True), nullable=True)
	orbit_direction = Column(String(16), nullable=True)  # ascending / descending
	vv = Column(Float, nullable=True)
	vh = Column(Float, nullable=True)
	vh_vv_ratio = Column(Float, nullable=True)
	delta_vv = Column(Float, nullable=True)
	delta_vh = Column(Float, nullable=True)
	temporal_vh_slope = Column(Float, nullable=True)
	native_resolution_m = Column(Float, default=10.0)
	quality_flag = Column(String(32), default="VALID")

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandDailySAR {self.land_id} {self.grid_id} {self.date} VV={self.vv} VH={self.vh}>"


class LandPhenology(Base):
	__tablename__ = "land_phenology"

	__table_args__ = (
		UniqueConstraint("land_id", "date", name="uq_land_phenology_land_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	gdd_daily = Column(Float, nullable=True)
	gdd_cumulative = Column(Float, nullable=True)
	stage = Column(String(64), nullable=False)  # flooding, transplanting, tillering, heading, maturity
	stage_confidence = Column(Float, nullable=True)
	days_since_planting = Column(Integer, nullable=True)
	phenological_evidence = Column(Text, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandPhenology {self.land_id} {self.date} stage={self.stage} GDD={self.gdd_cumulative}>"


class LandMultiSensorStress(Base):
	__tablename__ = "land_multi_sensor_stress"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_land_multi_sensor_stress_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	land_id = Column(Integer, nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	water_stress_score = Column(Float, nullable=True)
	chlorophyll_stress_score = Column(Float, nullable=True)
	heat_stress_score = Column(Float, nullable=True)
	flooding_state = Column(String(32), nullable=True)  # flooded_rice, dry_canopy, saturated, permanent_water
	confidence_score = Column(Float, nullable=True)
	confidence_level = Column(String(16), nullable=True)  # HIGH, MEDIUM, LOW, INSUFFICIENT_DATA
	dominant_stress = Column(String(64), nullable=True)
	evidence_json = Column(Text, nullable=True)
	is_water = Column(Boolean, nullable=True)

	def __repr__(self) -> str:  # pragma: no cover
		return f"<LandMultiSensorStress {self.land_id} {self.grid_id} {self.date} stress={self.dominant_stress} conf={self.confidence_score}>"


# ==============================================================================
# CPDE v2 Research-Grade Production Models
# ==============================================================================

class User(Base):
	__tablename__ = "users"

	id = Column(Integer, primary_key=True, autoincrement=True)
	email = Column(String(255), unique=True, index=True, nullable=False)
	hashed_password = Column(String(255), nullable=False)
	name = Column(String(255), nullable=False)
	phone = Column(String(32), nullable=True)
	role = Column(String(32), nullable=False, default="farmer")  # farmer, researcher, admin
	state = Column(String(64), nullable=True)
	district = Column(String(64), nullable=True)
	mandal = Column(String(64), nullable=True)
	village = Column(String(64), nullable=True)
	created_at = Column(DateTime(timezone=True), server_default=text("now()"))


class Farm(Base):
	__tablename__ = "farms"

	id = Column(Integer, primary_key=True, autoincrement=True)
	user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
	name = Column(String(255), nullable=False)
	description = Column(Text, nullable=True)
	state = Column(String(64), nullable=True)
	district = Column(String(64), nullable=True)
	mandal = Column(String(64), nullable=True)
	village = Column(String(64), nullable=True)
	created_at = Column(DateTime(timezone=True), server_default=text("now()"))


class Field(Base):
	__tablename__ = "fields"

	id = Column(Integer, primary_key=True, autoincrement=True)
	farm_id = Column(Integer, ForeignKey("farms.id", ondelete="CASCADE"), nullable=False, index=True)
	name = Column(String(255), nullable=False)
	crop_type = Column(String(64), nullable=True)
	sowing_date = Column(Date, nullable=True)
	geom = Column(Geometry(geometry_type="POLYGON", srid=32644), nullable=False)
	centroid = Column(Geometry(geometry_type="POINT", srid=32644), nullable=True)
	area_sqm = Column(Float, nullable=True)
	area_ha = Column(Float, nullable=True)
	perimeter_m = Column(Float, nullable=True)
	utm_epsg = Column(Integer, nullable=True)
	bbox = Column(Text, nullable=True)
	village = Column(String(128), nullable=True)
	mandal = Column(String(128), nullable=True)
	district = Column(String(128), nullable=True)
	state = Column(String(128), nullable=True)
	country = Column(String(64), nullable=True)
	geometry_quality_score = Column(Float, nullable=True)
	registration_confidence = Column(Float, nullable=True)
	created_at = Column(DateTime(timezone=True), server_default=text("now()"))


class FieldGridCell(Base):
	__tablename__ = "field_grid_cells"

	id = Column(Integer, primary_key=True, autoincrement=True)
	field_id = Column(Integer, ForeignKey("fields.id", ondelete="CASCADE"), nullable=False, index=True)
	grid_id = Column(String(128), unique=True, index=True, nullable=False)
	grid_num = Column(Integer, nullable=True, index=True)
	row_idx = Column(Integer, nullable=True)
	col_idx = Column(Integer, nullable=True)
	geom = Column(Geometry(geometry_type="POLYGON", srid=32644), nullable=False)
	centroid = Column(Geometry(geometry_type="POINT", srid=32644), nullable=True)
	area_sqm = Column(Float, nullable=True)
	is_water = Column(Boolean, nullable=True)


class FieldObservation(Base):
	__tablename__ = "field_observations"

	__table_args__ = (
		UniqueConstraint("field_id", "date", name="uq_field_observations_field_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	field_id = Column(Integer, ForeignKey("fields.id", ondelete="CASCADE"), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	satellite_source = Column(String(64), nullable=True)
	stac_item_id = Column(String(256), nullable=True)
	tile_id = Column(String(64), nullable=True)
	cloud_cover_pct = Column(Float, nullable=True)
	acquisition_datetime = Column(DateTime(timezone=True), nullable=True)
	processing_level = Column(String(32), nullable=True, default="L2A")
	quality_score = Column(Float, nullable=True)
	confidence_score = Column(Float, nullable=True)
	growth_stage = Column(String(64), nullable=True)
	weather_summary = Column(Text, nullable=True)
	created_at = Column(DateTime(timezone=True), server_default=text("now()"))


class GridObservation(Base):
	__tablename__ = "grid_observations"

	__table_args__ = (
		UniqueConstraint("grid_id", "date", name="uq_grid_observations_grid_date"),
	)

	id = Column(Integer, primary_key=True, autoincrement=True)
	field_id = Column(Integer, ForeignKey("fields.id", ondelete="CASCADE"), nullable=False, index=True)
	grid_id = Column(String(128), nullable=False, index=True)
	date = Column(Date, nullable=False, index=True)
	b02 = Column(Float, nullable=True)
	b03 = Column(Float, nullable=True)
	b04 = Column(Float, nullable=True)
	b05 = Column(Float, nullable=True)
	b06 = Column(Float, nullable=True)
	b07 = Column(Float, nullable=True)
	b08 = Column(Float, nullable=True)
	b8a = Column(Float, nullable=True)
	b11 = Column(Float, nullable=True)
	b12 = Column(Float, nullable=True)
	scl = Column(Integer, nullable=True)
	ndvi = Column(Float, nullable=True)
	ndmi = Column(Float, nullable=True)
	ndre = Column(Float, nullable=True)
	evi = Column(Float, nullable=True)
	savi = Column(Float, nullable=True)
	gci = Column(Float, nullable=True)
	quality_score = Column(Float, nullable=True)
	confidence_score = Column(Float, nullable=True)
	health_status = Column(String(32), nullable=True)
	pre_cause_alerts = Column(Text, nullable=True)


class GroundTruthSample(Base):
	__tablename__ = "ground_truth_samples"

	id = Column(Integer, primary_key=True, autoincrement=True)
	field_id = Column(Integer, ForeignKey("fields.id", ondelete="CASCADE"), nullable=False, index=True)
	grid_id = Column(String(128), nullable=True)
	recorded_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
	date = Column(Date, nullable=False, index=True)
	crop_stage = Column(String(64), nullable=True)
	observed_condition = Column(String(64), nullable=False)
	photo_url = Column(Text, nullable=True)
	notes = Column(Text, nullable=True)
	latitude = Column(Float, nullable=True)
	longitude = Column(Float, nullable=True)
	created_at = Column(DateTime(timezone=True), server_default=text("now()"))

