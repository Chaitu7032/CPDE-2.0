import pytest
from backend.pipelines.phenology import (
    calculate_daily_gdd,
    determine_phenology_stage,
    RICE_T_BASE_C,
)


class TestRicePhenology:
    def test_daily_gdd_calculation(self):
        # Temp = 28°C, T_base = 10°C -> GDD = 18°C-days
        assert calculate_daily_gdd(28.0, t_base_c=10.0) == pytest.approx(18.0)

        # Temp below base temp (8°C) -> GDD = 0
        assert calculate_daily_gdd(8.0, t_base_c=10.0) == 0.0

        # Null / NaN temp -> GDD = 0
        assert calculate_daily_gdd(None, t_base_c=10.0) == 0.0

    def test_phenology_stage_mapping(self):
        # 50 GDD -> Flooding / Land Prep
        stage, conf = determine_phenology_stage(50.0)
        assert "Flooding" in stage
        assert conf > 0.7

        # 200 GDD -> Transplanting
        stage, conf = determine_phenology_stage(200.0)
        assert "Transplanting" in stage

        # 500 GDD -> Tillering
        stage, conf = determine_phenology_stage(500.0)
        assert "Tillering" in stage

        # 1200 GDD -> Heading / Flowering
        stage, conf = determine_phenology_stage(1200.0)
        assert "Heading" in stage

        # 2000 GDD -> Maturity / Harvest Ready
        stage, conf = determine_phenology_stage(2000.0)
        assert "Maturity" in stage
