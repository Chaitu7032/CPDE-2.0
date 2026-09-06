import pytest
from backend.pipelines.fusion import (
    calculate_data_confidence,
    calculate_diagnosis_confidence,
    _sigmoid,
)


class TestMultiSensorFusion:
    def test_sigmoid_numerical_stability(self):
        assert _sigmoid(0.0) == pytest.approx(0.5)
        assert _sigmoid(100.0) == pytest.approx(1.0)
        assert _sigmoid(-100.0) == pytest.approx(0.0)

    def test_high_data_confidence_all_sensors(self):
        score, level = calculate_data_confidence(
            has_optical=True,
            has_sar=True,
            has_thermal=True,
            has_weather=True,
            cloud_cover_pct=10.0,
            thermal_source="LANDSAT_8",
        )
        assert score >= 0.75
        assert level == "HIGH"

    def test_medium_data_confidence_optical_plus_weather(self):
        score, level = calculate_data_confidence(
            has_optical=True,
            has_sar=False,
            has_thermal=False,
            has_weather=True,
            cloud_cover_pct=20.0,
        )
        assert 0.45 <= score < 0.75
        assert level == "MEDIUM"

    def test_low_data_confidence_single_sensor(self):
        score, level = calculate_data_confidence(
            has_optical=False,
            has_sar=True,
            has_thermal=False,
            has_weather=False,
        )
        assert 0.20 <= score < 0.45
        assert level == "LOW"

    def test_diagnosis_confidence_corroborated(self):
        # 3 agreeing sensors with high data confidence
        diag_score, diag_level = calculate_diagnosis_confidence(
            data_confidence_score=0.85,
            sensor_agreement=True,
            supporting_sensor_count=3,
        )
        assert diag_score >= 0.70
        assert diag_level == "HIGH"

    def test_diagnosis_confidence_conflicting_sensors(self):
        # Conflicting sensors downgrade diagnosis confidence
        diag_score, diag_level = calculate_diagnosis_confidence(
            data_confidence_score=0.85,
            sensor_agreement=False,
            supporting_sensor_count=2,
        )
        assert diag_score < 0.50
        assert diag_level in ("MEDIUM", "LOW", "UNCERTAIN")
