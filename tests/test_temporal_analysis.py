from datetime import date, timedelta
import pytest

from backend.services.temporal_comparison import _build_trend, _compute_vpd


class TestTemporalAnalysis:
    def test_compute_vpd_valid(self):
        # 30°C and 50% RH
        vpd = _compute_vpd(30.0, 50.0)
        assert vpd is not None
        # Saturation vapor pressure at 30°C: ~4.24 kPa. At 50% RH: VPD ~ 2.12 kPa
        assert 2.0 < vpd < 2.3

    def test_compute_vpd_edge_cases(self):
        assert _compute_vpd(None, 50.0) is None
        assert _compute_vpd(30.0, None) is None
        assert _compute_vpd(30.0, 100.0) == 0.0  # 100% RH -> 0 VPD
        assert _compute_vpd(30.0, -10.0) is None
        assert _compute_vpd(30.0, 150.0) is None

    def test_build_trend_insufficient_history(self):
        # Empty history
        res = _build_trend([], {"value": 0.5, "date": date(2026, 8, 1)})
        assert res is not None
        assert res["status"] == "INSUFFICIENT_DATA"

        # Only 1 point
        history = [{"value": 0.5, "date": date(2026, 8, 1)}]
        res1 = _build_trend(history, {"value": 0.5, "date": date(2026, 8, 1)})
        assert res1 is not None
        assert res1["status"] == "INSUFFICIENT_DATA"

    def test_build_trend_rising_ols(self):
        # 4 points steadily rising
        base = date(2026, 8, 1)
        history = [
            {"value": 0.20, "date": base},
            {"value": 0.30, "date": base + timedelta(days=5)},
            {"value": 0.40, "date": base + timedelta(days=10)},
            {"value": 0.50, "date": base + timedelta(days=15)},
        ]
        ref = history[-1]
        res = _build_trend(history, ref)
        assert res is not None
        assert res["status"] == "ADEQUATE"
        assert res["direction"] == "up"
        assert res["slope_per_day"] == pytest.approx(0.02, rel=1e-2)
        assert res["r_squared"] == pytest.approx(1.0, rel=1e-2)
        assert res["observation_count"] == 4

    def test_build_trend_falling_ols(self):
        # 4 points steadily falling
        base = date(2026, 8, 1)
        history = [
            {"value": 0.60, "date": base},
            {"value": 0.50, "date": base + timedelta(days=5)},
            {"value": 0.40, "date": base + timedelta(days=10)},
            {"value": 0.30, "date": base + timedelta(days=15)},
        ]
        ref = history[-1]
        res = _build_trend(history, ref)
        assert res is not None
        assert res["status"] == "ADEQUATE"
        assert res["direction"] == "down"
        assert res["slope_per_day"] == pytest.approx(-0.02, rel=1e-2)
        assert res["observation_count"] == 4
