import numpy as np
import pytest

from backend.pipelines.sentinel2 import _safe_sample_val as s2_safe_sample
from backend.pipelines.modis import _safe_sample_val as modis_safe_sample


@pytest.mark.parametrize("safe_func", [s2_safe_sample, modis_safe_sample])
class TestSafeSampleVal:
    def test_scalar_unmasked_value(self, safe_func):
        """1. scalar unmasked value"""
        # Python native scalar
        assert safe_func(123.45) == pytest.approx(123.45)
        # Numpy float scalar
        assert safe_func(np.float64(456.78)) == pytest.approx(456.78)
        # 0-d array
        assert safe_func(np.array(789.0)) == pytest.approx(789.0)
        # 0-d masked array with mask=False
        assert safe_func(np.ma.array(999.0, mask=False)) == pytest.approx(999.0)

    def test_scalar_masked_value(self, safe_func):
        """2. scalar masked value"""
        default_val = safe_func(np.ma.masked)
        assert default_val is None or np.isnan(default_val)

        # 0-d masked array with mask=True
        masked_0d = np.ma.array(123.45, mask=True)
        res = safe_func(masked_0d)
        assert res is None or np.isnan(res)

        # scalar with numpy bool mask
        masked_scalar_bool = np.ma.masked_array(123.45, mask=np.bool_(True))
        res = safe_func(masked_scalar_bool)
        assert res is None or np.isnan(res)

    def test_array_mask(self, safe_func):
        """3. array mask"""
        # 1D array unmasked
        arr_unmasked = np.ma.array([123.0], mask=[False])
        assert safe_func(arr_unmasked) == pytest.approx(123.0)

        # 1D array masked
        arr_masked = np.ma.array([123.0], mask=[True])
        res = safe_func(arr_masked)
        assert res is None or np.isnan(res)

        # 1D array with scalar mask=True
        arr_scalar_mask = np.ma.array([123.0])
        arr_scalar_mask.mask = True
        res = safe_func(arr_scalar_mask)
        assert res is None or np.isnan(res)

    def test_nodata(self, safe_func):
        """4. nodata (None, custom default)"""
        assert safe_func(None, default=np.nan) is np.nan or np.isnan(safe_func(None, default=np.nan))
        assert safe_func(None, default=-9999.0) == -9999.0
        assert safe_func(None, default=None) is None

    def test_nan(self, safe_func):
        """5. NaN handling"""
        res_nan = safe_func(np.nan)
        assert res_nan is None or np.isnan(res_nan)

        res_arr_nan = safe_func(np.array([np.nan]))
        assert res_arr_nan is None or np.isnan(res_arr_nan)

        res_ma_nan = safe_func(np.ma.array([np.nan], mask=[False]))
        assert res_ma_nan is None or np.isnan(res_ma_nan)

    def test_valid_float(self, safe_func):
        """6. valid float"""
        assert safe_func(0.0) == 0.0
        assert safe_func(10000.0) == 10000.0
        assert safe_func(-5.2) == pytest.approx(-5.2)

    def test_invalid_sample(self, safe_func):
        """7. invalid sample (empty list, string, incompatible object)"""
        res_empty = safe_func([])
        assert res_empty is None or np.isnan(res_empty)

        res_str = safe_func("corrupted_data")
        assert res_str is None or np.isnan(res_str)

        res_obj = safe_func(object())
        assert res_obj is None or np.isnan(res_obj)

    def test_multiple_bands(self, safe_func):
        """8. multiple bands sample"""
        # Multi-band sample where band 0 is valid
        multi_band_valid = np.ma.array([150.0, 300.0, 450.0], mask=[False, True, False])
        assert safe_func(multi_band_valid, band_idx=0) == pytest.approx(150.0)

        # Multi-band sample where band 1 is masked
        res_b1 = safe_func(multi_band_valid, band_idx=1)
        assert res_b1 is None or np.isnan(res_b1)

        # Multi-band sample where band 2 is valid
        assert safe_func(multi_band_valid, band_idx=2) == pytest.approx(450.0)
