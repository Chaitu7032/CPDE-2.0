import pytest
from backend.pipelines.landsat import ST_SCALE_FACTOR, ST_OFFSET, KELVIN_TO_CELSIUS


def landsat_dn_to_celsius(raw_dn: float) -> float:
    st_k = raw_dn * ST_SCALE_FACTOR + ST_OFFSET
    return st_k - KELVIN_TO_CELSIUS


class TestLandsatThermal:
    def test_landsat_c2_scaling(self):
        # A typical DN value of ~44000
        # 44000 * 0.00341802 + 149.0 = 150.39 + 149.0 = 299.39 K (~26.24 °C)
        temp_c = landsat_dn_to_celsius(44000)
        assert 25.0 < temp_c < 28.0

        # High heat DN ~46000 -> ~33 °C
        temp_c_hot = landsat_dn_to_celsius(46000)
        assert 32.0 < temp_c_hot < 35.0
