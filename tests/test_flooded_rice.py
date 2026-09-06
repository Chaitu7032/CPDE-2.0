import pytest
import numpy as np


def classify_rice_pixel(scl: int, red: float, nir: float, swir: float):
    """Testable model of the flooded rice classifier based on Xiao et al. (2005)."""
    is_scl_water = scl == 6
    is_clear = (scl in (2, 4, 5, 7)) or is_scl_water

    if not (is_clear and np.isfinite(red) and np.isfinite(nir) and np.isfinite(swir)):
        return {"usable": False, "flooded_rice": False, "is_water": False}

    denom_ndvi = nir + red
    denom_ndmi = nir + swir

    ndvi = (nir - red) / denom_ndvi if denom_ndvi != 0 else 0.0
    ndmi = (nir - swir) / denom_ndmi if denom_ndmi != 0 else 0.0
    lswi = ndmi
    evi = 2.5 * (nir - red) / (nir + 2.4 * red + 1.0) if (nir + 2.4 * red + 1.0) != 0 else 0.0

    is_flooded_rice = False
    if lswi is not None and ndvi is not None:
        min_veg = min(ndvi, evi)
        if (lswi + 0.05) >= min_veg or is_scl_water:
            is_flooded_rice = True

    return {
        "usable": True,
        "flooded_rice": is_flooded_rice,
        "is_water": is_scl_water and not is_flooded_rice,
        "ndvi": ndvi,
        "lswi": lswi,
    }


class TestFloodedRiceClassifier:
    def test_flooded_rice_during_transplanting(self):
        # Flooded rice: NIR is low-moderate, SWIR is very low (absorbed by water), Red is low
        # SCL might be 6 (water) or 4 (vegetation)
        res = classify_rice_pixel(scl=6, red=400.0, nir=800.0, swir=200.0)
        assert res["usable"] is True
        assert res["flooded_rice"] is True
        assert res["is_water"] is False  # not permanent water
        assert res["lswi"] > 0.5

    def test_healthy_vegetative_rice(self):
        # Canopy closed: high NIR, low Red, moderate SWIR, SCL=4 (vegetation)
        res = classify_rice_pixel(scl=4, red=300.0, nir=3500.0, swir=1200.0)
        assert res["usable"] is True
        assert res["flooded_rice"] is False
        assert res["is_water"] is False
        assert res["ndvi"] > 0.8

    def test_cloud_masked_pixel(self):
        # SCL=9 (high probability cloud)
        res = classify_rice_pixel(scl=9, red=3000.0, nir=4000.0, swir=3500.0)
        assert res["usable"] is False
