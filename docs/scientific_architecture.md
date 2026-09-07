# CPDE 2.0 Scientific Architecture & Remote Sensing Specifications

## 1. System Overview
**Platform Title**: Near-Real-Time Satellite-Based Crop Monitoring and Decision Support Platform  
**Target Validation Site**: Bapatla District, Andhra Pradesh, India  
**Core Pipeline**:  
$$\text{Satellite Acquisition} \longrightarrow \text{Quality Assurance (QA)} \longrightarrow \text{Projected Resampling (UTM 44N)} \longrightarrow \text{Phenological Modeling} \longrightarrow \text{Calibrated Decision Support} \longrightarrow \text{Farmer Verification}$$

---

## 2. Sensor Suite & Spatial Support Model

| Sensor / Constellation | Physical Measurement | Native Sensor Resolution | Product Grid | Analysis Grid | Resampling Policy | Nature / Role |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Sentinel-2A/B/C (L2A)** | Visible & NIR (B02, B03, B04, B08) | $10\,\text{m}$ | $10\,\text{m}$ | $10\,\text{m}$ | Direct Alignment | Photosynthetic biomass & canopy greenness |
| **Sentinel-2A/B/C (L2A)** | Red Edge & SWIR (B05, B8A, B11) | $20\,\text{m}$ | $20\,\text{m}$ | $10\,\text{m}$ | Bilinear | Canopy moisture (NDMI) & red-edge chlorophyll (NDRE) |
| **Sentinel-2A/B/C (L2A)** | Scene Classification (SCL) | $20\,\text{m}$ | $20\,\text{m}$ | $10\,\text{m}$ | **Nearest Neighbor Strictly** | Cloud, shadow, and water masking |
| **Landsat 8/9 (Collection 2 L2)** | Thermal Infrared (TIRS Band 10) | $100\,\text{m}$ | $30\,\text{m}$ | $10\,\text{m}$ | Bilinear | Field-scale Land Surface Temperature (LST) |
| **Sentinel-1A/C (GRD)** | C-Band SAR ($\gamma^0_{VV}, \gamma^0_{VH}$) | $10\,\text{m}$ | $10\,\text{m}$ | $10\,\text{m}$ | Projected VRT | Moisture- and canopy structure-sensitive predictor |
| **MODIS (MOD11A1)** | Thermal LST (Daytime) | $1\,\text{km}$ | $1\,\text{km}$ | $10\,\text{m}$ | Bilinear | Regional contextual thermal continuity |
| **NASA POWER / ERA5** | $T_{max}, T_{min}, RH, R_s, P, VPD$ | $\sim 50\,\text{km}$ ($0.5^\circ \times 0.625^\circ$) | $50\,\text{km}$ | $10\,\text{m}$ | Spatial Support Assignment | Regional meteorological forcing & FAO-56 $ET_0$ |

---

## 3. The 6 Inviolable Scientific Governance Rules

1. **Landsat Thermal Resolution Provenance**: Recorded as a **30-m product grid**, with the **100-m native TIRS thermal sensor resolution** explicitly documented in all metadata schemas.
2. **Deterministic Resampling Policy**: Production pipeline strictly uses `Bilinear` for continuous spectral & thermal reflectance, `Nearest Neighbor` strictly for categorical masks (SCL/QA), and exact projected transformation to `UTM Zone 44N (EPSG:32644)`.
3. **Crop-Specific Sourced GDD Parameters**: Thermal thresholds ($T_{base}, T_{opt}, T_{cutoff}$) are explicitly sourced from agronomic literature (IRRI, ICAR-CICR, ICAR-IIHR, IIMR, ICRISAT, ANGRAU Andhra Pradesh standards) for Paddy, Cotton, Chilli, Maize, and Groundnut.
4. **SAR Feature Semantics**: Sentinel-1 backscatter ($\gamma^0_{VV}, \gamma^0_{VH}, VH/VV$) are defined strictly as **moisture- and structure-sensitive predictors**, not direct soil-moisture measurements without in-situ sensor calibration.
5. **Tri-Concept Separation**: System enforces three distinct orthogonal metrics:
   - **Stress Probability ($P \in [0.0, 1.0]$)**: Calibrated statistical likelihood from trained ML models.
   - **Model Confidence ($C$)**: Epistemic certainty / prediction interval width.
   - **Evidence Sufficiency ($Q$)**: Physical observation integrity (valid pixel coverage %, cloud contamination %, sensor staleness).
6. **Empirical Generation of Evaluative Claims**: Claims such as *"potential early stress signal"*, *"water stress pattern"*, or lead times are generated solely from empirical validation statistics, never hardcoded assertions.

---

## 4. Mathematical Formulations & Indices

### A. Normalized Difference Vegetation Index (NDVI)
$$\text{NDVI} = \frac{\text{B08} - \text{B04}}{\text{B08} + \text{B04}} \quad (\text{Native } 10\,\text{m})$$

### B. Normalized Difference Moisture Index (NDMI)
$$\text{NDMI} = \frac{\text{B8A} - \text{B11}}{\text{B8A} + \text{B11}} \quad (20\,\text{m source resampled to } 10\,\text{m analysis grid})$$

### C. Normalized Difference Red Edge Index (NDRE)
$$\text{NDRE} = \frac{\text{B8A} - \text{B05}}{\text{B8A} + \text{B05}} \quad (20\,\text{m source resampled to } 10\,\text{m analysis grid})$$

### D. Green Chlorophyll Index Proxy (GCI)
$$\text{GCI} = \frac{\text{B8A}}{\text{B05}} - 1.0 \quad (20\,\text{m source resampled to } 10\,\text{m analysis grid})$$

### E. Landsat 8/9 Level-2 Surface Temperature (LST)
$$\text{LST (Kelvin)} = \text{DN} \times 0.00341802 + 149.0, \quad \text{LST (}^\circ\text{C)} = \text{LST (Kelvin)} - 273.15$$

### F. Growing Degree Days ($GDD$) & Thermal Time
$$GDD_{daily} = \max(0, \min(T_{mean}, T_{max}) - T_{base}), \quad GDD_{cum} = \sum_{t=1}^{N} GDD_{daily}(t)$$

### G. Dual Baseline Anomaly Formulation
$$\text{Temporal Index Delta} = \text{Observed}(t_0) - \mu(t_{-5} \dots t_{-1})$$
$$\text{Phenology Anomaly (\%)} = \frac{\text{Observed} - \text{Expected}(Crop, GDD, Stage)}{\text{Expected}(Crop, GDD, Stage)} \times 100$$
