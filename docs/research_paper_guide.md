# Research Paper Guide: Near-Real-Time Satellite Precision Agriculture

## 1. Title & Abstract Guidance
- **Title**: *Near-Real-Time Multi-Sensor Satellite Crop Monitoring and Decision Support for Smallholder Agriculture in Andhra Pradesh*
- **Positioning**: AI + Multi-Sensor (Sentinel-2, Sentinel-1, Landsat 8/9, NASA POWER) decision support platform evaluated on field-scale parcels in Bapatla District, Andhra Pradesh.
- **Workflow**: `Satellite Acquisition → Quality Assurance (QA) → Projected 10m Resampling Grid (UTM 44N) → Phenological Modeling → Calibrated Decision Support → Farmer Field Verification`.

---

## 2. Explicit Academic Limitations Section (Mandatory for Peer Review)

The research paper must explicitly articulate these scientific boundaries:

1. **Optical Cloud Contamination Gaps**:
   - Sentinel-2 optical multispectral observations are susceptible to heavy monsoon cloud cover and partial cloud shadows.
   - *Mitigation*: Sentinel-1 C-Band SAR backscatter ($\gamma^0_{VV}, \gamma^0_{VH}$) is integrated as an all-weather structural and moisture proxy during optical data gaps.
2. **Sensor Spatial Resolution Disparities**:
   - Sentinel-2 optical visible/NIR bands have a $10\,\text{m}$ native resolution, whereas Red-Edge ($B05$) and SWIR ($B11$) have a $20\,\text{m}$ native resolution.
   - NDMI and NDRE are therefore calculated on a $20\,\text{m}$ source basis and resampled onto the $10\,\text{m}$ analysis grid using documented Bilinear interpolation.
   - Landsat 8/9 Surface Temperature operates on a $30\,\text{m}$ product grid derived from a $100\,\text{m}$ native TIRS sensor.
3. **Remote Sensing Does Not Directly Measure Physiological Plant Stress**:
   - Satellite spectral and thermal indices reflect radiometric canopy signatures (chlorophyll absorption, water thickness, transpirational cooling).
   - In-situ ground observations (soil moisture probes, leaf chlorophyll SPAD readings, disease scouting) remain necessary for definitive physiological confirmation.
4. **Decision Support vs. Autonomous Prescriptions**:
   - Recommendations (e.g. FAO-56 atmospheric water demand) are provided strictly as decision support tools to guide farmer scouting and verification, rather than autonomous irrigation or nutrient commands.
5. **Geographical & Phenological Generalization**:
   - The platform is calibrated and validated for agricultural fields in **Bapatla District, Andhra Pradesh** across Paddy, Cotton, Chilli, Maize, and Groundnut crops. Expansion across wider agro-ecological zones requires regional baseline recalibration.
