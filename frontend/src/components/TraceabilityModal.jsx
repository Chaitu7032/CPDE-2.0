import React from 'react'

const SCIENTIFIC_SPECS = {
  ndvi: {
    name: 'Normalized Difference Vegetation Index',
    acronym: 'NDVI',
    formula: '(B08 - B04) / (B08 + B04)',
    resolution: '10 m Native Observation',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm', resolution: '10 m' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm', resolution: '10 m' },
    ],
    citation: 'Rouse, J. W., Haas, R. H., Schell, J. A., & Deering, D. W. (1974). Monitoring vegetation systems in the Great Plains with ERTS. NASA SP-351, 309-317.',
    explanation: 'Quantifies active photosynthetic biomass. Chlorophyll absorbs red light, while leaf mesophyll structure strongly reflects near-infrared.',
  },
  ndmi: {
    name: 'Normalized Difference Moisture Index',
    acronym: 'NDMI',
    formula: '(B8A - B11) / (B8A + B11)',
    resolution: '20 m source resolution, resampled to 10 m analysis grid (Bilinear)',
    bands: [
      { band: 'B8A', name: 'Narrow Near-Infrared (NNIR)', wavelength: '865 nm', bandwidth: '20 nm', resolution: '20 m' },
      { band: 'B11', name: 'Shortwave-Infrared (SWIR)', wavelength: '1610 nm', bandwidth: '90 nm', resolution: '20 m' },
    ],
    citation: 'Gao, B. C. (1996). NDWI—A normalized difference water index for remote sensing of vegetation liquid water from space. Remote Sensing of Environment, 58(3), 257-266.',
    explanation: 'Measures equivalent water thickness in the canopy. Liquid water in spongy leaf mesophyll absorbs SWIR radiation. Resampled to 10m reporting grid via Bilinear interpolation.',
  },
  ndre: {
    name: 'Normalized Difference Red Edge Index',
    acronym: 'NDRE',
    formula: '(B8A - B05) / (B8A + B05)',
    resolution: '20 m source resolution, resampled to 10 m analysis grid (Bilinear)',
    bands: [
      { band: 'B8A', name: 'Narrow Near-Infrared (NNIR)', wavelength: '865 nm', bandwidth: '20 nm', resolution: '20 m' },
      { band: 'B05', name: 'Red Edge 1', wavelength: '705 nm', bandwidth: '15 nm', resolution: '20 m' },
    ],
    citation: 'Barnes, E. M., Clarke, T. R., & Richards, P. J. (2000). Coincident detection of crop water and nitrogen stress in corn. Proc. 5th Intl Conf. Precision Agriculture.',
    explanation: 'Red edge wavelengths penetrate deeper into dense canopies. Variations in NDRE indicate potential chlorophyll status anomalies or early vegetation stress relative to field baselines.',
  },
  evi: {
    name: 'Enhanced Vegetation Index',
    acronym: 'EVI',
    formula: '2.5 * ((B08 - B04) / (B08 + 6.0*B04 - 7.5*B02 + 1.0))',
    resolution: '10 m Native Observation',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm', resolution: '10 m' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm', resolution: '10 m' },
      { band: 'B02', name: 'Blue', wavelength: '490 nm', bandwidth: '65 nm', resolution: '10 m' },
    ],
    citation: 'Huete, A., et al. (2002). Overview of the radiometric and biophysical performance of the MODIS vegetation indices. Remote Sensing of Environment, 83(1-2), 195-213.',
    explanation: 'Corrects for canopy background noise and residual atmospheric aerosols using the blue band. Resists saturation in high-biomass crops.',
  },
  savi: {
    name: 'Soil-Adjusted Vegetation Index',
    acronym: 'SAVI',
    formula: '((B08 - B04) / (B08 + B04 + 0.5)) * 1.5',
    resolution: '10 m Native Observation',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm', resolution: '10 m' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm', resolution: '10 m' },
    ],
    citation: 'Huete, A. R. (1988). A soil-adjusted vegetation index (SAVI). Remote Sensing of Environment, 25(3), 295-309.',
    explanation: 'Incorporates a soil calibration factor (L=0.5) to neutralize soil optical reflectance during early seedling emergence and sparse ground cover.',
  },
  gci: {
    name: 'Green Chlorophyll Index',
    acronym: 'GCI',
    formula: '(B8A / B05) - 1.0  or  (B08 / B03) - 1.0',
    resolution: '20 m source resolution, resampled to 10 m analysis grid',
    bands: [
      { band: 'B8A', name: 'Narrow Near-Infrared', wavelength: '865 nm', bandwidth: '20 nm', resolution: '20 m' },
      { band: 'B05', name: 'Red Edge 1', wavelength: '705 nm', bandwidth: '15 nm', resolution: '20 m' },
    ],
    citation: 'Gitelson, A. A., et al. (2005). Remote estimation of canopy chlorophyll content in crops. Geophysical Research Letters, 32(8).',
    explanation: 'Proxy sensitive to canopy chlorophyll status. Interpretation depends on crop phenology, canopy architecture, and calibration against field SPAD measurements.',
  },
  lst: {
    name: 'Land Surface Temperature (Landsat 8/9 Level-2)',
    acronym: 'LST',
    formula: 'DN * 0.00341802 + 149.0 - 273.15  [°C]',
    resolution: '30 m product grid (100 m native TIRS thermal sensor)',
    bands: [
      { band: 'B10', name: 'Thermal Infrared (TIRS)', wavelength: '10.89 µm', bandwidth: '0.59 µm', resolution: '100 m (resampled to 30m USGS product)' },
    ],
    citation: 'Cook, M., et al. (2014). Atmospheric compensation for a Landsat land surface temperature product. Remote Sensing, 6(11), 11444-11476.',
    explanation: 'Thermal infrared radiometric surface temperature. Canopy thermal elevation relative to air temperature indicates stomatal closure and transpiration reduction under water stress.',
  },
  sar: {
    name: 'Synthetic Aperture Radar (Sentinel-1 C-Band)',
    acronym: 'SAR',
    formula: '10 * log10(DN^2) - CalibrationOffset  [dB]',
    resolution: '10 m C-Band GRD',
    bands: [
      { band: 'VV', name: 'Vertical-Vertical Polarisation', frequency: '5.405 GHz', resolution: '10 m' },
      { band: 'VH', name: 'Vertical-Horizontal Cross Polarisation', frequency: '5.405 GHz', resolution: '10 m' },
    ],
    citation: 'Torres, R., et al. (2012). GMES Sentinel-1 mission. Remote Sensing of Environment, 120, 9-24.',
    explanation: 'Cloud-independent active microwave backscatter. Serves as a moisture- and canopy structure-sensitive predictor across all weather conditions.',
  },
  vpd: {
    name: 'Vapor Pressure Deficit',
    acronym: 'VPD',
    formula: 'e_sat(T) - e_act(T, RH) [FAO-56 formulation]',
    resolution: '~50 km Regional Meteorological Grid (NASA POWER / ERA5)',
    bands: [],
    citation: 'Allen, R. G., et al. (1998). Crop evapotranspiration-Guidelines for computing crop water requirements-FAO Irrigation and drainage paper 56.',
    explanation: 'Quantifies atmospheric evaporative pull. VPD > 2.0 kPa triggers stomatal closure to prevent xylem cavitation, causing transpirational reduction.',
  },
}

export default function TraceabilityModal({ indexKey, isOpen, onClose }) {
  if (!isOpen || !indexKey) return null
  const spec = SCIENTIFIC_SPECS[indexKey.toLowerCase()] || SCIENTIFIC_SPECS.ndvi

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4 backdrop-blur-xs animate-in fade-in">
      <div className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-xl border border-slate-200 bg-white p-6 shadow-2xl">
        <div className="flex items-start justify-between border-b pb-4">
          <div>
            <div className="flex items-center gap-2">
              <span className="rounded bg-emerald-100 px-2 py-0.5 font-mono text-xs font-bold text-emerald-800">
                {spec.acronym}
              </span>
              <h3 className="text-lg font-bold text-slate-800">{spec.name}</h3>
            </div>
            <p className="mt-1 text-xs text-slate-500">Scientific Lineage & Peer-Reviewed Specification</p>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
          >
            ✕
          </button>
        </div>

        <div className="mt-4 space-y-4 text-sm text-slate-700">
          <div>
            <span className="font-semibold text-slate-900">Mathematical Formulation:</span>
            <div className="mt-1 rounded-md bg-slate-900 p-3 font-mono text-xs text-emerald-400">
              {spec.formula}
            </div>
          </div>

          <div>
            <span className="font-semibold text-slate-900">Spatial Support & Resolution:</span>
            <div className="mt-1 rounded-md border border-slate-200 bg-slate-50 p-2 text-xs text-slate-800">
              {spec.resolution}
            </div>
          </div>

          {spec.bands?.length > 0 && (
            <div>
              <span className="font-semibold text-slate-900">Spectral Bands Used:</span>
              <div className="mt-1 overflow-x-auto rounded-md border border-slate-200">
                <table className="min-w-full divide-y divide-slate-200 text-xs">
                  <thead className="bg-slate-50 text-slate-700">
                    <tr>
                      <th className="px-3 py-2 text-left font-semibold">Band</th>
                      <th className="px-3 py-2 text-left font-semibold">Description</th>
                      <th className="px-3 py-2 text-left font-semibold">Wavelength / Freq</th>
                      <th className="px-3 py-2 text-left font-semibold">Native Resolution</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200 bg-white">
                    {spec.bands.map((b) => (
                      <tr key={b.band}>
                        <td className="px-3 py-2 font-mono font-bold text-slate-900">{b.band}</td>
                        <td className="px-3 py-2 text-slate-700">{b.name}</td>
                        <td className="px-3 py-2 font-mono text-slate-600">{b.wavelength || b.frequency}</td>
                        <td className="px-3 py-2 font-mono text-slate-600">{b.resolution}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div>
            <span className="font-semibold text-slate-900">Biophysical Interpretation:</span>
            <p className="mt-1 text-xs leading-relaxed text-slate-600">{spec.explanation}</p>
          </div>

          <div>
            <span className="font-semibold text-slate-900">Primary Literature Citation:</span>
            <div className="mt-1 rounded-md border border-slate-200 bg-slate-50 p-3 text-xs italic text-slate-600">
              {spec.citation}
            </div>
          </div>
        </div>

        <div className="mt-6 flex justify-end border-t pt-4">
          <button
            onClick={onClose}
            className="rounded-lg bg-slate-900 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-800"
          >
            Close Specification
          </button>
        </div>
      </div>
    </div>
  )
}
