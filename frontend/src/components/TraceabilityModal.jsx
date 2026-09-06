import React from 'react'

const SCIENTIFIC_SPECS = {
  ndvi: {
    name: 'Normalized Difference Vegetation Index',
    acronym: 'NDVI',
    formula: '(B08 - B04) / (B08 + B04)',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm' },
    ],
    citation: 'Rouse, J. W., Haas, R. H., Schell, J. A., & Deering, D. W. (1974). Monitoring vegetation systems in the Great Plains with ERTS. NASA SP-351, 309-317.',
    explanation: 'Quantifies active photosynthetic biomass. Chlorophyll absorbs red light, while leaf mesophyll structure strongly reflects near-infrared.',
  },
  ndmi: {
    name: 'Normalized Difference Moisture Index',
    acronym: 'NDMI',
    formula: '(B08 - B11) / (B08 + B11)',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B11', name: 'Shortwave-Infrared (SWIR)', wavelength: '1610 nm', bandwidth: '90 nm' },
    ],
    citation: 'Gao, B. C. (1996). NDWI—A normalized difference water index for remote sensing of vegetation liquid water from space. Remote Sensing of Environment, 58(3), 257-266.',
    explanation: 'Measures equivalent water thickness in the canopy. Liquid water in spongy leaf mesophyll absorbs SWIR radiation.',
  },
  ndre: {
    name: 'Normalized Difference Red Edge Index',
    acronym: 'NDRE',
    formula: '(B08 - B05) / (B08 + B05)',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B05', name: 'Red Edge 1', wavelength: '705 nm', bandwidth: '15 nm' },
    ],
    citation: 'Barnes, E. M., Clarke, T. R., & Richards, P. J. (2000). Coincident detection of crop water and nitrogen stress in corn. Proc. 5th Intl Conf. Precision Agriculture.',
    explanation: 'Crucial for early pre-cause detection. Red edge wavelengths penetrate deeper into dense canopies and detect chlorophyll/nitrogen depletion days before visible yellowing.',
  },
  evi: {
    name: 'Enhanced Vegetation Index',
    acronym: 'EVI',
    formula: '2.5 * ((B08 - B04) / (B08 + 6.0*B04 - 7.5*B02 + 1.0))',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm' },
      { band: 'B02', name: 'Blue', wavelength: '490 nm', bandwidth: '65 nm' },
    ],
    citation: 'Huete, A., et al. (2002). Overview of the radiometric and biophysical performance of the MODIS vegetation indices. Remote Sensing of Environment, 83(1-2), 195-213.',
    explanation: 'Corrects for canopy background noise and residual atmospheric aerosols using the blue band. Resists saturation in high-biomass crops.',
  },
  savi: {
    name: 'Soil-Adjusted Vegetation Index',
    acronym: 'SAVI',
    formula: '((B08 - B04) / (B08 + B04 + 0.5)) * 1.5',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B04', name: 'Red', wavelength: '665 nm', bandwidth: '30 nm' },
    ],
    citation: 'Huete, A. R. (1988). A soil-adjusted vegetation index (SAVI). Remote Sensing of Environment, 25(3), 295-309.',
    explanation: 'Incorporates a soil calibration factor (L=0.5) to neutralize soil optical reflectance during early seedling emergence and sparse ground cover.',
  },
  gci: {
    name: 'Green Chlorophyll Index',
    acronym: 'GCI',
    formula: '(B08 / B03) - 1.0',
    bands: [
      { band: 'B08', name: 'Near-Infrared (NIR)', wavelength: '842 nm', bandwidth: '115 nm' },
      { band: 'B03', name: 'Green', wavelength: '560 nm', bandwidth: '35 nm' },
    ],
    citation: 'Gitelson, A. A., et al. (2005). Remote estimation of canopy chlorophyll content in crops. Geophysical Research Letters, 32(8).',
    explanation: 'Direct proxy for total canopy chlorophyll mass and nitrogen accumulation throughout vegetative growth.',
  },
  vpd: {
    name: 'Vapor Pressure Deficit',
    acronym: 'VPD',
    formula: 'e_sat(T) - e_act(T, RH) [FAO-56 formulation]',
    bands: [],
    citation: 'Allen, R. G., et al. (1998). Crop evapotranspiration-Guidelines for computing crop water requirements-FAO Irrigation and drainage paper 56.',
    explanation: 'Quantifies atmospheric evaporative pull. VPD > 2.0 kPa triggers stomatal closure to prevent water cavitation, causing canopy thermal heating.',
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
            <div className="text-xs font-bold uppercase tracking-wider text-slate-500">Mathematical Definition</div>
            <div className="mt-1 rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm font-semibold text-slate-800">
              {spec.formula}
            </div>
          </div>

          <div>
            <div className="text-xs font-bold uppercase tracking-wider text-slate-500">Physical & Biological Rationale</div>
            <p className="mt-1 text-slate-600 leading-relaxed">{spec.explanation}</p>
          </div>

          {spec.bands.length > 0 && (
            <div>
              <div className="text-xs font-bold uppercase tracking-wider text-slate-500">Sensor Band Specifications (Sentinel-2 MSI)</div>
              <div className="mt-1 overflow-hidden rounded-md border border-slate-200">
                <table className="w-full text-left text-xs">
                  <thead className="bg-slate-100 text-slate-700">
                    <tr>
                      <th className="p-2">Band ID</th>
                      <th className="p-2">Spectral Region</th>
                      <th className="p-2">Central Wavelength</th>
                      <th className="p-2">Bandwidth</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {spec.bands.map((b) => (
                      <tr key={b.band}>
                        <td className="p-2 font-mono font-bold text-emerald-700">{b.band}</td>
                        <td className="p-2">{b.name}</td>
                        <td className="p-2 font-mono">{b.wavelength}</td>
                        <td className="p-2 font-mono text-slate-500">{b.bandwidth}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div>
            <div className="text-xs font-bold uppercase tracking-wider text-slate-500">Academic Citation</div>
            <blockquote className="mt-1 border-l-2 border-emerald-500 pl-3 italic text-slate-600 text-xs">
              {spec.citation}
            </blockquote>
          </div>

          <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-600">
            <div className="font-semibold text-slate-800">Reproducibility Guarantee</div>
            <p className="mt-0.5">
              All pixel calculations are performed on Top-Of-Canopy (TOC) Bottom-Of-Atmosphere (BOA) Surface Reflectance (L2A) in metric UTM coordinates (EPSG:32644). No empirical fudge factors are applied.
            </p>
          </div>
        </div>

        <div className="mt-6 flex justify-end">
          <button
            onClick={onClose}
            className="rounded-md bg-slate-800 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-900"
          >
            Close Specification
          </button>
        </div>
      </div>
    </div>
  )
}
