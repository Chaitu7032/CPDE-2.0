import React from 'react'

export const CPDE_SCALES = {
  ndvi: [
    { range: '< 0.20', label: 'Severe Stress / Bare', color: '#dc2626' },
    { range: '0.20 - 0.40', label: 'Stressed / Sparse', color: '#f97316' },
    { range: '0.40 - 0.60', label: 'Moderate Vigor', color: '#eab308' },
    { range: '0.60 - 0.75', label: 'Healthy Canopy', color: '#22c55e' },
    { range: '>= 0.75', label: 'Dense / Peak Biomass', color: '#15803d' },
  ],
  ndmi: [
    { range: '< -0.15', label: 'Severe Moisture Deficit', color: '#b91c1c' },
    { range: '-0.15 - 0.00', label: 'Water Stressed / Dry', color: '#ea580c' },
    { range: '0.00 - 0.20', label: 'Moderate Moisture', color: '#0284c7' },
    { range: '0.20 - 0.40', label: 'Adequate Hydration', color: '#2563eb' },
    { range: '>= 0.40', label: 'High Canopy Moisture', color: '#1d4ed8' },
  ],
  ndre: [
    { range: '< 0.15', label: 'Acute Chlorophyll Deficit', color: '#991b1b' },
    { range: '0.15 - 0.28', label: 'Early Nitrogen Stress', color: '#c2410c' },
    { range: '0.28 - 0.42', label: 'Moderate Vigor', color: '#d97706' },
    { range: '0.42 - 0.55', label: 'Healthy Nitrogen Status', color: '#16a34a' },
    { range: '>= 0.55', label: 'Optimal Chlorophyll', color: '#166534' },
  ],
  evi: [
    { range: '< 0.15', label: 'Low Biomass / Stress', color: '#dc2626' },
    { range: '0.15 - 0.30', label: 'Sub-optimal Canopy', color: '#f97316' },
    { range: '0.30 - 0.45', label: 'Moderate Canopy Density', color: '#eab308' },
    { range: '0.45 - 0.65', label: 'Dense Healthy Foliage', color: '#22c55e' },
    { range: '>= 0.65', label: 'Lush Vegetative Vigour', color: '#15803d' },
  ],
  savi: [
    { range: '< 0.15', label: 'Bare / Stressed Soil', color: '#dc2626' },
    { range: '0.15 - 0.25', label: 'Early Germination', color: '#ea580c' },
    { range: '0.25 - 0.40', label: 'Moderate Ground Cover', color: '#ca8a04' },
    { range: '0.40 - 0.55', label: 'Good Emergence', color: '#16a34a' },
    { range: '>= 0.55', label: 'Closed Canopy', color: '#15803d' },
  ],
  gci: [
    { range: '< 1.0', label: 'Severe Chlorosis', color: '#dc2626' },
    { range: '1.0 - 2.5', label: 'Low Chlorophyll', color: '#f97316' },
    { range: '2.5 - 4.5', label: 'Moderate Chlorophyll', color: '#ca8a04' },
    { range: '4.5 - 6.5', label: 'Strong Chlorophyll', color: '#16a34a' },
    { range: '>= 6.5', label: 'Optimum Peak', color: '#15803d' },
  ],
  lst: [
    { range: '< 25°C', label: 'Cool / Transpiring', color: '#2563eb' },
    { range: '25 - 30°C', label: 'Normal Thermal Range', color: '#16a34a' },
    { range: '30 - 35°C', label: 'Elevated Heat', color: '#f59e0b' },
    { range: '>= 35°C', label: 'Hot Thermal Stress', color: '#dc2626' },
  ],
}

export function getColorForValue(indexKey, value) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return '#808080'
  }
  const scale = CPDE_SCALES[indexKey]
  if (!scale) return '#808080'

  if (indexKey === 'ndvi' || indexKey === 'savi') {
    if (value < 0.20) return scale[0].color
    if (value < 0.40) return scale[1].color
    if (value < 0.60) return scale[2].color
    if (value < 0.75) return scale[3].color
    return scale[4].color
  }
  if (indexKey === 'ndmi') {
    if (value < -0.15) return scale[0].color
    if (value < 0.00) return scale[1].color
    if (value < 0.20) return scale[2].color
    if (value < 0.40) return scale[3].color
    return scale[4].color
  }
  if (indexKey === 'ndre') {
    if (value < 0.15) return scale[0].color
    if (value < 0.28) return scale[1].color
    if (value < 0.42) return scale[2].color
    if (value < 0.55) return scale[3].color
    return scale[4].color
  }
  if (indexKey === 'evi') {
    if (value < 0.15) return scale[0].color
    if (value < 0.30) return scale[1].color
    if (value < 0.45) return scale[2].color
    if (value < 0.65) return scale[3].color
    return scale[4].color
  }
  if (indexKey === 'gci') {
    if (value < 1.0) return scale[0].color
    if (value < 2.5) return scale[1].color
    if (value < 4.5) return scale[2].color
    if (value < 6.5) return scale[3].color
    return scale[4].color
  }
  if (indexKey === 'lst') {
    if (value < 25) return scale[0].color
    if (value < 30) return scale[1].color
    if (value < 35) return scale[2].color
    return scale[3].color
  }
  return '#808080'
}

function renderScaleCard(title, scale) {
  return (
    <div className="rounded-md border border-slate-200 bg-white p-3 shadow-sm">
      <div className="mb-2 text-xs font-bold uppercase tracking-wider text-slate-800">{title}</div>
      <div className="space-y-1.5 text-xs text-slate-700">
        {scale.map((step) => (
          <div key={`${title}-${step.range}`} className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className="inline-block h-3 w-3 rounded-sm shadow-sm" style={{ backgroundColor: step.color }} />
              <span className="font-mono">{step.range}</span>
            </div>
            <span className="text-[11px] font-medium text-slate-600">{step.label}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function ScientificLegend({ ndvi, ndmi, lst, risk, colorScales, activeMode = 'ndvi' }) {
  return (
    <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-sm font-bold text-slate-800">Scientific Classification Palettes</h3>
          <p className="text-xs text-slate-600">Calibrated biophysical thresholds based on peer-reviewed remote sensing literature.</p>
        </div>
      </div>

      <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {renderScaleCard('NDVI (Canopy Biomass)', CPDE_SCALES.ndvi)}
        {renderScaleCard('NDMI (Moisture Content)', CPDE_SCALES.ndmi)}
        {renderScaleCard('NDRE (Red Edge / Nitrogen)', CPDE_SCALES.ndre)}
        {renderScaleCard('EVI (Enhanced Vegetation)', CPDE_SCALES.evi)}
        {renderScaleCard('SAVI (Soil-Adjusted)', CPDE_SCALES.savi)}
        {renderScaleCard('GCI (Green Chlorophyll)', CPDE_SCALES.gci)}
      </div>
    </div>
  )
}
