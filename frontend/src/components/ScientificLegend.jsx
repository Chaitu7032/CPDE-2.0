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
    { range: '0.15 - 0.28', label: 'Potential Canopy Stress', color: '#c2410c' },
    { range: '0.28 - 0.42', label: 'Moderate Vigor', color: '#d97706' },
    { range: '0.42 - 0.55', label: 'Healthy Red Edge', color: '#16a34a' },
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
    { range: '1.0 - 2.5', label: 'Low Chlorophyll Status', color: '#f97316' },
    { range: '2.5 - 4.5', label: 'Moderate Status', color: '#ca8a04' },
    { range: '4.5 - 6.5', label: 'Strong Status', color: '#16a34a' },
    { range: '>= 6.5', label: 'Optimum Peak', color: '#15803d' },
  ],
  lst: [
    { range: '< 25°C', label: 'Cool / Transpiring', color: '#2563eb' },
    { range: '25 - 30°C', label: 'Normal Thermal Range', color: '#16a34a' },
    { range: '30 - 35°C', label: 'Elevated Heat', color: '#f59e0b' },
    { range: '>= 35°C', label: 'Hot Thermal Stress', color: '#dc2626' },
  ],
  sar: [
    { range: '< -18 dB', label: 'Low Backscatter / Dry / Flooded', color: '#dc2626' },
    { range: '-18 to -12 dB', label: 'Moderate Backscatter', color: '#f59e0b' },
    { range: '>= -12 dB', label: 'High Canopy Scattering', color: '#16a34a' },
  ],
  stress_prob: [
    { range: '< 0.30', label: 'Low Stress (Nominal)', color: '#16a34a' },
    { range: '0.30 - 0.60', label: 'Watch / Moderate', color: '#eab308' },
    { range: '0.60 - 0.80', label: 'Elevated Stress', color: '#f97316' },
    { range: '>= 0.80', label: 'High Stress Anomaly', color: '#dc2626' },
  ],
  uncertainty: [
    { range: 'High QA (>80%)', label: 'High Quality Observation', color: '#16a34a' },
    { range: 'Med QA (50-80%)', label: 'Moderate Quality', color: '#f59e0b' },
    { range: 'Low QA (<50%)', label: 'High Uncertainty / Gaps', color: '#7f1d1d' },
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
  if (indexKey === 'sar') {
    if (value < -18.0) return scale[0].color
    if (value < -12.0) return scale[1].color
    return scale[2].color
  }
  if (indexKey === 'stress_prob') {
    if (value < 0.30) return scale[0].color
    if (value < 0.60) return scale[1].color
    if (value < 0.80) return scale[2].color
    return scale[3].color
  }
  if (indexKey === 'uncertainty') {
    if (value >= 80.0) return scale[0].color
    if (value >= 50.0) return scale[1].color
    return scale[2].color
  }
  return '#808080'
}

export default function ScientificLegend({ activeIndex = 'ndvi', onOpenTraceability }) {
  const scale = CPDE_SCALES[activeIndex.toLowerCase()] || CPDE_SCALES.ndvi

  return (
    <div className="rounded-xl border border-slate-200 bg-white/95 p-3 shadow-md backdrop-blur-xs">
      <div className="flex items-center justify-between border-b pb-2">
        <span className="text-xs font-bold uppercase tracking-wider text-slate-700">
          Scale: {activeIndex.toUpperCase()}
        </span>
        {onOpenTraceability && (
          <button
            onClick={() => onOpenTraceability(activeIndex)}
            className="flex items-center gap-1 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-semibold text-slate-700 hover:bg-slate-200"
            title="Inspect Peer-Reviewed Formulation"
          >
            Traceability ↗
          </button>
        )}
      </div>

      <div className="mt-2 space-y-1">
        {scale.map((item, idx) => (
          <div key={idx} className="flex items-center justify-between gap-3 text-[11px]">
            <div className="flex items-center gap-1.5">
              <span
                className="h-2.5 w-2.5 rounded-xs border border-black/10"
                style={{ backgroundColor: item.color }}
              />
              <span className="font-mono text-slate-600">{item.range}</span>
            </div>
            <span className="text-right font-medium text-slate-700">{item.label}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
