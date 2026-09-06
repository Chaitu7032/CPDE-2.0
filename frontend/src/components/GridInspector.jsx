import React, { useMemo, useState } from 'react'
import TraceabilityModal from './TraceabilityModal'

function InfoBadge({ label, description }) {
  return (
    <span
      className="ml-1 inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-slate-300 bg-slate-100 text-[10px] font-bold text-slate-700"
      title={`${label}: ${description}`}
    >
      i
    </span>
  )
}

function toFiniteNumber(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function getCentroid(geometry) {
  if (!geometry || geometry.type !== 'Polygon') {
    return { lon: null, lat: null }
  }
  const ring = geometry.coordinates?.[0]
  if (!Array.isArray(ring) || ring.length === 0) {
    return { lon: null, lat: null }
  }
  let lonSum = 0, latSum = 0, count = 0
  for (const point of ring) {
    if (!Array.isArray(point) || point.length < 2) continue
    const lon = toFiniteNumber(point[0])
    const lat = toFiniteNumber(point[1])
    if (lon !== null && lat !== null) {
      lonSum += lon
      latSum += lat
      count += 1
    }
  }
  return count === 0 ? { lon: null, lat: null } : { lon: lonSum / count, lat: latSum / count }
}

function classifyIndex(key, value) {
  if (value === null || !Number.isFinite(value)) {
    return { label: 'No Data', color: '#94a3b8', bg: 'bg-slate-100 text-slate-600' }
  }
  if (key === 'ndvi' || key === 'savi') {
    if (value < 0.20) return { label: 'Severe Stress', color: '#dc2626', bg: 'bg-red-100 text-red-700' }
    if (value < 0.40) return { label: 'Stressed', color: '#f97316', bg: 'bg-orange-100 text-orange-700' }
    if (value < 0.60) return { label: 'Moderate', color: '#eab308', bg: 'bg-yellow-100 text-yellow-800' }
    if (value < 0.75) return { label: 'Healthy', color: '#22c55e', bg: 'bg-green-100 text-green-700' }
    return { label: 'Excellent', color: '#15803d', bg: 'bg-emerald-100 text-emerald-800' }
  }
  if (key === 'ndmi') {
    if (value < -0.15) return { label: 'Severe Dehydration', color: '#b91c1c', bg: 'bg-red-100 text-red-700' }
    if (value < 0.00) return { label: 'Water Stressed', color: '#ea580c', bg: 'bg-orange-100 text-orange-700' }
    if (value < 0.20) return { label: 'Moderate Moisture', color: '#0284c7', bg: 'bg-sky-100 text-sky-700' }
    if (value < 0.40) return { label: 'Adequate Hydration', color: '#2563eb', bg: 'bg-blue-100 text-blue-700' }
    return { label: 'Optimal Moisture', color: '#1d4ed8', bg: 'bg-indigo-100 text-indigo-700' }
  }
  if (key === 'ndre') {
    if (value < 0.15) return { label: 'Acute Chlorophyll Deficit', color: '#991b1b', bg: 'bg-red-100 text-red-700' }
    if (value < 0.28) return { label: 'Early Nutrient Stress', color: '#c2410c', bg: 'bg-orange-100 text-orange-700' }
    if (value < 0.42) return { label: 'Moderate Status', color: '#d97706', bg: 'bg-amber-100 text-amber-800' }
    if (value < 0.55) return { label: 'Healthy Nitrogen', color: '#16a34a', bg: 'bg-green-100 text-green-700' }
    return { label: 'Peak Chlorophyll', color: '#166534', bg: 'bg-emerald-100 text-emerald-800' }
  }
  if (key === 'evi') {
    if (value < 0.15) return { label: 'Low Biomass', color: '#dc2626', bg: 'bg-red-100 text-red-700' }
    if (value < 0.30) return { label: 'Sparse Canopy', color: '#f97316', bg: 'bg-orange-100 text-orange-700' }
    if (value < 0.45) return { label: 'Moderate Canopy', color: '#eab308', bg: 'bg-yellow-100 text-yellow-800' }
    if (value < 0.65) return { label: 'Dense Foliage', color: '#22c55e', bg: 'bg-green-100 text-green-700' }
    return { label: 'Lush Vigor', color: '#15803d', bg: 'bg-emerald-100 text-emerald-800' }
  }
  if (key === 'gci') {
    if (value < 1.0) return { label: 'Severe Chlorosis', color: '#dc2626', bg: 'bg-red-100 text-red-700' }
    if (value < 2.5) return { label: 'Low Chlorophyll', color: '#f97316', bg: 'bg-orange-100 text-orange-700' }
    if (value < 4.5) return { label: 'Moderate Level', color: '#ca8a04', bg: 'bg-amber-100 text-amber-800' }
    if (value < 6.5) return { label: 'Strong Chlorophyll', color: '#16a34a', bg: 'bg-green-100 text-green-700' }
    return { label: 'Optimum Peak', color: '#15803d', bg: 'bg-emerald-100 text-emerald-800' }
  }
  return { label: 'Normal', color: '#22c55e', bg: 'bg-green-100 text-green-700' }
}

export default function GridInspector({ selectedGrid, gridOptions, onSelectGrid, latestDate }) {
  const [traceModalKey, setTraceModalKey] = useState(null)
  const properties = selectedGrid?.properties || {}
  const geometry = selectedGrid?.geometry || null
  const centroid = useMemo(() => getCentroid(geometry), [geometry])

  // Extract bands
  const b02 = toFiniteNumber(properties?.bands?.b02 ?? properties?.b02)
  const b03 = toFiniteNumber(properties?.bands?.b03 ?? properties?.b03)
  const b04 = toFiniteNumber(properties?.bands?.b04 ?? properties?.b04)
  const b05 = toFiniteNumber(properties?.bands?.b05 ?? properties?.b05)
  const b08 = toFiniteNumber(properties?.bands?.b08 ?? properties?.b08)
  const b11 = toFiniteNumber(properties?.bands?.b11 ?? properties?.b11)

  // Compute or extract 6 indices
  const ndvi = toFiniteNumber(properties?.indices?.ndvi ?? properties?.ndvi ?? (b08 && b04 ? (b08 - b04) / (b08 + b04) : null))
  const ndmi = toFiniteNumber(properties?.indices?.ndmi ?? properties?.ndmi ?? (b08 && b11 ? (b08 - b11) / (b08 + b11) : null))
  const ndre = toFiniteNumber(properties?.indices?.ndre ?? properties?.ndre ?? (b08 && b05 ? (b08 - b05) / (b08 + b05) : null))
  const evi = toFiniteNumber(properties?.indices?.evi ?? properties?.evi ?? (b08 && b04 && b02 ? 2.5 * ((b08 - b04) / (b08 + 6*b04 - 7.5*b02 + 1)) : null))
  const savi = toFiniteNumber(properties?.indices?.savi ?? properties?.savi ?? (b08 && b04 ? ((b08 - b04) / (b08 + b04 + 0.5)) * 1.5 : null))
  const gci = toFiniteNumber(properties?.indices?.gci ?? properties?.gci ?? (b08 && b03 ? (b08 / b03) - 1 : null))

  const indicesList = [
    { key: 'ndvi', name: 'NDVI', label: 'Vegetation Index', value: ndvi, citationKey: 'ndvi' },
    { key: 'ndmi', name: 'NDMI', label: 'Canopy Moisture', value: ndmi, citationKey: 'ndmi' },
    { key: 'ndre', name: 'NDRE', label: 'Red Edge / Nitrogen (Pre-Cause)', value: ndre, citationKey: 'ndre' },
    { key: 'evi', name: 'EVI', label: 'Enhanced Vegetation', value: evi, citationKey: 'evi' },
    { key: 'savi', name: 'SAVI', label: 'Soil-Adjusted Index', value: savi, citationKey: 'savi' },
    { key: 'gci', name: 'GCI', label: 'Green Chlorophyll', value: gci, citationKey: 'gci' },
  ]

  return (
    <section className="space-y-4">
      {/* Header & Grid Selector */}
      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-xs">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <span className="rounded-full bg-emerald-100 px-2.5 py-0.5 text-xs font-bold text-emerald-800">
                10m x 10m Scientific Unit
              </span>
              <h2 className="text-base font-bold text-slate-800">Grid Intelligence Inspector</h2>
            </div>
            <p className="mt-0.5 text-xs text-slate-500">Multispectral band analysis and early biophysical stress detection.</p>
          </div>
          {gridOptions.length > 0 && (
            <div className="min-w-[200px]">
              <select
                className="w-full rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold"
                value={properties?.grid_id ?? ''}
                onChange={(e) => onSelectGrid(e.target.value)}
              >
                {gridOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>

        {/* Identity row */}
        <div className="mt-3 grid grid-cols-2 gap-2 border-t pt-3 text-xs sm:grid-cols-4">
          <div>
            <span className="text-slate-500">Grid Identifier:</span>
            <div className="font-mono font-bold text-slate-800">{properties?.grid_id ?? 'Selected Cell'}</div>
          </div>
          <div>
            <span className="text-slate-500">Centroid Coordinates:</span>
            <div className="font-mono font-medium text-slate-800">
              {centroid.lon ? `${centroid.lon.toFixed(5)}, ${centroid.lat.toFixed(5)}` : 'N/A'}
            </div>
          </div>
          <div>
            <span className="text-slate-500">Observation Date:</span>
            <div className="font-medium text-slate-800">{latestDate || 'Latest Sentinel-2'}</div>
          </div>
          <div>
            <span className="text-slate-500">Spatial Resolution:</span>
            <div className="font-medium text-slate-800">100 m² (10m cell)</div>
          </div>
        </div>
      </div>

      {/* 6 Scientific Indices Grid */}
      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-xs">
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-xs font-bold uppercase tracking-wider text-slate-700">Scientific Vegetation & Moisture Indices</h3>
          <span className="text-[11px] text-slate-500">Click ⓘ for formula & academic lineage</span>
        </div>

        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {indicesList.map((item) => {
            const classification = classifyIndex(item.key, item.value)
            return (
              <div key={item.key} className="rounded-lg border border-slate-200 bg-slate-50/50 p-3 transition hover:border-slate-300">
                <div className="flex items-center justify-between">
                  <span className="font-bold text-slate-800">{item.name}</span>
                  <button
                    onClick={() => setTraceModalKey(item.citationKey)}
                    className="inline-flex h-4 w-4 cursor-pointer items-center justify-center rounded-full bg-slate-200 text-[10px] font-bold text-slate-700 hover:bg-emerald-200 hover:text-emerald-900"
                    title="Inspect formula & scientific citation"
                  >
                    i
                  </button>
                </div>
                <div className="text-[11px] text-slate-500">{item.label}</div>

                <div className="mt-2 flex items-baseline justify-between">
                  <div className="font-mono text-lg font-bold text-slate-900">
                    {item.value !== null ? item.value.toFixed(3) : '–'}
                  </div>
                  <span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${classification.bg}`}>
                    {classification.label}
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Raw Spectral Reflectance Bands */}
      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-xs">
        <div className="mb-2 flex items-center justify-between">
          <h3 className="text-xs font-bold uppercase tracking-wider text-slate-700">Sentinel-2 Surface Reflectance (L2A BOA)</h3>
          <span className="text-[11px] text-slate-500">Scaled Surface Reflectance [0.0 - 1.0]</span>
        </div>

        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6 text-center text-xs">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
            <div className="font-bold text-blue-600">B02 (Blue)</div>
            <div className="text-[10px] text-slate-400">490 nm</div>
            <div className="mt-1 font-mono font-semibold">{b02 !== null ? b02.toFixed(3) : '–'}</div>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
            <div className="font-bold text-green-600">B03 (Green)</div>
            <div className="text-[10px] text-slate-400">560 nm</div>
            <div className="mt-1 font-mono font-semibold">{b03 !== null ? b03.toFixed(3) : '–'}</div>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
            <div className="font-bold text-red-600">B04 (Red)</div>
            <div className="text-[10px] text-slate-400">665 nm</div>
            <div className="mt-1 font-mono font-semibold">{b04 !== null ? b04.toFixed(3) : '–'}</div>
          </div>
          <div className="rounded-md border border-amber-200 bg-amber-50/50 p-2">
            <div className="font-bold text-amber-700">B05 (RedEdge)</div>
            <div className="text-[10px] text-amber-600">705 nm</div>
            <div className="mt-1 font-mono font-semibold">{b05 !== null ? b05.toFixed(3) : '–'}</div>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
            <div className="font-bold text-purple-600">B08 (NIR)</div>
            <div className="text-[10px] text-slate-400">842 nm</div>
            <div className="mt-1 font-mono font-semibold">{b08 !== null ? b08.toFixed(3) : '–'}</div>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
            <div className="font-bold text-indigo-600">B11 (SWIR)</div>
            <div className="text-[10px] text-slate-400">1610 nm</div>
            <div className="mt-1 font-mono font-semibold">{b11 !== null ? b11.toFixed(3) : '–'}</div>
          </div>
        </div>
      </div>

      {/* Scientific Traceability Modal */}
      <TraceabilityModal
        indexKey={traceModalKey}
        isOpen={!!traceModalKey}
        onClose={() => setTraceModalKey(null)}
      />
    </section>
  )
}
