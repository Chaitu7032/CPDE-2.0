import React, { useMemo } from 'react'

function InfoBadge({ label, description }) {
  return (
    <span
      className="ml-1 inline-flex h-4 w-4 items-center justify-center rounded-full border border-slate-300 bg-slate-50 text-[10px] font-bold text-slate-700"
      title={`${label}: ${description}`}
      aria-label={`${label}: ${description}`}
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

  let lonSum = 0
  let latSum = 0
  let count = 0

  for (const point of ring) {
    if (!Array.isArray(point) || point.length < 2) continue
    const lon = toFiniteNumber(point[0])
    const lat = toFiniteNumber(point[1])
    if (lon === null || lat === null) continue
    lonSum += lon
    latSum += lat
    count += 1
  }

  if (count === 0) return { lon: null, lat: null }
  return { lon: lonSum / count, lat: latSum / count }
}

function classifyNdvi(value) {
  if (!Number.isFinite(value)) {
    return { label: 'Unavailable', colorClass: 'text-slate-600', badgeClass: 'bg-slate-100 text-slate-700', emoji: '⚪' }
  }
  if (value < 0.2) {
    return { label: 'Sparse Vegetation', colorClass: 'text-orange-700', badgeClass: 'bg-orange-100 text-orange-700', emoji: '🟠' }
  }
  if (value < 0.4) {
    return { label: 'Moderate Vegetation', colorClass: 'text-yellow-700', badgeClass: 'bg-yellow-100 text-yellow-700', emoji: '🟡' }
  }
  if (value < 0.6) {
    return { label: 'Healthy Vegetation', colorClass: 'text-green-700', badgeClass: 'bg-green-100 text-green-700', emoji: '🟢' }
  }
  return { label: 'Dense Vegetation', colorClass: 'text-emerald-700', badgeClass: 'bg-emerald-100 text-emerald-700', emoji: '🟢' }
}

function formatCoordinate(value) {
  return Number.isFinite(value) ? value.toFixed(8) : 'N/A'
}

function formatMetric(value, digits = 3) {
  return Number.isFinite(value) ? value.toFixed(digits) : 'N/A'
}

export default function GridInspector({ selectedGrid, gridOptions, onSelectGrid, latestDate }) {
  const properties = selectedGrid?.properties || {}
  const geometry = selectedGrid?.geometry || null

  const centroid = useMemo(() => getCentroid(geometry), [geometry])

  const anomalyBag = properties?.anomalies || {}
  const rawB04FromPayload = toFiniteNumber(
    properties?.b04 ?? properties?.B04 ?? anomalyBag?.B04?.value ?? anomalyBag?.b04?.value
  )
  const rawB08FromPayload = toFiniteNumber(
    properties?.b08 ?? properties?.B08 ?? anomalyBag?.B08?.value ?? anomalyBag?.b08?.value
  )

  const fallbackBands = { b04: 0.153, b08: 0.200 }
  const usesFallbackBands = rawB04FromPayload === null || rawB08FromPayload === null

  const b04 = rawB04FromPayload ?? fallbackBands.b04
  const b08 = rawB08FromPayload ?? fallbackBands.b08

  const denominator = b08 + b04
  const computedNdvi = denominator !== 0 ? (b08 - b04) / denominator : null
  const payloadNdvi = toFiniteNumber(properties?.ndvi)
  const finalNdvi = payloadNdvi ?? computedNdvi

  const classification = classifyNdvi(finalNdvi)

  return (
    <section className="space-y-4">
      <div className="rounded-lg border bg-white p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">Grid Inspector</h2>
            <p className="text-sm text-slate-600">Scientific drill-down for a single analysis grid with equation traceability.</p>
          </div>
          <div className="min-w-[220px]">
            <label className="block text-xs font-semibold text-slate-600">Select Grid</label>
            <select
              className="mt-1 w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm"
              value={properties?.grid_id ?? ''}
              onChange={(event) => onSelectGrid(Number(event.target.value))}
            >
              {gridOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <div className="rounded-lg border bg-white p-4">
          <div className="mb-3 text-sm font-semibold text-slate-700">Grid Identity</div>
          <div className="space-y-2 text-sm text-slate-700">
            <div>
              <span className="font-medium">Grid ID:</span> {properties?.grid_id ?? 'N/A'}
            </div>
            <div>
              <span className="font-medium">Centroid (WGS84):</span> ({formatCoordinate(centroid.lon)}, {formatCoordinate(centroid.lat)})
            </div>
            <div>
              <span className="font-medium">Latest acquisition:</span> {latestDate || 'N/A'}
            </div>
          </div>
        </div>

        <div className="rounded-lg border bg-white p-4">
          <div className="mb-3 text-sm font-semibold text-slate-700">Raw Sentinel-2 Values</div>
          <div className="space-y-2 text-sm text-slate-700">
            <div>
              <span className="font-medium">B04 (Red)</span>
              <InfoBadge label="B04" description="Sentinel-2 red band centered at 665 nm." />
              : {formatMetric(b04)}
            </div>
            <div>
              <span className="font-medium">B08 (NIR)</span>
              <InfoBadge label="B08" description="Sentinel-2 near-infrared band centered at 842 nm." />
              : {formatMetric(b08)}
            </div>
            {usesFallbackBands && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-800">
                Raw band values were not present in the current payload. Demo fallback values are displayed to keep the formula trace visible.
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="mb-2 text-sm font-semibold text-slate-700">NDVI Calculation (Step-by-Step)</div>
        <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm text-slate-700">
          <div>NDVI = (NIR - RED) / (NIR + RED)</div>
          <div>NDVI = ({formatMetric(b08)} - {formatMetric(b04)}) / ({formatMetric(b08)} + {formatMetric(b04)})</div>
          <div>NDVI = {formatMetric(b08 - b04)} / {formatMetric(b08 + b04)}</div>
          <div>NDVI = {formatMetric(computedNdvi)}</div>
          {payloadNdvi !== null && (
            <div className="font-sans text-xs text-slate-600">
              Final value from system payload: {formatMetric(payloadNdvi)} (shown for reproducibility check)
            </div>
          )}
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3 text-sm">
          <span className="rounded-md bg-slate-100 px-2 py-1 font-semibold text-slate-700">
            NDVI: {formatMetric(finalNdvi)}
          </span>
          <span className={`rounded-md px-2 py-1 font-semibold ${classification.badgeClass}`}>
            {classification.label} {classification.emoji}
          </span>
        </div>

        <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          Confidence indicator: Based on Sentinel-2 10m resolution data.
        </div>
      </div>
    </section>
  )
}
