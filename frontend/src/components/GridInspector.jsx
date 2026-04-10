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
    return { label: 'NDVI unavailable', badgeClass: 'bg-slate-100 text-slate-700' }
  }
  if (value < 0.2) {
    return { label: 'NDVI-based', badgeClass: 'bg-orange-100 text-orange-700' }
  }
  if (value < 0.4) {
    return { label: 'NDVI-based', badgeClass: 'bg-yellow-100 text-yellow-700' }
  }
  if (value < 0.6) {
    return { label: 'NDVI-based', badgeClass: 'bg-green-100 text-green-700' }
  }
  return { label: 'NDVI-based', badgeClass: 'bg-emerald-100 text-emerald-700' }
}

function classifyNdmi(value) {
  if (!Number.isFinite(value)) {
    return { label: 'NDMI unavailable', badgeClass: 'bg-slate-100 text-slate-700' }
  }
  if (value < -0.1) {
    return { label: 'Dry', badgeClass: 'bg-red-100 text-red-700' }
  }
  if (value < 0) {
    return { label: 'Slightly dry', badgeClass: 'bg-orange-100 text-orange-700' }
  }
  if (value < 0.2) {
    return { label: 'Moderate moisture', badgeClass: 'bg-sky-100 text-sky-700' }
  }
  return { label: 'Wet', badgeClass: 'bg-blue-100 text-blue-700' }
}

function classifyRisk(value) {
  if (!Number.isFinite(value)) {
    return { label: 'Risk unavailable', badgeClass: 'bg-slate-100 text-slate-700' }
  }
  if (value < 0.25) {
    return { label: 'Low risk', badgeClass: 'bg-emerald-100 text-emerald-700' }
  }
  if (value < 0.5) {
    return { label: 'Moderate risk', badgeClass: 'bg-yellow-100 text-yellow-700' }
  }
  if (value < 0.75) {
    return { label: 'Elevated risk', badgeClass: 'bg-orange-100 text-orange-700' }
  }
  return { label: 'High risk', badgeClass: 'bg-red-100 text-red-700' }
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
  const rawB11FromPayload = toFiniteNumber(properties?.b11 ?? properties?.B11 ?? anomalyBag?.B11?.value ?? anomalyBag?.b11?.value)

  const b04 = rawB04FromPayload
  const b08 = rawB08FromPayload
  const b11 = rawB11FromPayload

  const hasRawBands = b04 !== null && b08 !== null
  const bandDifference = hasRawBands ? b08 - b04 : null
  const denominator = hasRawBands ? b08 + b04 : null
  const computedNdvi = denominator !== null && denominator !== 0 ? bandDifference / denominator : null
  const payloadNdvi = toFiniteNumber(properties?.ndvi)
  const finalNdvi = payloadNdvi ?? computedNdvi

  const hasNdmiBands = b08 !== null && b11 !== null
  const ndmiDifference = hasNdmiBands ? b08 - b11 : null
  const ndmiDenominator = hasNdmiBands ? b08 + b11 : null
  const computedNdmi = ndmiDenominator !== null && ndmiDenominator !== 0 ? ndmiDifference / ndmiDenominator : null
  const payloadNdmi = toFiniteNumber(properties?.ndmi)
  const finalNdmi = payloadNdmi ?? computedNdmi

  const payloadRisk = toFiniteNumber(properties?.risk)
  const hasStoredNdmi = Number.isFinite(payloadNdmi)

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
            <div>
              <span className="font-medium">B11 (SWIR)</span>
              <InfoBadge label="B11" description="Sentinel-2 short-wave infrared band centered at 1610 nm." />
              : {formatMetric(b11)}
            </div>
            {b04 === null || b08 === null ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-xs text-amber-800">
                Raw Sentinel-2 band values are not available for this grid yet. Re-run Sentinel-2 processing to populate the real satellite sample.
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="mb-2 text-sm font-semibold text-slate-700">NDVI Calculation (Step-by-Step)</div>
        <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm text-slate-700">
          <div>NDVI = (NIR - RED) / (NIR + RED)</div>
          <div>NDVI = ({formatMetric(b08)} - {formatMetric(b04)}) / ({formatMetric(b08)} + {formatMetric(b04)})</div>
          <div>NDVI = {formatMetric(bandDifference)} / {formatMetric(denominator)}</div>
          <div>NDVI = {formatMetric(computedNdvi)}</div>
          {payloadNdvi !== null && (
            <div className="font-sans text-xs text-slate-600">
              Final value from satellite payload: {formatMetric(payloadNdvi)} (recomputed from stored Sentinel-2 bands)
            </div>
          )}
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3 text-sm">
          <span className="rounded-md bg-slate-100 px-2 py-1 font-semibold text-slate-700">
            NDVI: {formatMetric(finalNdvi)}
          </span>
          <span className={`rounded-md px-2 py-1 font-semibold ${classification.badgeClass}`}>
            {classification.label}
          </span>
        </div>

        <div className="mt-4 rounded-lg border bg-white p-4">
          <div className="mb-2 text-sm font-semibold text-slate-700">NDMI Calculation (Step-by-Step)</div>
          {hasNdmiBands ? (
            <>
              <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm text-slate-700">
                <div>NDMI = (NIR - SWIR) / (NIR + SWIR)</div>
                <div>NDMI = ({formatMetric(b08)} - {formatMetric(b11)}) / ({formatMetric(b08)} + {formatMetric(b11)})</div>
                <div>NDMI = {formatMetric(ndmiDifference)} / {formatMetric(ndmiDenominator)}</div>
                <div>NDMI = {formatMetric(computedNdmi)}</div>
                {payloadNdmi !== null && (
                  <div className="font-sans text-xs text-slate-600">
                    Final value from satellite payload: {formatMetric(payloadNdmi)} (recomputed from stored Sentinel-2 bands)
                  </div>
                )}
              </div>
              <div className="mt-4 flex flex-wrap items-center gap-3 text-sm">
                <span className="rounded-md bg-slate-100 px-2 py-1 font-semibold text-slate-700">
                  NDMI: {formatMetric(finalNdmi)}
                </span>
                <span className={`rounded-md px-2 py-1 font-semibold ${classifyNdmi(finalNdmi).badgeClass}`}>
                  {classifyNdmi(finalNdmi).label}
                </span>
              </div>
            </>
          ) : hasStoredNdmi ? (
            <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <div>NDMI is stored in the dashboard payload for this grid.</div>
              <div className="flex flex-wrap items-center gap-3">
                <span className="rounded-md bg-slate-100 px-2 py-1 font-semibold text-slate-700">
                  NDMI: {formatMetric(payloadNdmi)}
                </span>
                <span className={`rounded-md px-2 py-1 font-semibold ${classifyNdmi(payloadNdmi).badgeClass}`}>
                  {classifyNdmi(payloadNdmi).label}
                </span>
              </div>
              <div className="text-xs text-slate-500">
                Raw SWIR/B11 is not backfilled for this legacy grid, so the formula trace cannot be rebuilt from bands here.
              </div>
            </div>
          ) : (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
              NDMI cannot be calculated yet because SWIR/B11 is missing for this grid. Re-run Sentinel-2 processing to store the real SWIR band.
            </div>
          )}
        </div>

        <div className="mt-4 rounded-lg border bg-white p-4">
          <div className="mb-2 text-sm font-semibold text-slate-700">Risk Score</div>
          <div className="space-y-2 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
            <div>Risk score is supplied by the dashboard from the latest grid-level forecast.</div>
            <div className="flex flex-wrap items-center gap-3">
              <span className="rounded-md bg-slate-100 px-2 py-1 font-semibold text-slate-700">
                Risk: {formatMetric(payloadRisk)}
              </span>
              <span className={`rounded-md px-2 py-1 font-semibold ${classifyRisk(payloadRisk).badgeClass}`}>
                {classifyRisk(payloadRisk).label}
              </span>
            </div>
          </div>
        </div>

        <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          Confidence indicator: Based on Sentinel-2 10m resolution data.
        </div>
      </div>
    </section>
  )
}
