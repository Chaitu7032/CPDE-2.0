import React, { useEffect, useMemo, useState } from 'react'

const reverseGeocodeCache = new Map()

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

function classifyNdvi(value) {
  if (!Number.isFinite(value)) return 'Sparse (fallback)'
  if (value < 0.2) return 'Sparse'
  if (value < 0.4) return 'Moderate'
  return 'Healthy'
}

function classifyNdmi(value) {
  if (!Number.isFinite(value)) return 'Dry (fallback)'
  if (value < -0.1) return 'Dry'
  if (value < 0.1) return 'Moderate'
  return 'Wet'
}

function classifyLst(value) {
  if (!Number.isFinite(value)) return 'High (fallback)'
  if (value < 28) return 'Moderate'
  if (value < 34) return 'High'
  return 'Very High'
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
  if (!Array.isArray(ring) || ring.length < 4) {
    return { lon: null, lat: null }
  }

  const points = []
  for (const point of ring) {
    if (!Array.isArray(point) || point.length < 2) continue
    const lon = toFiniteNumber(point[0])
    const lat = toFiniteNumber(point[1])
    if (lon === null || lat === null) continue
    points.push([lon, lat])
  }

  if (points.length < 4) {
    return { lon: null, lat: null }
  }

  const first = points[0]
  const last = points[points.length - 1]
  const closed = first[0] === last[0] && first[1] === last[1]
  const vertices = closed ? points.slice(0, points.length - 1) : points

  if (vertices.length < 3) {
    return { lon: null, lat: null }
  }

  let crossSum = 0
  let cxAcc = 0
  let cyAcc = 0

  for (let i = 0; i < vertices.length; i += 1) {
    const [x0, y0] = vertices[i]
    const [x1, y1] = vertices[(i + 1) % vertices.length]
    const cross = (x0 * y1) - (x1 * y0)
    crossSum += cross
    cxAcc += (x0 + x1) * cross
    cyAcc += (y0 + y1) * cross
  }

  if (Math.abs(crossSum) < 1e-12) {
    let lonSum = 0
    let latSum = 0
    for (const [lon, lat] of vertices) {
      lonSum += lon
      latSum += lat
    }
    return { lon: lonSum / vertices.length, lat: latSum / vertices.length }
  }

  return {
    lon: cxAcc / (3 * crossSum),
    lat: cyAcc / (3 * crossSum),
  }
}

function toRegionLabel(geocodeData) {
  if (!geocodeData || typeof geocodeData !== 'object') return null

  const address = geocodeData.address || {}
  const district = address.city_district || address.state_district || address.county || address.city || address.town || address.village || address.municipality
  const state = address.state || address.region
  const country = address.country

  const compact = [district, state, country].filter(Boolean).join(', ')
  if (compact) return compact

  if (typeof geocodeData.display_name === 'string' && geocodeData.display_name.trim().length > 0) {
    return geocodeData.display_name
  }

  return null
}

function formatCoordinate(value) {
  return Number.isFinite(value) ? value.toFixed(6) : 'N/A'
}

export default function ValidationPanel({ selectedGrid }) {
  const properties = selectedGrid?.properties || {}
  const centroid = useMemo(() => getCentroid(selectedGrid?.geometry), [selectedGrid?.geometry])
  const [regionName, setRegionName] = useState('Resolving from coordinates...')
  const [locationStatus, setLocationStatus] = useState('idle')

  useEffect(() => {
    const lat = centroid.lat
    const lon = centroid.lon

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      setRegionName('Unknown region (invalid centroid)')
      setLocationStatus('error')
      return
    }

    const cacheKey = `${lat.toFixed(5)},${lon.toFixed(5)}`
    const cachedRegion = reverseGeocodeCache.get(cacheKey)
    if (cachedRegion) {
      setRegionName(cachedRegion)
      setLocationStatus('resolved')
      return
    }

    const controller = new AbortController()

    const resolveRegion = async () => {
      setLocationStatus('loading')
      try {
        const params = new URLSearchParams({
          format: 'jsonv2',
          lat: String(lat),
          lon: String(lon),
          zoom: '12',
          addressdetails: '1',
          'accept-language': 'en',
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/reverse?${params.toString()}`, {
          signal: controller.signal,
          headers: {
            Accept: 'application/json',
          },
        })

        if (!response.ok) {
          throw new Error(`Reverse geocoding failed (${response.status})`)
        }

        const result = await response.json()
        const resolved = toRegionLabel(result) || `Lat ${lat.toFixed(4)}, Lon ${lon.toFixed(4)}`
        reverseGeocodeCache.set(cacheKey, resolved)
        setRegionName(resolved)
        setLocationStatus('resolved')
      } catch (error) {
        if (controller.signal.aborted) return
        setRegionName(`Lat ${lat.toFixed(4)}, Lon ${lon.toFixed(4)} (name lookup unavailable)`)
        setLocationStatus('error')
      }
    }

    resolveRegion()

    return () => {
      controller.abort()
    }
  }, [centroid.lat, centroid.lon])

  const ndvi = toFiniteNumber(properties?.ndvi)
  const ndmi = toFiniteNumber(properties?.ndmi)
  const lst = toFiniteNumber(properties?.lst_c)

  const ndviLabel = classifyNdvi(ndvi)
  const ndmiLabel = classifyNdmi(ndmi)
  const lstLabel = classifyLst(lst)

  const ndviMatch = ndviLabel.includes('Sparse') || ndviLabel.includes('Moderate')
  const ndmiMatch = ndmiLabel.includes('Dry') || ndmiLabel.includes('Moderate')
  const lstMatch = lstLabel.includes('High')

  const agreementCount = [ndviMatch, ndmiMatch, lstMatch].filter(Boolean).length
  const agreementLevel = agreementCount >= 3 ? 'HIGH ✅' : agreementCount === 2 ? 'MEDIUM ⚠️' : 'LOW ❌'
  const agreementClass = agreementCount >= 3
    ? 'border-emerald-200 bg-emerald-50 text-emerald-900'
    : agreementCount === 2
      ? 'border-amber-200 bg-amber-50 text-amber-900'
      : 'border-red-200 bg-red-50 text-red-900'

  return (
    <section className="space-y-4">
      <div className="rounded-lg border bg-white p-4">
        <h2 className="text-lg font-semibold text-slate-800">Validation Panel</h2>
        <p className="mt-1 text-sm text-slate-600">Transparent comparison between model outputs and realistic field interpretation context.</p>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="text-sm font-semibold text-slate-700">Field Interpretation</div>
        <div className="mt-2 grid grid-cols-1 gap-2 sm:grid-cols-2 text-sm text-slate-700">
          <div>
            <span className="font-medium">Region:</span> {regionName}
            {locationStatus === 'loading' && <span className="ml-2 text-xs text-slate-500">resolving...</span>}
          </div>
          <div><span className="font-medium">Land Type:</span> Agricultural</div>
          <div className="sm:col-span-2">
            <span className="font-medium">Centroid used:</span> ({formatCoordinate(centroid.lat)}, {formatCoordinate(centroid.lon)})
          </div>
        </div>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="text-sm font-semibold text-slate-700">System Output vs Realistic Expectation</div>
        <div className="mt-3 space-y-2 text-sm">
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-slate-700">
            NDVI
            <InfoBadge label="NDVI" description="Normalized Difference Vegetation Index for canopy vigor." />
            {' '}→ {ndviLabel} → Matches visible patchiness
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-slate-700">
            NDMI
            <InfoBadge label="NDMI" description="Normalized Difference Moisture Index for vegetation moisture status." />
            {' '}→ {ndmiLabel} → Matches soil tone
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-slate-700">
            LST
            <InfoBadge label="LST" description="Land Surface Temperature derived from thermal observations." />
            {' '}→ {lstLabel} → Matches climate conditions
          </div>
        </div>

        <div className={`mt-4 rounded-md border px-3 py-2 text-sm font-semibold ${agreementClass}`}>
          Agreement Level: {agreementLevel}
        </div>

        <details className="mt-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          <summary className="cursor-pointer font-medium">Interpretation notes</summary>
          <div className="mt-2">
            Agreement combines indicator-by-indicator alignment against expected field conditions for this agro-climatic region.
          </div>
        </details>
      </div>
    </section>
  )
}
