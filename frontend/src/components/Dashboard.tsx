import React, { useEffect, useState, useCallback, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { GeoJSON, MapContainer, TileLayer } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import axios from 'axios'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Legend,
} from 'recharts'

type DashboardData = {
  land: {
    land_id: number
    farmer_name: string
    crop_type: string | null
    geometry: any
    area_sqm: number | null
    created_at: string | null
  }
  grids: {
    type: 'FeatureCollection'
    features: Array<{
      type: 'Feature'
      properties: {
        grid_id: string
        is_water: boolean
        ndvi: number | null
        ndmi: number | null
        lst_c: number | null
        ndvi_norm: number | null
        ndmi_norm: number | null
        lst_norm: number | null
        risk: number | null
        anomalies: Record<string, { zscore: number | null; value: number | null }> | null
      }
      geometry: any
    }>
  }
  latest_date: string | null
  summary: {
    grid_count: number
    ndvi: { mean: number; min: number; max: number; count: number } | null
    ndmi: { mean: number; min: number; max: number; count: number } | null
    lst: { mean: number; min: number; max: number; count: number } | null
    risk: { mean: number; min: number; max: number; count: number } | null
  }
  weather: Array<{ date: string; t2m: number | null; rh2m: number | null; prectotcorr: number | null }>
  processing: { status: string; step: string | null; error: string | null }
}

type ColorMode = 'ndvi' | 'ndmi' | 'lst' | 'risk'

function ndviColor(v: number | null): string {
  if (v === null) return '#808080'
  // Green (high NDVI) to Red (low NDVI)
  const t = Math.max(0, Math.min(1, (v + 0.2) / 1.0))
  const r = Math.round(255 * (1 - t))
  const g = Math.round(200 * t)
  return `rgb(${r},${g},50)`
}

function ndmiColor(v: number | null): string {
  if (v === null) return '#808080'
  const t = Math.max(0, Math.min(1, (v + 0.3) / 0.9))
  const r = Math.round(200 * (1 - t))
  const b = Math.round(220 * t)
  return `rgb(${r},80,${b})`
}

function lstColor(v: number | null): string {
  if (v === null) return '#808080'
  // Blue (cool) to Red (hot), range ~10-50°C
  const t = Math.max(0, Math.min(1, (v - 10) / 40))
  const r = Math.round(255 * t)
  const b = Math.round(255 * (1 - t))
  return `rgb(${r},50,${b})`
}

function riskColor(v: number | null): string {
  if (v === null) return '#808080'
  // Green (low risk) to Red (high risk)
  const t = Math.max(0, Math.min(1, v))
  const r = Math.round(255 * t)
  const g = Math.round(200 * (1 - t))
  return `rgb(${r},${g},50)`
}

export default function Dashboard() {
  const { landId } = useParams<{ landId: string }>()
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [colorMode, setColorMode] = useState<ColorMode>('ndvi')
  const [pollCount, setPollCount] = useState(0)

  const fetchDashboard = useCallback(async () => {
    try {
      const res = await axios.get(`/dashboard/${landId}`)
      setData(res.data)
      setError(null)
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to load dashboard')
    } finally {
      setLoading(false)
    }
  }, [landId])

  useEffect(() => {
    fetchDashboard()
  }, [fetchDashboard])

  // Poll for updates while processing is running
  useEffect(() => {
    if (!data) return
    const ps = data.processing?.status
    if (ps === 'running' || ps === 'queued') {
      const timer = setTimeout(() => {
        fetchDashboard()
        setPollCount(c => c + 1)
      }, 5000)
      return () => clearTimeout(timer)
    }
  }, [data, pollCount, fetchDashboard])

  const mapCenter = useMemo(() => {
    if (!data?.land?.geometry) return [0, 0] as [number, number]
    const coords = data.land.geometry.coordinates?.[0]
    if (!coords || coords.length === 0) return [0, 0] as [number, number]
    let latSum = 0, lonSum = 0
    for (const pt of coords) {
      lonSum += pt[0]
      latSum += pt[1]
    }
    return [latSum / coords.length, lonSum / coords.length] as [number, number]
  }, [data])

  const getGridStyle = useCallback((feature: any) => {
    const props = feature?.properties
    if (!props) return { weight: 1, fillOpacity: 0.3, color: '#666', fillColor: '#808080' }

    if (props.is_water) return { weight: 1, fillOpacity: 0.5, color: '#3388ff', fillColor: '#3388ff' }

    let fillColor = '#808080'
    if (colorMode === 'ndvi') fillColor = ndviColor(props.ndvi)
    else if (colorMode === 'ndmi') fillColor = ndmiColor(props.ndmi)
    else if (colorMode === 'lst') fillColor = lstColor(props.lst_c)
    else if (colorMode === 'risk') fillColor = riskColor(props.risk)

    return { weight: 1, fillOpacity: 0.65, color: '#333', fillColor }
  }, [colorMode])

  const gridChartData = useMemo(() => {
    if (!data?.grids?.features) return []
    return data.grids.features
      .filter(f => !f.properties.is_water)
      .map((f, i) => ({
        idx: i + 1,
        ndvi: f.properties.ndvi != null ? +f.properties.ndvi.toFixed(3) : null,
        ndmi: f.properties.ndmi != null ? +f.properties.ndmi.toFixed(3) : null,
        lst: f.properties.lst_c != null ? +f.properties.lst_c.toFixed(1) : null,
        risk: f.properties.risk != null ? +f.properties.risk.toFixed(3) : null,
      }))
  }, [data])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-500">Loading dashboard for land {landId}...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-red-700">
        <div className="font-semibold">Error</div>
        <div className="mt-1">{error}</div>
        <Link to="/" className="mt-3 inline-block text-sm text-blue-600 underline">Back to registration</Link>
      </div>
    )
  }

  if (!data) return null

  const { land, summary, weather, processing, latest_date } = data
  const isProcessing = processing?.status === 'running' || processing?.status === 'queued'
  const hasData = summary.ndvi !== null || summary.lst !== null
  const latestWeather = weather && weather.length > 0 ? weather[weather.length - 1] : null
  const latestT2m = (typeof latestWeather?.t2m === 'number' && Number.isFinite(latestWeather.t2m)) ? latestWeather.t2m : null

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-green-800">Land Dashboard</h1>
          <p className="text-sm text-gray-600">
            {land.farmer_name} {land.crop_type ? `· ${land.crop_type}` : ''} · Land #{land.land_id}
            {land.area_sqm ? ` · ${Math.round(land.area_sqm).toLocaleString()} m²` : ''}
          </p>
        </div>
        <Link to="/" className="rounded-md bg-gray-200 px-3 py-1.5 text-sm font-medium hover:bg-gray-300">
          + New Land
        </Link>
      </div>

      {/* Processing status */}
      {isProcessing && (
        <div className="rounded-md border border-blue-200 bg-blue-50 p-3 text-sm text-blue-800 animate-pulse">
          Processing: <strong>{processing.step || 'starting'}</strong>... Data will appear as pipelines complete.
        </div>
      )}
      {processing?.status === 'error' && (
        <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          Processing error at step "{processing.step}": {processing.error}
        </div>
      )}

      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">Grids</div>
          <div className="text-xl font-bold">{summary.grid_count}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">NDVI (mean)</div>
          <div className="text-xl font-bold">{summary.ndvi ? summary.ndvi.mean.toFixed(3) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">NDMI (mean)</div>
          <div className="text-xl font-bold">{summary.ndmi ? summary.ndmi.mean.toFixed(3) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">LST (mean °C)</div>
          <div className="text-xl font-bold">{summary.lst ? summary.lst.mean.toFixed(1) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">Risk (mean)</div>
          <div className="text-xl font-bold">{summary.risk ? summary.risk.mean.toFixed(3) : '–'}</div>
        </div>
      </div>

      {latest_date && (
        <div className="text-xs text-gray-500">Latest satellite data: {latest_date}</div>
      )}

      {/* Map + controls */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <div className="rounded-lg border overflow-hidden">
            <div className="flex gap-1 bg-gray-100 p-2">
              {(['ndvi', 'ndmi', 'lst', 'risk'] as ColorMode[]).map(m => (
                <button
                  key={m}
                  className={`rounded px-3 py-1 text-xs font-semibold ${colorMode === m ? 'bg-green-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-200'}`}
                  onClick={() => setColorMode(m)}
                >
                  {m.toUpperCase()}
                </button>
              ))}
            </div>
            <div style={{ height: '55vh' }}>
              <MapContainer center={mapCenter} zoom={16} style={{ height: '100%', width: '100%' }}>
                <TileLayer
                  url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                  attribution="Tiles &copy; Esri"
                />
                {land.geometry && (
                  <GeoJSON
                    data={{ type: 'Feature', properties: {}, geometry: land.geometry } as any}
                    style={() => ({ weight: 2, fillOpacity: 0.05, color: '#fff' })}
                  />
                )}
                {data.grids && data.grids.features.length > 0 && (
                  <GeoJSON
                    key={`${colorMode}-${latestWeather?.date ?? 'no-weather'}`}
                    data={data.grids as any}
                    style={getGridStyle}
                    onEachFeature={(feature, layer) => {
                      const p = feature.properties
                      if (!p) return
                      const lines = [
                        `Grid: ${p.grid_id?.slice(0, 8)}...`,
                        p.is_water ? 'Water' : '',
                        p.ndvi != null ? `NDVI: ${p.ndvi.toFixed(3)}` : '',
                        p.ndmi != null ? `NDMI: ${p.ndmi.toFixed(3)}` : '',
                        p.lst_c != null ? `LST: ${p.lst_c.toFixed(1)}°C` : '',
                        latestT2m != null ? `Temp: ${latestT2m.toFixed(1)}°C` : '',
                        p.risk != null ? `Risk: ${(p.risk * 100).toFixed(1)}%` : '',
                      ].filter(Boolean)
                      layer.bindPopup(lines.join('<br/>'))
                    }}
                  />
                )}
              </MapContainer>
            </div>
          </div>
        </div>

        {/* Side panel: charts */}
        <div className="space-y-4">
          {/* Grid bar chart */}
          {hasData && gridChartData.length > 0 && (
            <div className="rounded-lg border bg-white p-3">
              <div className="text-sm font-semibold mb-2">
                Per-Grid {colorMode.toUpperCase()}
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={gridChartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="idx" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Bar
                    dataKey={colorMode === 'lst' ? 'lst' : colorMode}
                    fill={colorMode === 'ndvi' ? '#22c55e' : colorMode === 'ndmi' ? '#3b82f6' : colorMode === 'lst' ? '#ef4444' : '#f59e0b'}
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Weather chart */}
          {weather && weather.length > 0 && (
            <div className="rounded-lg border bg-white p-3">
              <div className="text-sm font-semibold mb-2">Weather (NASA POWER)</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={weather}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" tick={{ fontSize: 9 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                  <Line type="monotone" dataKey="t2m" name="Temp (°C)" stroke="#ef4444" dot={false} />
                  <Line type="monotone" dataKey="rh2m" name="RH (%)" stroke="#3b82f6" dot={false} />
                  <Line type="monotone" dataKey="prectotcorr" name="Precip (mm)" stroke="#22c55e" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* No data message */}
          {!hasData && !isProcessing && (
            <div className="rounded-lg border bg-yellow-50 p-4 text-sm text-yellow-800">
              <div className="font-semibold">No satellite data yet</div>
              <p className="mt-1">
                Grids are ready ({summary.grid_count} cells). Satellite indices will appear once
                the processing pipeline completes. This may take a few minutes depending on data availability.
              </p>
              <button
                className="mt-3 rounded bg-green-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-green-700"
                onClick={async () => {
                  try {
                    await axios.post(`/dashboard/${landId}/process`)
                    fetchDashboard()
                  } catch {}
                }}
              >
                Retry Processing
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
