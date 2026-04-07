import React, { useEffect, useState, useCallback, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { GeoJSON, MapContainer, TileLayer } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import axios from 'axios'
import ScientificLegend from './ScientificLegend'
import {
  BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
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
        grid_id: number
        internal_grid_key: string
        row: number | null
        col: number | null
        is_water: boolean
        ndvi: number | null
        ndmi: number | null
        lst_c: number | null
        ndvi_norm: number | null
        ndmi_norm: number | null
        lst_norm: number | null
        risk: number | null
        color: {
          ndvi: string
          ndmi: string
          lst: string
        }
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
  color_scales?: {
    ndvi: Array<{ range: string; label: string; color: string }>
    ndmi: Array<{ range: string; label: string; color: string }>
    lst: Array<{ range: string; label: string; color: string }>
    no_data_color: string
  }
}

type ColorMode = 'ndvi' | 'ndmi' | 'lst' | 'risk'

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
  const [hoveredGridId, setHoveredGridId] = useState<number | null>(null)
  const [selectedGridId, setSelectedGridId] = useState<number | null>(null)

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
    if (!props) return { weight: 1, fillOpacity: 0.68, color: '#333', fillColor: '#808080' }

    const gridId = Number(props.grid_id)
    const isHovered = Number.isFinite(gridId) && hoveredGridId === gridId
    const isSelected = Number.isFinite(gridId) && selectedGridId === gridId

    if (props.is_water) return { weight: 1, fillOpacity: 0.68, color: '#333', fillColor: '#3388ff' }

    const backendColor = colorMode === 'ndvi'
      ? props.color?.ndvi
      : colorMode === 'ndmi'
        ? props.color?.ndmi
        : colorMode === 'lst'
          ? props.color?.lst
          : null

    const fillColor = backendColor || (colorMode === 'risk' ? riskColor(props.risk) : '#808080')

    if (isHovered) {
      return { weight: 2.5, fillOpacity: 0.8, color: '#333', fillColor }
    }
    if (isSelected) {
      return { weight: 2.5, fillOpacity: 0.78, color: '#333', fillColor }
    }

    return { weight: 1, fillOpacity: 0.68, color: '#333', fillColor }
  }, [colorMode, hoveredGridId, selectedGridId])

  const gridChartData = useMemo(() => {
    if (!data?.grids?.features) return []
    return data.grids.features
      .filter(f => !f.properties.is_water)
      .map((f) => ({
        grid_id: Number(f.properties.grid_id),
        row: f.properties.row,
        col: f.properties.col,
        ndvi: f.properties.ndvi != null ? +f.properties.ndvi.toFixed(3) : null,
        ndmi: f.properties.ndmi != null ? +f.properties.ndmi.toFixed(3) : null,
        lst: f.properties.lst_c != null ? +f.properties.lst_c.toFixed(1) : null,
        risk: f.properties.risk != null ? +f.properties.risk.toFixed(3) : null,
        colors: f.properties.color,
      }))
      .sort((a, b) => a.grid_id - b.grid_id)
  }, [data])

  const activeMetricKey = colorMode === 'lst' ? 'lst' : colorMode
  const barBaseColor = colorMode === 'ndvi' ? '#16a34a' : colorMode === 'ndmi' ? '#2563eb' : colorMode === 'lst' ? '#dc2626' : '#f59e0b'
  const chartWidth = useMemo(() => Math.max(700, gridChartData.length * 24), [gridChartData.length])
  const xTickInterval = useMemo(() => {
    if (gridChartData.length <= 24) return 0
    return Math.floor(gridChartData.length / 24)
  }, [gridChartData.length])

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
                      const gridId = Number(p.grid_id)
                      if (Number.isFinite(gridId)) {
                        layer.on({
                          mouseover: () => setHoveredGridId(gridId),
                          mouseout: () => {
                            setHoveredGridId(current => (current === gridId ? null : current))
                          },
                          click: () => setSelectedGridId(gridId),
                        })
                      }
                      const lines = [
                        `Grid: ${p.grid_id}`,
                        p.row != null && p.col != null ? `Row/Col: ${p.row}, ${p.col}` : '',
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

          <ScientificLegend
            ndvi={summary.ndvi?.mean ?? null}
            ndmi={summary.ndmi?.mean ?? null}
            lst={summary.lst?.mean ?? null}
            risk={summary.risk?.mean ?? null}
            colorScales={data.color_scales}
          />
        </div>

        {/* Side panel: charts */}
        <div className="space-y-4">
          {/* Grid bar chart */}
          {hasData && gridChartData.length > 0 && (
            <div className="rounded-lg border bg-white p-3">
              <div className="text-sm font-semibold mb-2">
                Per-Grid {colorMode.toUpperCase()}
              </div>
              <div className="mb-2 text-xs text-gray-600">
                {selectedGridId != null ? `Selected grid: ${selectedGridId}` : 'Click a grid or bar to lock selection'}
              </div>
              <div className="overflow-x-auto">
                <div style={{ width: chartWidth, height: 220 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      data={gridChartData}
                      margin={{ top: 8, right: 8, left: 0, bottom: 40 }}
                      onMouseMove={(state: any) => {
                        const gid = state?.activePayload?.[0]?.payload?.grid_id
                        if (typeof gid === 'number' && Number.isFinite(gid)) {
                          setHoveredGridId(gid)
                        }
                      }}
                      onMouseLeave={() => setHoveredGridId(null)}
                      onClick={(state: any) => {
                        const gid = state?.activePayload?.[0]?.payload?.grid_id
                        if (typeof gid === 'number' && Number.isFinite(gid)) {
                          setSelectedGridId(gid)
                        }
                      }}
                    >
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis
                        dataKey="grid_id"
                        tick={{ fontSize: 10 }}
                        interval={xTickInterval}
                        angle={-35}
                        textAnchor="end"
                        height={52}
                      />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip
                        formatter={(value: number | null, _name: string, payload: any) => {
                          if (value == null) return ['–', activeMetricKey.toUpperCase()]
                          const row = payload?.payload?.row
                          const col = payload?.payload?.col
                          const meta = row != null && col != null ? ` (row ${row}, col ${col})` : ''
                          return [value, `${activeMetricKey.toUpperCase()}${meta}`]
                        }}
                        labelFormatter={(label) => `Grid ${label}`}
                      />
                      <Bar dataKey={activeMetricKey}>
                        {gridChartData.map((entry) => {
                          const isHovered = entry.grid_id === hoveredGridId
                          const isSelected = entry.grid_id === selectedGridId
                          const metricColor = colorMode === 'ndvi'
                            ? entry.colors?.ndvi
                            : colorMode === 'ndmi'
                              ? entry.colors?.ndmi
                              : colorMode === 'lst'
                                ? entry.colors?.lst
                                : null
                          const fill = isSelected ? '#111827' : isHovered ? '#1f2937' : (metricColor || barBaseColor)
                          return (
                            <Cell
                              key={`grid-${entry.grid_id}`}
                              fill={fill}
                              fillOpacity={isSelected || isHovered ? 1 : 0.78}
                              stroke={isSelected ? '#000000' : 'none'}
                              strokeWidth={isSelected ? 1 : 0}
                            />
                          )
                        })}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
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
