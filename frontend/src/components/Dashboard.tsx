import React, { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { useParams, Link } from 'react-router-dom'
import { GeoJSON, MapContainer, TileLayer } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import axios from 'axios'
import ScientificLegend, { getColorForValue } from './ScientificLegend'
import TraceabilityModal from './TraceabilityModal'
import AvailableDataPanel from './AvailableDataPanel'
import GridInspector from './GridInspector'
import EvidencePanel from './EvidencePanel'
import ValidationPanel from './ValidationPanel'
import MethodologyPanel from './MethodologyPanel'
import TemporalAnalysisPanel from './TemporalAnalysisPanel'
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
        b04: number | null
        b08: number | null
        b11: number | null
        stac_item_id: string | null
        acquisition_datetime: string | null
        tile_id: string | null
        cloud_coverage_pct: number | null
        ndvi: number | null
        ndmi: number | null
        lst_c: number | null
        pixel_count: number | null
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
  latest_complete_date: string | null
  mode: 'latest' | 'select'
  selected_date: string | null
  active_data_date: string | null
  provenance: {
    satellite_source: string | null
    acquisition_date: string | null
    acquisition_datetime: string | null
    stac_item_id: string | null
    tile_id: string | null
    cloud_coverage_pct: number | null
  } | null
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

type ColorMode = 'ndvi' | 'ndmi' | 'ndre' | 'evi' | 'savi' | 'gci' | 'lst' | 'risk'
type Persona = 'farmer' | 'researcher'
type DashboardTab = 'dashboard' | 'grid-inspector' | 'evidence' | 'validation' | 'methodology' | 'available-data' | 'temporal-analysis'

const DASHBOARD_TABS: Array<{ id: DashboardTab; label: string }> = [
  { id: 'dashboard', label: 'Dashboard' },
  { id: 'grid-inspector', label: 'Grid Inspector' },
  { id: 'evidence', label: 'Evidence' },
  { id: 'validation', label: 'Validation' },
  { id: 'methodology', label: 'Methodology' },
  { id: 'available-data', label: 'Available Data' },
  { id: 'temporal-analysis', label: 'Temporal Analysis' },
]

function riskColor(v: number | null): string {
  if (v === null) return '#808080'
  // Green (low risk) to Red (high risk)
  const t = Math.max(0, Math.min(1, v))
  const r = Math.round(255 * t)
  const g = Math.round(200 * (1 - t))
  return `rgb(${r},${g},50)`
}

class TabErrorBoundary extends React.Component<
  { title: string; children: React.ReactNode },
  { hasError: boolean; message: string | null }
> {
  constructor(props: { title: string; children: React.ReactNode }) {
    super(props)
    this.state = { hasError: false, message: null }
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, message: error.message }
  }

  componentDidCatch(error: Error) {
    console.error(`${this.props.title} failed`, error)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-800">
          <div className="font-semibold">{this.props.title} is temporarily unavailable</div>
          <div className="mt-1">{this.state.message || 'A rendering error occurred.'}</div>
          <div className="mt-2 text-xs text-rose-700">Refresh the page after the dashboard settles, or return to the main tab.</div>
        </div>
      )
    }

    return this.props.children
  }
}

export default function Dashboard() {
  const { landId } = useParams<{ landId: string }>()
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<DashboardTab>('dashboard')
  const [colorMode, setColorMode] = useState<ColorMode>('ndvi')
  const [persona, setPersona] = useState<Persona>('farmer')
  const [traceModalKey, setTraceModalKey] = useState<string | null>(null)
  const [pollCount, setPollCount] = useState(0)
  const [hoveredGridId, setHoveredGridId] = useState<number | null>(null)
  const [selectedGridId, setSelectedGridId] = useState<number | null>(null)
  const mountedRef = useRef(true)

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
    }
  }, [])

  // Reset UI state when navigating between lands.
  useEffect(() => {
    setData(null)
    setError(null)
    setLoading(true)
  }, [landId])

  const fetchDashboard = useCallback(async () => {
    try {
      const res = await axios.get(`/dashboard/${landId}`)
      if (!mountedRef.current) return
      setData(res.data)
      setError(null)
    } catch (err: any) {
      if (!mountedRef.current) return
      setError(err?.response?.data?.detail || err.message || 'Failed to load dashboard')
    } finally {
      if (mountedRef.current) {
        setLoading(false)
      }
    }
  }, [landId])

  useEffect(() => {
    void fetchDashboard()
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

    const b02 = props.b02
    const b03 = props.b03
    const b04 = props.b04
    const b05 = props.b05
    const b08 = props.b08
    const b11 = props.b11

    let fillColor = '#808080'
    if (colorMode === 'ndvi') {
      fillColor = getColorForValue('ndvi', props.ndvi)
    } else if (colorMode === 'ndmi') {
      fillColor = getColorForValue('ndmi', props.ndmi)
    } else if (colorMode === 'ndre') {
      const ndre = props.ndre ?? (b08 && b05 ? (b08 - b05) / (b08 + b05) : null)
      fillColor = getColorForValue('ndre', ndre)
    } else if (colorMode === 'evi') {
      const evi = props.evi ?? (b08 && b04 && b02 ? 2.5 * ((b08 - b04) / (b08 + 6 * b04 - 7.5 * b02 + 1)) : null)
      fillColor = getColorForValue('evi', evi)
    } else if (colorMode === 'savi') {
      const savi = props.savi ?? (b08 && b04 ? ((b08 - b04) / (b08 + b04 + 0.5)) * 1.5 : null)
      fillColor = getColorForValue('savi', savi)
    } else if (colorMode === 'gci') {
      const gci = props.gci ?? (b08 && b03 ? (b08 / b03) - 1 : null)
      fillColor = getColorForValue('gci', gci)
    } else if (colorMode === 'lst') {
      fillColor = getColorForValue('lst', props.lst_c)
    } else if (colorMode === 'risk') {
      fillColor = riskColor(props.risk)
    }

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

  const gridSelectionOptions = useMemo(() => {
    if (!data?.grids?.features) return [] as Array<{ value: number; label: string }>
    return data.grids.features
      .filter(f => !f.properties.is_water)
      .map((f) => {
        const gridId = Number(f.properties.grid_id)
        const rowCol = f.properties.row != null && f.properties.col != null
          ? ` (R${f.properties.row}, C${f.properties.col})`
          : ''
        return {
          value: gridId,
          label: `Grid ${gridId}${rowCol}`,
        }
      })
      .filter((x) => Number.isFinite(x.value))
  }, [data])

  useEffect(() => {
    if (selectedGridId !== null) return
    if (gridSelectionOptions.length > 0) {
      setSelectedGridId(gridSelectionOptions[0].value)
    }
  }, [gridSelectionOptions, selectedGridId])

  const selectedGridFeature = useMemo(() => {
    if (!data?.grids?.features || data.grids.features.length === 0) return null

    if (selectedGridId !== null) {
      const selected = data.grids.features.find(
        (feature) => Number(feature.properties.grid_id) === selectedGridId
      )
      if (selected) return selected
    }

    const firstNonWater = data.grids.features.find((feature) => !feature.properties.is_water)
    return firstNonWater || data.grids.features[0]
  }, [data, selectedGridId])

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

  if (!data) {
    return (
      <div className="rounded-lg border bg-white p-6 text-sm text-gray-600">
        {loading
          ? `Loading dashboard for land ${landId}...`
          : 'Dashboard data is not available yet. Please try again in a moment.'}
      </div>
    )
  }

  const land = data?.land || { land_id: Number(landId), farmer_name: 'Land', crop_type: null, geometry: null, area_sqm: null, created_at: null }
  const summary = data?.summary || { grid_count: 0, ndvi: null, ndmi: null, lst: null, risk: null }
  const weather = data?.weather || []
  const processing = data?.processing || { status: 'idle', step: null, error: null }
  const { latest_date, latest_complete_date, mode, selected_date, active_data_date } = data
  const isProcessing = processing?.status === 'running' || processing?.status === 'queued'
  const hasData = Boolean(summary?.ndvi || summary?.lst)
  const latestWeather = weather && weather.length > 0 ? weather[weather.length - 1] : null
  const latestT2m = (typeof latestWeather?.t2m === 'number' && Number.isFinite(latestWeather.t2m)) ? latestWeather.t2m : null
  const analysisDate = active_data_date || latest_complete_date || latest_date

  return (
    <div className="space-y-4">
      {/* Header with Dual Persona Switcher */}
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white p-4 shadow-xs">
        <div>
          <div className="flex items-center gap-2">
            <span className="rounded bg-emerald-100 px-2 py-0.5 text-xs font-bold text-emerald-800">
              CPDE v2 Engine
            </span>
            <h1 className="text-xl font-bold text-slate-800">Precision Field Cockpit</h1>
          </div>
          <p className="mt-0.5 text-xs text-slate-600">
            {land.farmer_name} {land.crop_type ? `· Crop: ${land.crop_type}` : ''} · Field #{land.land_id}
            {land.area_sqm ? ` · Area: ${(land.area_sqm / 10000).toFixed(2)} ha (${Math.round(land.area_sqm).toLocaleString()} m²)` : ''}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {/* Dual Persona Switcher */}
          <div className="flex items-center rounded-lg border border-slate-200 bg-slate-50 p-1">
            <button
              type="button"
              onClick={() => setPersona('farmer')}
              className={`flex items-center gap-1.5 rounded-md px-3 py-1 text-xs font-bold transition ${
                persona === 'farmer' ? 'bg-emerald-600 text-white shadow-xs' : 'text-slate-600 hover:bg-slate-200'
              }`}
            >
              👨‍🌾 Farmer View
            </button>
            <button
              type="button"
              onClick={() => setPersona('researcher')}
              className={`flex items-center gap-1.5 rounded-md px-3 py-1 text-xs font-bold transition ${
                persona === 'researcher' ? 'bg-indigo-600 text-white shadow-xs' : 'text-slate-600 hover:bg-slate-200'
              }`}
            >
              🔬 Researcher View
            </button>
          </div>

          <Link to="/" className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-100">
            + New Field
          </Link>
        </div>
      </div>

      {/* Farmer View: High-Contrast Actionable Status Card */}
      {persona === 'farmer' && (
        <div className="rounded-xl border border-emerald-200 bg-gradient-to-r from-emerald-50 to-teal-50 p-4 shadow-xs">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-bold ${
                (summary.ndvi?.mean ?? 0.5) >= 0.60
                  ? 'bg-emerald-600 text-white'
                  : (summary.ndvi?.mean ?? 0.5) >= 0.40
                    ? 'bg-amber-500 text-white'
                    : 'bg-red-600 text-white'
              }`}>
                {(summary.ndvi?.mean ?? 0.5) >= 0.60
                  ? '✓ Optimal Crop Vigor'
                  : (summary.ndvi?.mean ?? 0.5) >= 0.40
                    ? '⚠ Moderate Canopy Growth'
                    : '🚨 Early Crop Stress Alert'}
              </span>
              <span className="text-xs font-semibold text-slate-700">Sentinel-2 10m High-Resolution Diagnosis</span>
            </div>
            <span className="text-xs text-slate-500">
              {analysisDate ? `Observation Date: ${analysisDate}` : 'Latest available capture'}
            </span>
          </div>

          <div className="mt-2 text-sm text-slate-800">
            {(summary.ndvi?.mean ?? 0.5) >= 0.60 ? (
              <p>
                <strong>Farmer Guidance:</strong> Field vegetation is actively photosynthesizing with balanced moisture. No premature chlorosis or thermal stress detected. Maintain scheduled agronomic practices.
              </p>
            ) : (summary.ndvi?.mean ?? 0.5) >= 0.40 ? (
              <p>
                <strong>Farmer Guidance:</strong> Canopy growth is moderate. Moisture levels are stable. Inspect lower leaves for minor nitrogen deficiency before next watering.
              </p>
            ) : (
              <p>
                <strong>Urgent Action Required:</strong> Pre-cause stress detected (declining canopy moisture and red-edge absorption before visible wilting). Schedule supplemental irrigation within 24 to 48 hours.
              </p>
            )}
          </div>
        </div>
      )}

      {/* Researcher View: Scientific Export Center */}
      {persona === 'researcher' && (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-indigo-100 bg-indigo-50/50 p-3 text-xs">
          <div className="flex items-center gap-2">
            <span className="font-bold text-indigo-900">Research Publication Center:</span>
            <span className="text-slate-600">Export 10m grid observation datasets with raw bands & indices</span>
          </div>
          <div className="flex items-center gap-2">
            <a
              href={`/api/v2/fields/${landId}/export?format=csv`}
              download
              className="rounded-md border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700 hover:bg-slate-50"
            >
              📥 CSV
            </a>
            <a
              href={`/api/v2/fields/${landId}/export?format=geojson`}
              download
              className="rounded-md border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700 hover:bg-slate-50"
            >
              📥 GeoJSON
            </a>
            <a
              href={`/api/v2/fields/${landId}/export?format=parquet`}
              download
              className="rounded-md border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700 hover:bg-slate-50"
            >
              📥 Parquet
            </a>
            <button
              onClick={() => setTraceModalKey('ndvi')}
              className="rounded-md bg-indigo-600 px-2.5 py-1 font-semibold text-white hover:bg-indigo-700"
            >
              🔬 Lineage & Citations
            </button>
          </div>
        </div>
      )}

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

      {analysisDate && (
        <div className="text-xs text-gray-500">
          {mode === 'select'
            ? `Data date: ${analysisDate} (User selected)`
            : `Latest satellite data: ${analysisDate}`}
        </div>
      )}

      <div className="rounded-lg border bg-white p-2">
        <div className="flex flex-wrap gap-2">
          {DASHBOARD_TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              className={`rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'bg-green-700 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {activeTab === 'dashboard' && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="lg:col-span-2">
            <div className="rounded-lg border overflow-hidden">
              <div className="flex flex-wrap gap-1 bg-slate-100 p-2">
                {(['ndvi', 'ndmi', 'ndre', 'evi', 'savi', 'gci', 'lst', 'risk'] as ColorMode[]).map(m => (
                  <button
                    key={m}
                    className={`rounded px-2.5 py-1 text-xs font-bold uppercase transition ${
                      colorMode === m
                        ? 'bg-emerald-600 text-white shadow-xs'
                        : 'bg-white text-slate-700 hover:bg-slate-200'
                    }`}
                    onClick={() => setColorMode(m)}
                  >
                    {m}
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

          <div className="space-y-4">
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
                          formatter={((value: any, _name: any, payload: any) => {
                            if (value == null) return ['–', activeMetricKey.toUpperCase()]
                            const row = payload?.payload?.row
                            const col = payload?.payload?.col
                            const meta = row != null && col != null ? ` (row ${row}, col ${col})` : ''
                            return [value, `${activeMetricKey.toUpperCase()}${meta}`]
                          }) as any}

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
      )}

      {activeTab === 'grid-inspector' && (
        <GridInspector
          selectedGrid={selectedGridFeature as any}
          gridOptions={gridSelectionOptions}
          onSelectGrid={setSelectedGridId}
          latestDate={analysisDate}
        />
      )}

      {activeTab === 'evidence' && (
        <EvidencePanel
          selectedGrid={selectedGridFeature as any}
          latestDate={analysisDate}
          provenance={data.provenance || null}
        />
      )}

      {activeTab === 'validation' && (
        <ValidationPanel selectedGrid={selectedGridFeature as any} />
      )}

      {activeTab === 'methodology' && (
        <MethodologyPanel selectedGrid={selectedGridFeature as any} />
      )}

      {activeTab === 'available-data' && (
        <AvailableDataPanel
          landId={landId || ''}
          landGeometry={land.geometry}
          latestDate={latest_complete_date || latest_date}
          mode={mode}
          selectedDate={selected_date}
          activeDataDate={analysisDate}
          processingStatus={processing?.status || 'unknown'}
          onRefresh={fetchDashboard}
        />
      )}

      {activeTab === 'temporal-analysis' && (
        <TabErrorBoundary title="Temporal Analysis">
          <TemporalAnalysisPanel
            landId={landId || ''}
            activeDate={analysisDate}
            mode={mode}
          />
        </TabErrorBoundary>
      )}

      {/* Scientific Traceability Modal */}
      <TraceabilityModal
        indexKey={traceModalKey}
        isOpen={!!traceModalKey}
        onClose={() => setTraceModalKey(null)}
      />
    </div>
  )
}

