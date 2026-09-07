import React, { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { useParams, Link } from 'react-router-dom'
import { GeoJSON, MapContainer, TileLayer, useMap } from 'react-leaflet'
import L from 'leaflet'
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

function AutoFitMapBounds({ geometry, triggerCount }: { geometry: any; triggerCount?: number }) {
  const map = useMap()
  useEffect(() => {
    if (!geometry) return
    try {
      const layer = L.geoJSON(geometry)
      const bounds = layer.getBounds()
      if (bounds.isValid()) {
        map.flyToBounds(bounds, { padding: [35, 35], maxZoom: 18, duration: 1.0 })
      }
    } catch (e) {
      console.warn('AutoFitMapBounds error:', e)
    }
  }, [geometry, map, triggerCount])
  return null
}
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
        b02?: number | null
        b03?: number | null
        b04: number | null
        b05?: number | null
        b08: number | null
        b8a?: number | null
        b11: number | null
        ndre?: number | null
        evi?: number | null
        savi?: number | null
        gci?: number | null
        sar_vv_db?: number | null
        sar_vh_db?: number | null
        sar_cr?: number | null
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
        stress_prob?: number | null
        qa_score?: number | null
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
    ndre?: { mean: number; min: number; max: number; count: number } | null
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

type ColorMode = 'ndvi' | 'ndmi' | 'ndre' | 'evi' | 'savi' | 'gci' | 'lst' | 'sar' | 'stress_prob' | 'uncertainty' | 'risk'
type BasemapKey = 's2-tci' | 'google-hybrid' | 'esri'
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
  const [basemap, setBasemap] = useState<BasemapKey>('s2-tci')
  const [gridOpacity, setGridOpacity] = useState<number>(0.72)
  const [recenterCount, setRecenterCount] = useState<number>(0)
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
    } else if (colorMode === 'sar') {
      fillColor = getColorForValue('sar', props.sar_vh_db ?? props.sar_vv_db ?? null)
    } else if (colorMode === 'stress_prob' || colorMode === 'risk') {
      fillColor = getColorForValue('stress_prob', props.risk ?? props.stress_prob ?? null)
    } else if (colorMode === 'uncertainty') {
      fillColor = getColorForValue('uncertainty', props.qa_score ?? 100)
    }

    if (isHovered) {
      return { weight: 2.5, fillOpacity: Math.min(1.0, gridOpacity + 0.2), color: '#fbbf24', fillColor }
    }
    if (isSelected) {
      return { weight: 2.5, fillOpacity: Math.min(1.0, gridOpacity + 0.15), color: '#ffffff', fillColor }
    }

    return { weight: 0.8, fillOpacity: gridOpacity, color: '#334155', fillColor }
  }, [colorMode, hoveredGridId, selectedGridId, gridOpacity])

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
  const stacItemId = data?.provenance?.stac_item_id || data?.grids?.features?.[0]?.properties?.stac_item_id || null

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4 rounded-xl border border-slate-200 bg-white p-4 shadow-xs">
        <div>
          <div className="flex items-center gap-2">
            <span className="rounded-md bg-emerald-100 px-2 py-0.5 text-xs font-bold text-emerald-800">
              CPDE 2.0 Engine
            </span>
            <h1 className="text-xl font-bold text-slate-800">Near-Real-Time Satellite Crop Monitoring & Decision Support</h1>
          </div>
          <p className="mt-0.5 text-xs text-slate-600">
            {land.farmer_name} {land.crop_type ? `· Crop: ${land.crop_type}` : ''} · Field #{land.land_id}
            {land.area_sqm ? ` · Area: ${(land.area_sqm / 10000).toFixed(2)} ha (${Math.round(land.area_sqm).toLocaleString()} m²)` : ''}
            <span className="ml-2 text-emerald-700 font-medium">Bapatla District, AP Validation Site</span>
          </p>
          <div className="mt-1 flex items-center gap-1 text-[11px] font-mono text-slate-500">
            <span>Satellite Acquisition</span>
            <span>→</span>
            <span>Quality Control (QA)</span>
            <span>→</span>
            <span>Resampling (UTM 44N)</span>
            <span>→</span>
            <span>Phenological Baseline</span>
            <span>→</span>
            <span>Calibrated Decision Support</span>
          </div>
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

      {/* Farmer View: Evidence-Based Decision Support Alert (Rule 6) */}
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
                    : '🟠 Potential Crop Stress Anomaly'}
              </span>
              <span className="text-xs font-semibold text-slate-700">Near-Real-Time Decision Support</span>
            </div>
            <span className="text-xs text-slate-500">
              {analysisDate ? `Observation Date: ${analysisDate}` : 'Latest available capture'}
            </span>
          </div>

          <div className="mt-2 text-sm text-slate-800">
            {(summary.ndvi?.mean ?? 0.5) >= 0.60 ? (
              <p>
                <strong>Agronomic Guidance:</strong> Field vegetation demonstrates healthy canopy reflectance and normal moisture metrics for the current phenological growth stage. Maintain scheduled farm practices.
              </p>
            ) : (summary.ndvi?.mean ?? 0.5) >= 0.40 ? (
              <p>
                <strong>Agronomic Guidance:</strong> Canopy growth is moderate. Review red-edge (NDRE) and moisture indicators. Inspect field for subtle nutrient or moisture variations.
              </p>
            ) : (
              <p>
                <strong>Evidence-Based Alert:</strong> Vegetation indices show a persistent anomaly relative to the field baseline. Pattern is consistent with water or chlorophyll deficit. <em>Action: Verify root-zone soil moisture and crop condition before scheduling irrigation.</em>
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
            <span className="text-slate-600">Export 10m grid observation datasets with raw bands, 20m resampled indices, and Landsat 30m LST</span>
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

      {/* Summary cards with Resolution Provenance */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-6">
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">Analysis Grids (10m)</div>
          <div className="text-xl font-bold">{summary.grid_count}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">NDVI (10m Native)</div>
          <div className="text-xl font-bold">{summary.ndvi ? summary.ndvi.mean.toFixed(3) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">NDMI (20m Resampled)</div>
          <div className="text-xl font-bold">{summary.ndmi ? summary.ndmi.mean.toFixed(3) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">NDRE (20m Resampled)</div>
          <div className="text-xl font-bold">{summary.ndre ? summary.ndre.mean.toFixed(3) : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">Landsat LST (30m)</div>
          <div className="text-xl font-bold">{summary.lst ? `${summary.lst.mean.toFixed(1)}°C` : '–'}</div>
        </div>
        <div className="rounded-lg border bg-white p-3">
          <div className="text-xs font-medium text-gray-500">Stress Probability</div>
          <div className="text-xl font-bold">{summary.risk ? `${(summary.risk.mean * 100).toFixed(1)}%` : '–'}</div>
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
            <div className="rounded-lg border overflow-hidden bg-white shadow-xs">
              {/* Scientific Metric Switcher */}
              <div className="flex flex-wrap gap-1 bg-slate-100 p-2 border-b">
                {[
                  { id: 'ndvi', label: 'NDVI (10m)' },
                  { id: 'ndmi', label: 'NDMI (20m)' },
                  { id: 'ndre', label: 'NDRE (20m)' },
                  { id: 'evi', label: 'EVI (10m)' },
                  { id: 'savi', label: 'SAVI (10m)' },
                  { id: 'gci', label: 'GCI (20m)' },
                  { id: 'lst', label: 'Landsat LST (30m)' },
                  { id: 'sar', label: 'SAR (10m)' },
                  { id: 'stress_prob', label: 'Stress Prob' },
                  { id: 'uncertainty', label: 'QA / Uncertainty' },
                ].map(item => (
                  <button
                    key={item.id}
                    className={`rounded px-2.5 py-1 text-xs font-bold transition ${
                      colorMode === item.id
                        ? 'bg-emerald-600 text-white shadow-xs'
                        : 'bg-white text-slate-700 hover:bg-slate-200'
                    }`}
                    onClick={() => setColorMode(item.id as ColorMode)}
                  >
                    {item.label}
                  </button>
                ))}
              </div>

              {/* Basemap & Opacity Toolbar */}
              <div className="flex flex-wrap items-center justify-between gap-3 bg-slate-50 px-3 py-2 border-b text-xs">
                <div className="flex flex-wrap items-center gap-1.5">
                  <span className="font-semibold text-slate-700">Basemap:</span>
                  {[
                    { id: 's2-tci', label: '🛰️ Sentinel-2 True Color (Real-Time)', desc: 'Actual satellite optical pass matching date' },
                    { id: 'google-hybrid', label: '🌍 Google Hybrid (0.3m)', desc: 'High-res aerial with field bunds' },
                    { id: 'esri', label: '🗺️ Esri Satellite', desc: 'Global aerial imagery' },
                  ].map(b => (
                    <button
                      key={b.id}
                      type="button"
                      title={b.desc}
                      onClick={() => setBasemap(b.id as BasemapKey)}
                      className={`rounded-md px-2 py-1 text-[11px] font-medium transition ${
                        basemap === b.id
                          ? 'bg-indigo-600 text-white shadow-xs'
                          : 'bg-white text-slate-700 border border-slate-200 hover:bg-slate-100'
                      }`}
                    >
                      {b.label}
                    </button>
                  ))}
                </div>

                <div className="flex items-center gap-3">
                  <button
                    type="button"
                    onClick={() => setRecenterCount((c) => c + 1)}
                    className="flex items-center gap-1.5 rounded-md bg-emerald-700 px-2.5 py-1 text-[11px] font-bold text-white shadow-xs hover:bg-emerald-800 transition"
                    title="Fly map directly back to your registered field"
                  >
                    🎯 Locate My Field
                  </button>

                  <div className="flex items-center gap-2 border-l pl-3 border-slate-200">
                    <span className="font-medium text-slate-600 text-[11px]">Grid Opacity:</span>
                    <input
                      type="range"
                      min="0"
                      max="100"
                      value={Math.round(gridOpacity * 100)}
                      onChange={(e) => setGridOpacity(Number(e.target.value) / 100)}
                      className="h-1.5 w-20 cursor-pointer accent-emerald-600"
                      title="Slide to fade between 10m stress grid and raw satellite photograph"
                    />
                    <span className="font-mono text-[11px] text-slate-700 w-8">{Math.round(gridOpacity * 100)}%</span>
                  </div>
                </div>
              </div>

              {/* STAC / Scene Real-Time Info Badge */}
              {basemap === 's2-tci' && (
                <div className="flex items-center justify-between bg-emerald-50/80 px-3 py-1 border-b text-[11px] text-emerald-800">
                  <div className="flex items-center gap-1.5">
                    <span className="h-2 w-2 rounded-full bg-emerald-500 animate-ping" />
                    <span><strong>Active Optical Pass:</strong> {analysisDate || 'Latest'} Sentinel-2 L2A (10 m Native RGB)</span>
                  </div>
                  {stacItemId && (
                    <span className="font-mono text-[10px] text-emerald-700 truncate max-w-xs" title={stacItemId}>
                      Scene: {stacItemId}
                    </span>
                  )}
                </div>
              )}

              <div style={{ height: '55vh' }}>
                <MapContainer center={mapCenter} zoom={16} style={{ height: '100%', width: '100%' }}>
                  <AutoFitMapBounds geometry={land.geometry} triggerCount={recenterCount} />

                  {basemap === 's2-tci' && (
                    <TileLayer
                      key={`s2-tci-${stacItemId || analysisDate || 'default'}`}
                      url={
                        stacItemId
                          ? `https://planetarycomputer.microsoft.com/api/data/v1/item/tiles/WebMercatorQuad/{z}/{x}/{y}@2x?collection=sentinel-2-l2a&item=${stacItemId}&assets=visual`
                          : "https://tiles.maps.eox.at/wmts/1.0.0/s2cloudless-2020_3857/default/g/{z}/{y}/{x}.jpg"
                      }
                      attribution="&copy; Copernicus Sentinel-2 L2A True Color (Real-Time Overpass)"
                      maxZoom={19}
                    />
                  )}
                  {basemap === 'google-hybrid' && (
                    <TileLayer
                      key="google-hybrid"
                      url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}"
                      attribution="&copy; Google Maps Satellite + Labels"
                      maxZoom={20}
                    />
                  )}
                  {basemap === 'esri' && (
                    <TileLayer
                      key="esri"
                      url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                      attribution="Tiles &copy; Esri World Imagery"
                      maxZoom={19}
                    />
                  )}

                  {land.geometry && (
                    <GeoJSON
                      data={{ type: 'Feature', properties: {}, geometry: land.geometry } as any}
                      style={() => ({ weight: 2.5, fillOpacity: 0.0, color: '#ffffff', dashArray: '4, 4' })}
                    />
                  )}
                  {data.grids && data.grids.features.length > 0 && (
                    <GeoJSON
                      key={`${colorMode}-${latestWeather?.date ?? 'no-weather'}-${gridOpacity}`}
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
                          p.ndre != null ? `NDRE: ${p.ndre.toFixed(3)}` : '',
                          p.lst_c != null ? `LST: ${p.lst_c.toFixed(1)}°C` : '',
                          latestT2m != null ? `Temp: ${latestT2m.toFixed(1)}°C` : '',
                          p.risk != null ? `Stress Prob: ${(p.risk * 100).toFixed(1)}%` : '',
                        ].filter(Boolean)
                        layer.bindPopup(lines.join('<br/>'))
                      }}
                    />
                  )}
                </MapContainer>
              </div>
            </div>

            <ScientificLegend
              activeIndex={colorMode}
              onOpenTraceability={setTraceModalKey}
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

