import React, { useEffect, useMemo, useState } from 'react'
import axios from 'axios'

type DashboardMode = 'latest' | 'select'

type AvailabilitySourceState = {
  available: boolean
  reason: string | null
  [key: string]: any
}

type AvailabilityResponse = {
  available: boolean
  selected_date: string
  future_date?: boolean
  missing_sources: string[]
  sources: Record<string, AvailabilitySourceState>
  title?: string | null
  message?: string | null
  cloud_threshold_pct?: number
}

type AvailableDataPanelProps = {
  landId: string
  landGeometry: any
  latestDate: string | null
  mode: DashboardMode
  selectedDate: string | null
  activeDataDate: string | null
  processingStatus: string
  onRefresh: () => Promise<void> | void
}

function toFiniteNumber(value: unknown): number | null {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function getCentroid(geometry: any): { lon: number | null; lat: number | null } {
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

  if (count === 0) {
    return { lon: null, lat: null }
  }

  return {
    lon: lonSum / count,
    lat: latSum / count,
  }
}

export default function AvailableDataPanel({
  landId,
  landGeometry,
  latestDate,
  mode,
  selectedDate,
  activeDataDate,
  processingStatus,
  onRefresh,
}: AvailableDataPanelProps) {
  const [draftMode, setDraftMode] = useState<DashboardMode>(mode)
  const [draftDate, setDraftDate] = useState<string>(selectedDate || latestDate || '')
  const [availability, setAvailability] = useState<AvailabilityResponse | null>(null)
  const [checking, setChecking] = useState(false)
  const [runningAnalysis, setRunningAnalysis] = useState(false)
  const [runningLatest, setRunningLatest] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [modalData, setModalData] = useState<AvailabilityResponse | null>(null)

  const isProcessing = processingStatus === 'running' || processingStatus === 'queued'
  const centroid = useMemo(() => getCentroid(landGeometry), [landGeometry])

  useEffect(() => {
    if (checking || runningAnalysis || runningLatest || isProcessing) {
      return
    }
    setDraftMode(mode)
    setDraftDate(selectedDate || latestDate || '')
    setAvailability(null)
    setError(null)
  }, [mode, selectedDate, latestDate, checking, runningAnalysis, runningLatest, isProcessing])

  const isFutureDate = useMemo(() => {
    if (!draftDate) return false
    return draftDate > new Date().toISOString().slice(0, 10)
  }, [draftDate])

  const checkAvailability = async () => {
    if (draftMode !== 'select') {
      return
    }
    if (!draftDate) {
      setError('Choose a date before checking availability.')
      return
    }
    if (isFutureDate) {
      setError('Future dates are blocked.')
      return
    }

    setChecking(true)
    setError(null)
    try {
      const response = await axios.get<AvailabilityResponse>(`/dashboard/${landId}/availability`, {
        params: { date: draftDate },
      })
      setAvailability(response.data)
      if (!response.data.available) {
        setModalData(response.data)
      }
    } catch (err: any) {
      const detail = err?.response?.data?.detail || err?.response?.data?.message || err.message || 'Availability check failed'
      setError(detail)
    } finally {
      setChecking(false)
    }
  }

  const runAnalysis = async () => {
    if (draftMode !== 'select' || !draftDate || !availability?.available) {
      return
    }

    setRunningAnalysis(true)
    setError(null)
    try {
      await axios.post(`/dashboard/${landId}/process-selected`, { date: draftDate })
      await onRefresh()
    } catch (err: any) {
      const detail = err?.response?.data?.detail
      if (detail && typeof detail === 'object' && detail.available === false) {
        setModalData(detail)
      } else {
        setError(typeof detail === 'string' ? detail : err.message || 'Exact-date analysis failed')
      }
    } finally {
      setRunningAnalysis(false)
    }
  }

  const returnToLatest = async () => {
    setRunningLatest(true)
    setError(null)
    try {
      await axios.post(`/dashboard/${landId}/process`)
      await onRefresh()
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Failed to return to latest data')
    } finally {
      setRunningLatest(false)
    }
  }

  const missingSources = modalData?.missing_sources || []
  const activeDateLabel = mode === 'select' && activeDataDate ? `${activeDataDate} (User selected)` : activeDataDate || latestDate || 'Unavailable'

  return (
    <section className="space-y-4">
      <div className="rounded-lg border bg-white p-4 shadow-sm">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 className="text-2xl font-semibold text-slate-900">Available Data</h2>
            <p className="mt-1 max-w-2xl text-sm text-slate-600">
              Check official source availability for Sentinel-2, MODIS, and NASA POWER before running analysis.
            </p>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-medium text-slate-800">Current analysis date</div>
            <div>{activeDateLabel}</div>
            <div className="text-xs text-slate-500">Mode: {mode}</div>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 lg:col-span-2">
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div>
                <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Mode</div>
                <div className="mt-3 flex flex-wrap gap-4 text-sm text-slate-700">
                  <label className="inline-flex items-center gap-2">
                    <input
                      type="radio"
                      name={`mode-${landId}`}
                      checked={draftMode === 'latest'}
                      onChange={() => {
                        setDraftMode('latest')
                        setError(null)
                        setAvailability(null)
                        setModalData(null)
                      }}
                    />
                    Latest Available
                  </label>
                  <label className="inline-flex items-center gap-2">
                    <input
                      type="radio"
                      name={`mode-${landId}`}
                      checked={draftMode === 'select'}
                      onChange={() => {
                        setDraftMode('select')
                        setError(null)
                        setAvailability(null)
                        setModalData(null)
                        setDraftDate(selectedDate || latestDate || new Date().toISOString().slice(0, 10))
                      }}
                    />
                    Select Date
                  </label>
                </div>
              </div>

              <div>
                <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Date</div>
                <div className="mt-3 flex flex-col gap-2">
                  <input
                    type="date"
                    className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm disabled:bg-slate-100"
                    value={draftDate}
                    max={new Date().toISOString().slice(0, 10)}
                    disabled={draftMode !== 'select'}
                    onChange={(event) => {
                      setDraftDate(event.target.value)
                      setAvailability(null)
                      setModalData(null)
                      setError(null)
                    }}
                  />
                  <div className="text-xs text-slate-500">
                    Date picker is enabled only in select mode.
                  </div>
                </div>
              </div>
            </div>

            {centroid.lat !== null && centroid.lon !== null && (
              <div className="mt-4 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
                Location: {centroid.lat.toFixed(6)}, {centroid.lon.toFixed(6)}
              </div>
            )}
          </div>

          <div className="rounded-lg border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Actions</div>
            <div className="mt-3 space-y-2">
              <button
                type="button"
                className="w-full rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
                disabled={draftMode !== 'select' || checking || isProcessing || !draftDate || isFutureDate}
                onClick={checkAvailability}
              >
                {checking ? 'Checking...' : 'Check Availability'}
              </button>
              <button
                type="button"
                className="w-full rounded-md bg-emerald-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-emerald-700 disabled:cursor-not-allowed disabled:opacity-50"
                disabled={draftMode !== 'select' || !availability?.available || runningAnalysis || isProcessing}
                onClick={runAnalysis}
              >
                {runningAnalysis ? 'Running...' : 'Run Analysis'}
              </button>
              {mode === 'select' && (
                <button
                  type="button"
                  className="w-full rounded-md bg-blue-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={runningLatest || isProcessing}
                  onClick={returnToLatest}
                >
                  {runningLatest ? 'Returning...' : 'Return to Latest Data'}
                </button>
              )}
            </div>
          </div>
        </div>

        {availability?.available && draftMode === 'select' && (
          <div className="mt-4 rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900">
            Exact-date availability confirmed for {draftDate}. All three sources are ready.
          </div>
        )}

        {error && (
          <div className="mt-4 rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
            {error}
          </div>
        )}

        <div className="mt-4 rounded-lg border border-slate-200 bg-white p-4">
          <div className="text-sm font-semibold text-slate-800">State summary</div>
          <div className="mt-2 grid grid-cols-1 gap-2 text-sm text-slate-700 md:grid-cols-3">
            <div>Persisted mode: {mode}</div>
            <div>Selected date: {selectedDate || '-'}</div>
            <div>Active date: {activeDataDate || latestDate || '-'}</div>
          </div>
          <div className="mt-2 text-xs text-slate-500">
            Processing status: {processingStatus}
          </div>
        </div>
      </div>

      {modalData && !modalData.available && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4">
          <div className="w-full max-w-lg rounded-xl bg-white p-6 shadow-2xl">
            <div className="text-lg font-semibold text-slate-900">Data Not Available</div>
            <p className="mt-2 text-sm text-slate-600">
              Complete dataset not found for selected date: {modalData.selected_date}.
            </p>
            <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
              <div className="font-semibold text-slate-800">Missing:</div>
              <div className="mt-2 flex flex-wrap gap-2">
                {missingSources.length > 0 ? missingSources.map((source) => (
                  <span key={source} className="rounded-full bg-red-100 px-3 py-1 text-red-800">
                    {source}
                  </span>
                )) : <span className="text-slate-500">Unavailable</span>}
              </div>
              <div className="mt-3 text-xs text-slate-500">
                Please select another date or use Latest mode.
              </div>
            </div>
            <div className="mt-5 flex justify-end">
              <button
                type="button"
                className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800"
                onClick={() => setModalData(null)}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}
