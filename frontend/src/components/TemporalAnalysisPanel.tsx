import React, { useEffect, useState } from 'react'
import axios from 'axios'
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

type DashboardMode = 'latest' | 'select'
type TemporalMode = 'strict' | 'smart'

type TemporalPoint = {
  date: string
  value: number | null
  sample_count?: number | null
  t2m?: number | null
  rh2m?: number | null
  prectotcorr?: number | null
  distance_days?: number | null
}

type TemporalSourceDates = {
  sentinel?: string | null
  nasa?: string | null
  modis?: string | null
  [key: string]: string | null | undefined
}

type TemporalMetric = {
  key: 'ndvi' | 'ndmi' | 'lst' | 'vpd'
  label: string
  source: string
  unit: string
  digits: number
  status: 'ready' | 'missing_reference' | 'missing_comparison'
  message: string | null
  reference: TemporalPoint | null
  comparison: TemporalPoint | null
  change: {
    absolute: number | null
    percent: number | null
  } | null
  trend: {
    baseline_date: string | null
    baseline_value: number | null
    delta: number | null
    percent: number | null
    direction: 'up' | 'down' | 'flat' | null
    label: string
  } | null
  history: TemporalPoint[]
}

type TemporalAnalysisResponse = {
  land_id: number
  reference_date: string | null
  comparison_date: string | null
  analysis_mode: 'historical' | 'comparison'
  history_window_days: number
  comparison_tolerance_days: number
  confidence: {
    available_metrics: number
    total_metrics: number
    label: 'High' | 'Medium' | 'Low' | 'None'
  }
  metrics: TemporalMetric[]
  warnings: string[]
  source_dates?: TemporalSourceDates
  metric_source_dates?: Record<string, string | null>
  comparison_source_dates?: TemporalSourceDates
  status?: 'ok' | 'no_data'
  mode?: TemporalMode
  note?: string | null
  message?: string | null
  data?: TemporalAnalysisResponse | null
}

type TemporalAnalysisPanelProps = {
  landId: string
  activeDate: string | null
  mode: DashboardMode
}

function formatValue(value: number | null, digits: number) {
  return value === null || !Number.isFinite(value) ? 'Unavailable' : value.toFixed(digits)
}

function formatPercent(value: number | null) {
  return value === null || !Number.isFinite(value) ? 'Unavailable' : `${value > 0 ? '+' : ''}${value.toFixed(1)}%`
}

function trendArrow(direction: 'up' | 'down' | 'flat' | null) {
  if (direction === 'up') return 'up'
  if (direction === 'down') return 'down'
  if (direction === 'flat') return 'flat'
  return '-'
}

function statusCopy(metric: TemporalMetric) {
  if (metric.status === 'missing_reference') {
    return metric.message || 'No reference data available.'
  }
  if (metric.status === 'missing_comparison') {
    return metric.message || 'No comparison data available.'
  }
  return 'Ready'
}

function confidenceClass(label: string) {
  if (label === 'High') return 'bg-emerald-100 text-emerald-800 border-emerald-200'
  if (label === 'Medium') return 'bg-amber-100 text-amber-800 border-amber-200'
  if (label === 'Low') return 'bg-rose-100 text-rose-800 border-rose-200'
  return 'bg-slate-100 text-slate-700 border-slate-200'
}

function sourceModeLabel(sourceDate: string | null | undefined, activeDate: string | null): string {
  if (!sourceDate) return 'Not Available'
  if (activeDate && sourceDate === activeDate) return 'Exact'
  return 'Nearest'
}

function sourceDisplayLabel(sourceName: string, sourceDate: string | null | undefined, activeDate: string | null) {
  const label = sourceModeLabel(sourceDate, activeDate)
  return `${sourceName} → ${sourceDate || 'Not Available'}${label === 'Not Available' ? '' : ` (${label})`}`
}

function metricTone(metric: TemporalMetric) {
  if (metric.key === 'ndvi') return 'text-emerald-700'
  if (metric.key === 'ndmi') return 'text-sky-700'
  if (metric.key === 'lst') return 'text-rose-700'
  return 'text-amber-700'
}

function MetricSparkline({ metric }: { metric: TemporalMetric }) {
  const history = Array.isArray(metric.history) ? metric.history : []
  const data = history
    .filter((point) => point && point.value !== null)
    .map((point) => ({
      date: point.date,
      label: point.date.slice(5),
      value: point.value,
    }))

  if (data.length === 0) {
    return <div className="mt-3 rounded-md border border-dashed border-slate-200 bg-slate-50 px-3 py-5 text-xs text-slate-500">No historical series available for this metric.</div>
  }

  return (
    <div className="mt-3 h-20 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 6, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="label" hide />
          <YAxis hide domain={['auto', 'auto']} />
          <Tooltip
            formatter={(value: number | string | Array<number | string>) => {
              const numericValue = Array.isArray(value) ? Number(value[0]) : Number(value)
              return [formatValue(Number.isFinite(numericValue) ? numericValue : null, metric.digits), metric.label]
            }}
            labelFormatter={(label) => `Date: ${label}`}
          />
          <ReferenceLine
            x={metric.reference?.date?.slice(5)}
            stroke="#0f172a"
            strokeDasharray="4 4"
            ifOverflow="extendDomain"
          />
          <Line type="monotone" dataKey="value" stroke={metric.key === 'ndvi' ? '#16a34a' : metric.key === 'ndmi' ? '#0f766e' : metric.key === 'lst' ? '#dc2626' : '#d97706'} strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function MetricCard({ metric }: { metric: TemporalMetric }) {
  const safeMetric = metric || {
    key: 'ndvi' as const,
    label: 'Unavailable',
    source: 'Unavailable',
    unit: 'index',
    digits: 3,
    status: 'missing_reference' as const,
    message: 'Metric payload is unavailable.',
    reference: null,
    comparison: null,
    change: null,
    trend: null,
    history: [],
  }
  const referenceValue = safeMetric.reference?.value ?? null
  const comparisonValue = safeMetric.comparison?.value ?? null
  const hasComparison = safeMetric.comparison?.value !== null && safeMetric.comparison?.value !== undefined
  const changeArrow = safeMetric.change?.absolute !== null && safeMetric.change?.absolute !== undefined
    ? safeMetric.change.absolute > 0
      ? 'up'
      : safeMetric.change.absolute < 0
        ? 'down'
        : 'flat'
    : '-'

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-base font-semibold text-slate-900">{safeMetric.label}</div>
          <div className="text-xs uppercase tracking-wide text-slate-500">{safeMetric.source}</div>
        </div>
        <div className={`rounded-full border px-2.5 py-1 text-xs font-semibold ${safeMetric.status === 'ready' ? 'border-emerald-200 bg-emerald-50 text-emerald-700' : 'border-amber-200 bg-amber-50 text-amber-700'}`}>
          {safeMetric.status === 'ready' ? 'Ready' : 'Unavailable'}
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div className="rounded-lg bg-slate-50 px-3 py-2">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Reference</div>
          <div className="mt-1 text-lg font-semibold text-slate-900">{formatValue(referenceValue, safeMetric.digits)}</div>
          <div className="text-xs text-slate-500">{safeMetric.reference?.date || 'Unavailable'}</div>
        </div>
        <div className="rounded-lg bg-slate-50 px-3 py-2">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Compared To</div>
          <div className="mt-1 text-lg font-semibold text-slate-900">{formatValue(comparisonValue, safeMetric.digits)}</div>
          <div className="text-xs text-slate-500">{safeMetric.comparison?.date ? `${safeMetric.comparison.date}${safeMetric.comparison.distance_days !== null && safeMetric.comparison.distance_days !== undefined ? ` (nearest: ${safeMetric.comparison.distance_days} day${safeMetric.comparison.distance_days === 1 ? '' : 's'})` : ''}` : 'No comparison date'}</div>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2 text-sm">
        <span className="rounded-md bg-slate-100 px-2 py-1 font-medium text-slate-700">
          {hasComparison ? `Change: ${changeArrow} ${formatValue(safeMetric.change?.absolute ?? null, safeMetric.digits)}` : `Trend delta: ${trendArrow(safeMetric.trend?.direction ?? null)} ${safeMetric.trend ? formatValue(safeMetric.trend.delta ?? null, safeMetric.digits) : 'Unavailable'}`}
        </span>
        <span className={`rounded-md px-2 py-1 font-medium ${metricTone(safeMetric)} bg-slate-50`}>
          {hasComparison ? `Change %: ${formatPercent(safeMetric.change?.percent ?? null)}` : `Trend %: ${safeMetric.trend ? formatPercent(safeMetric.trend.percent ?? null) : 'Unavailable'}`}
        </span>
        {safeMetric.trend ? (
          <span className="rounded-md bg-slate-100 px-2 py-1 font-medium text-slate-700">
            Trend: {trendArrow(safeMetric.trend.direction)} {safeMetric.trend.label}
          </span>
        ) : null}
      </div>

      <MetricSparkline metric={safeMetric} />

      <div className="mt-3 text-xs text-slate-500">Status: {statusCopy(safeMetric)}</div>
    </div>
  )
}

export default function TemporalAnalysisPanel({ landId, activeDate, mode: dashboardMode }: TemporalAnalysisPanelProps) {
  const [analysisMode, setAnalysisMode] = useState<TemporalMode>('strict')
  const [comparisonDate, setComparisonDate] = useState('')
  const [analysisEnvelope, setAnalysisEnvelope] = useState<TemporalAnalysisResponse | null>(null)
  const [analysis, setAnalysis] = useState<TemporalAnalysisResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [statusMessage, setStatusMessage] = useState<string | null>(null)

  const fetchAnalysis = async (comparison: string, requestedMode: TemporalMode = analysisMode) => {
    if (!activeDate) {
      setError('Reference date is unavailable until the dashboard finishes loading.')
      return
    }

    setLoading(true)
    setError(null)
    setStatusMessage(null)
    try {
      const response = await axios.get<TemporalAnalysisResponse>(`/temporal-analysis/${landId}`, {
        params: {
          active_date: activeDate,
          comparison_date: comparison || undefined,
          mode: requestedMode,
        },
      })
      const envelope = response.data
      setAnalysisEnvelope(envelope)
      if (envelope.status === 'no_data') {
        setAnalysis(null)
        setStatusMessage(envelope.message || 'No data available for selected date')
        return
      }

      const payload = envelope.data && typeof envelope.data === 'object' ? envelope.data : envelope
      setAnalysis(payload)
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'Temporal analysis failed')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (!activeDate) {
      return
    }
    void fetchAnalysis('')
  }, [activeDate, landId])

  const handleModeChange = (nextMode: TemporalMode) => {
    setAnalysisMode(nextMode)
    setAnalysisEnvelope(null)
    setAnalysis(null)
    setStatusMessage(null)
    void fetchAnalysis(comparisonDate, nextMode)
  }

  const confidence = analysis?.confidence
  const referenceLabel = dashboardMode === 'latest' ? 'AUTO' : 'SELECTED DATE'
  const comparisonLabel = comparisonDate || 'Not selected'
  const noComparisonMode = !comparisonDate
  const metrics = Array.isArray(analysis?.metrics) ? analysis.metrics : []
  const sourceDates = analysisEnvelope?.source_dates || analysis?.source_dates
  const smartModeActive = analysisEnvelope?.mode === 'smart' || analysisMode === 'smart'

  return (
    <section className="space-y-4">
      <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 className="text-2xl font-semibold text-slate-900">Temporal Analysis</h2>
            <p className="mt-1 max-w-3xl text-sm text-slate-600">
              Read-only temporal comparison anchored to the dashboard active date. All comparisons are sourced from persisted Sentinel-2, MODIS, and NASA POWER records only.
            </p>
          </div>

          <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <div className="font-semibold text-slate-800">Reference Date</div>
            <div>{activeDate || 'Unavailable'} ({referenceLabel})</div>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Temporal Mode</div>
          <div className="inline-flex rounded-full border border-slate-200 bg-slate-100 p-1">
            <button
              type="button"
              className={`rounded-full px-3 py-1 text-sm font-semibold transition ${analysisMode === 'strict' ? 'bg-slate-900 text-white shadow-sm' : 'text-slate-600 hover:text-slate-900'}`}
              onClick={() => handleModeChange('strict')}
            >
              Strict Mode
            </button>
            <button
              type="button"
              className={`rounded-full px-3 py-1 text-sm font-semibold transition ${analysisMode === 'smart' ? 'bg-slate-900 text-white shadow-sm' : 'text-slate-600 hover:text-slate-900'}`}
              onClick={() => handleModeChange('smart')}
            >
              Smart Mode
            </button>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="rounded-lg border border-slate-200 bg-slate-50 p-4 lg:col-span-2">
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div>
                <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Compare With Date</div>
                <div className="mt-3 flex flex-col gap-2">
                  <input
                    type="date"
                    className="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm"
                    value={comparisonDate}
                    max={activeDate || undefined}
                    onChange={(event) => setComparisonDate(event.target.value)}
                  />
                  <div className="text-xs text-slate-500">
                    Leave empty to run historical trend analysis against the active date window.
                  </div>
                </div>
              </div>

              <div>
                <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Analysis State</div>
                <div className="mt-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
                  <div>Comparison date: {comparisonLabel}</div>
                  <div className="text-xs text-slate-500">
                    {analysisMode === 'smart'
                      ? `Nearest matches are selected independently per source within +/- ${analysisEnvelope?.comparison_tolerance_days || 5} days.`
                      : 'Strict mode uses exact dates only and does not fall back to nearby data.'}
                  </div>
                </div>
              </div>
            </div>

            <button
              type="button"
              className="mt-4 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
              disabled={loading || !activeDate}
              onClick={() => void fetchAnalysis(comparisonDate)}
            >
              {loading ? 'Analyzing...' : comparisonDate ? 'Compare Dates' : 'Run Historical Trend'}
            </button>
          </div>

          <div className="rounded-lg border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm font-semibold uppercase tracking-wide text-slate-500">Confidence</div>
            <div className="mt-3 flex items-baseline gap-2">
              <span className={`rounded-full border px-3 py-1 text-sm font-semibold ${confidenceClass(confidence?.label || 'None')}`}>
                {confidence?.label || 'None'}
              </span>
              <span className="text-sm text-slate-600">
                {confidence ? `${confidence.available_metrics}/${confidence.total_metrics}` : '0/0'}
              </span>
            </div>
            <p className="mt-3 text-sm text-slate-600">
              Confidence reflects how many metric cards could be resolved from persisted data for the current request.
            </p>
          </div>
        </div>

        {analysisEnvelope?.status === 'no_data' || statusMessage ? (
          <div className="mt-4 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
            {statusMessage || analysisEnvelope?.message || 'No data available for selected date'}
          </div>
        ) : null}

        {smartModeActive ? (
          <div className="mt-4 space-y-3">
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
              ⚠ Using nearest available data within the configured tolerance window.
            </div>
            <div className="rounded-lg border border-sky-200 bg-sky-50 p-3 text-sm text-sky-800">
              Availability panel shows exact-date data only. Current view uses nearest available data.
            </div>
          </div>
        ) : null}

        {sourceDates ? (
          <div className="mt-4 rounded-lg border border-slate-200 bg-white p-3 text-sm text-slate-700">
            <div className="font-semibold text-slate-800">Data Sources Used</div>
            <div className="mt-2 grid grid-cols-1 gap-1 sm:grid-cols-3">
              <div>{sourceDisplayLabel('Sentinel-2', sourceDates.sentinel, activeDate)}</div>
              <div>{sourceDisplayLabel('NASA POWER', sourceDates.nasa, activeDate)}</div>
              <div>{sourceDisplayLabel('MODIS', sourceDates.modis, activeDate)}</div>
            </div>
            {analysisEnvelope?.comparison_source_dates ? (
              <div className="mt-3 text-xs text-slate-500">
                Comparison source dates - {sourceDisplayLabel('Sentinel-2', analysisEnvelope.comparison_source_dates.sentinel, analysis?.comparison_date || activeDate)}; {sourceDisplayLabel('NASA POWER', analysisEnvelope.comparison_source_dates.nasa, analysis?.comparison_date || activeDate)}; {sourceDisplayLabel('MODIS', analysisEnvelope.comparison_source_dates.modis, analysis?.comparison_date || activeDate)}
              </div>
            ) : null}
          </div>
        ) : null}

        {analysis?.warnings?.length ? (
          <div className="mt-4 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-600">
            {analysis.warnings.map((warning) => (
              <div key={warning}>{warning}</div>
            ))}
          </div>
        ) : null}

        {error ? (
          <div className="mt-4 rounded-lg border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
            {error}
          </div>
        ) : null}

        {analysis ? (
          <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
            <div className="font-semibold text-slate-800">Analysis Window</div>
            <div>Reference Date: {analysis.reference_date || 'Unavailable'}</div>
            <div>
              {analysis.analysis_mode === 'comparison' && analysis.comparison_date
                ? `Comparison Date: ${analysis.comparison_date}`
                : `Historical Trend: last ${analysis.history_window_days} days`}
            </div>
          </div>
        ) : null}
      </div>

      {analysis ? (
        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          {metrics.map((metric) => (
            <MetricCard key={metric.key} metric={metric} />
          ))}
        </div>
      ) : (
        <div className="rounded-xl border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500">
          {loading
            ? 'Loading temporal analysis...'
            : statusMessage
              ? statusMessage
              : noComparisonMode
                ? 'Temporal analysis will show a historical trend once the active date is available.'
                : 'Choose a comparison date and run the analysis.'}
        </div>
      )}
    </section>
  )
}