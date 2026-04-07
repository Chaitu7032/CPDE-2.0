import React from 'react'

const DEFAULT_SCALES = {
  ndvi: [
    { range: '< 0.2', label: 'Severe stress', color: '#7f1d1d' },
    { range: '0.2 - 0.4', label: 'Stressed', color: '#f97316' },
    { range: '0.4 - 0.6', label: 'Moderate', color: '#facc15' },
    { range: '>= 0.6', label: 'Healthy', color: '#16a34a' },
  ],
  ndmi: [
    { range: '< -0.1', label: 'Dry', color: '#dc2626' },
    { range: '-0.1 - 0', label: 'Slightly dry', color: '#f59e0b' },
    { range: '0 - 0.2', label: 'Moderate', color: '#7dd3fc' },
    { range: '>= 0.2', label: 'Wet', color: '#2563eb' },
  ],
  lst: [
    { range: '< 25', label: 'Cool', color: '#2563eb' },
    { range: '25 - 30', label: 'Normal', color: '#16a34a' },
    { range: '30 - 35', label: 'Warm', color: '#f59e0b' },
    { range: '>= 35', label: 'Hot stress', color: '#dc2626' },
  ],
}

function getNDVIStatus(v) {
  if (v < 0.2) return { label: 'Severe stress', color: '#7f1d1d' }
  if (v < 0.4) return { label: 'Stressed', color: '#f97316' }
  if (v < 0.6) return { label: 'Moderate', color: '#facc15' }
  return { label: 'Healthy', color: '#16a34a' }
}

function getNDMIStatus(v) {
  if (v < -0.1) return { label: 'Dry', color: '#dc2626' }
  if (v < 0.0) return { label: 'Slightly dry', color: '#f59e0b' }
  if (v < 0.2) return { label: 'Moderate', color: '#7dd3fc' }
  return { label: 'Wet', color: '#2563eb' }
}

function getLSTStatus(v) {
  if (v < 25.0) return { label: 'Cool', color: '#2563eb' }
  if (v < 30.0) return { label: 'Normal', color: '#16a34a' }
  if (v < 35.0) return { label: 'Warm', color: '#f59e0b' }
  return { label: 'Hot stress', color: '#dc2626' }
}

function isFiniteNumber(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

function renderCurrentValue({ label, value, classifier, formatter }) {
  if (!isFiniteNumber(value)) {
    return (
      <div className="flex items-center justify-between rounded-md bg-gray-50 px-3 py-2">
        <span className="font-medium text-gray-700">{label}</span>
        <span className="text-sm text-gray-500">Data unavailable</span>
      </div>
    )
  }

  const status = classifier(value)
  return (
    <div className="flex flex-wrap items-center justify-between gap-2 rounded-md bg-gray-50 px-3 py-2">
      <span className="font-medium text-gray-700">{label}: {formatter(value)}</span>
      <span className="text-sm font-semibold" style={{ color: status.color }}>
        {status.label}
      </span>
    </div>
  )
}

function renderScaleCard(title, scale, unit = '') {
  return (
    <div className="rounded-md border p-3">
      <div className="mb-2 text-sm font-semibold text-gray-800">{title}</div>
      <div className="space-y-1 text-sm text-gray-700">
        {scale.map((step) => (
          <div key={`${title}-${step.range}`} className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className="inline-block h-3 w-3 rounded-sm" style={{ backgroundColor: step.color }} />
              <span>{step.range}{unit}</span>
            </div>
            <span className="text-xs font-medium text-gray-600">{step.label}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

/**
 * @param {{ ndvi?: number | null, ndmi?: number | null, lst?: number | null, risk?: number | null, colorScales?: any }} props
 */
export default function ScientificLegend({ ndvi, ndmi, lst, risk, colorScales }) {
  const scales = colorScales || DEFAULT_SCALES
  return (
    <div className="mt-4 rounded-lg border bg-white p-4">
      <div className="text-base font-semibold text-gray-800">Scientific Interpretation</div>

      <div className="mt-3">
        <div className="text-sm font-semibold text-gray-800">Current Mean Values</div>
        <div className="mt-2 space-y-2 text-sm">
          {renderCurrentValue({
            label: 'NDVI',
            value: ndvi,
            classifier: getNDVIStatus,
            formatter: (v) => v.toFixed(3),
          })}
          {renderCurrentValue({
            label: 'NDMI',
            value: ndmi,
            classifier: getNDMIStatus,
            formatter: (v) => v.toFixed(3),
          })}
          {renderCurrentValue({
            label: 'LST',
            value: lst,
            classifier: getLSTStatus,
            formatter: (v) => v.toFixed(1) + '°C',
          })}
          <div className="flex items-center justify-between rounded-md bg-gray-50 px-3 py-2">
            <span className="font-medium text-gray-700">Risk: {isFiniteNumber(risk) ? risk.toFixed(3) : 'Data unavailable'}</span>
          </div>
        </div>
      </div>

      <div className="mt-4 border-t pt-3">
        <div className="text-sm font-semibold text-gray-800">Scientific Legend</div>

        <div className="mt-3 grid grid-cols-1 gap-4 lg:grid-cols-3">
          {renderScaleCard('NDVI', scales.ndvi || DEFAULT_SCALES.ndvi)}
          {renderScaleCard('NDMI', scales.ndmi || DEFAULT_SCALES.ndmi)}
          {renderScaleCard('LST', scales.lst || DEFAULT_SCALES.lst, '°C')}
        </div>
      </div>
    </div>
  )
}
