import React from 'react'

function getNDVIStatus(v) {
  if (v >= 0.6) return { label: 'Dense Healthy', color: 'green', emoji: '🟢' }
  if (v >= 0.4) return { label: 'Moderate', color: 'yellow', emoji: '🟡' }
  if (v >= 0.2) return { label: 'Sparse / Stressed', color: 'orange', emoji: '🟠' }
  if (v >= 0.0) return { label: 'Bare Soil / Severe Stress', color: 'red', emoji: '🔴' }
  return { label: 'Water / Non-vegetation', color: 'blue', emoji: '🔵' }
}

function getNDMIStatus(v) {
  if (v >= 0.3) return { label: 'High Moisture', color: 'green', emoji: '🟢' }
  if (v >= 0.1) return { label: 'Moderate Moisture', color: 'yellow', emoji: '🟡' }
  if (v >= -0.1) return { label: 'Dry Conditions', color: 'orange', emoji: '🟠' }
  return { label: 'Severe Dryness', color: 'red', emoji: '🔴' }
}

function getLSTStatus(v) {
  if (v <= 30) return { label: 'Optimal', color: 'green', emoji: '🟢' }
  if (v <= 35) return { label: 'Mild Stress', color: 'yellow', emoji: '🟡' }
  if (v <= 40) return { label: 'High Stress', color: 'orange', emoji: '🟠' }
  return { label: 'Severe Stress', color: 'red', emoji: '🔴' }
}

function getRiskStatus(v) {
  if (v <= 0.3) return { label: 'Low Risk', color: 'green', emoji: '🟢' }
  if (v <= 0.6) return { label: 'Moderate Risk', color: 'yellow', emoji: '🟡' }
  if (v <= 0.8) return { label: 'High Risk', color: 'orange', emoji: '🟠' }
  return { label: 'Severe Risk', color: 'red', emoji: '🔴' }
}

function isFiniteNumber(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

function toneClass(color) {
  if (color === 'green') return 'text-green-700'
  if (color === 'yellow') return 'text-amber-700'
  if (color === 'orange') return 'text-orange-700'
  if (color === 'red') return 'text-red-700'
  if (color === 'blue') return 'text-blue-700'
  return 'text-gray-700'
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
      <span className={'text-sm font-semibold ' + toneClass(status.color)}>
        {status.label} {status.emoji}
      </span>
    </div>
  )
}

/**
 * @param {{ ndvi?: number | null, ndmi?: number | null, lst?: number | null, risk?: number | null }} props
 */
export default function ScientificLegend({ ndvi, ndmi, lst, risk }) {
  return (
    <div className="mt-4 rounded-lg border bg-white p-4">
      <div className="text-base font-semibold text-gray-800">📊 Scientific Interpretation</div>

      <div className="mt-3">
        <div className="text-sm font-semibold text-gray-800">🧠 Your Field Analysis</div>
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
          {renderCurrentValue({
            label: 'Risk',
            value: risk,
            classifier: getRiskStatus,
            formatter: (v) => v.toFixed(3),
          })}
        </div>
      </div>

      <div className="mt-4 border-t pt-3">
        <div className="text-sm font-semibold text-gray-800">Scientific Legend</div>

        <div className="mt-3 grid grid-cols-1 gap-4 lg:grid-cols-2">
          <div className="rounded-md border p-3">
            <div className="mb-2 text-sm font-semibold text-gray-800">NDVI</div>
            <div className="space-y-1 text-sm text-gray-700">
              <div>0.6 - 1.0   🟢 Dense Healthy</div>
              <div>0.4 - 0.6   🟡 Moderate</div>
              <div>0.2 - 0.4   🟠 Sparse / Stressed</div>
              <div>0.0 - 0.2   🔴 Bare Soil</div>
              <div>&lt; 0        🔵 Water</div>
            </div>
          </div>

          <div className="rounded-md border p-3">
            <div className="mb-2 text-sm font-semibold text-gray-800">NDMI</div>
            <div className="space-y-1 text-sm text-gray-700">
              <div>0.3 - 1.0    🟢 High Moisture</div>
              <div>0.1 - 0.3    🟡 Moderate Moisture</div>
              <div>-0.1 - 0.1   🟠 Dry</div>
              <div>&lt; -0.1      🔴 Severe Dryness</div>
            </div>
          </div>

          <div className="rounded-md border p-3">
            <div className="mb-2 text-sm font-semibold text-gray-800">LST</div>
            <div className="space-y-1 text-sm text-gray-700">
              <div>20-30°C      🟢 Optimal</div>
              <div>30-35°C      🟡 Mild Stress</div>
              <div>35-40°C      🟠 High Stress</div>
              <div>&gt; 40°C      🔴 Severe Stress</div>
            </div>
          </div>

          <div className="rounded-md border p-3">
            <div className="mb-2 text-sm font-semibold text-gray-800">Risk</div>
            <div className="space-y-1 text-sm text-gray-700">
              <div>0.0-0.3      🟢 Low</div>
              <div>0.3-0.6      🟡 Moderate</div>
              <div>0.6-0.8      🟠 High</div>
              <div>0.8-1.0      🔴 Severe</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
