import React from 'react'

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

function formatValue(value, digits = 3) {
  return Number.isFinite(value) ? value.toFixed(digits) : 'N/A'
}

export default function MethodologyPanel({ selectedGrid }) {
  const properties = selectedGrid?.properties || {}
  const liveRisk = toFiniteNumber(properties?.risk)

  const exampleContrib = {
    ndvi: 0.4,
    ndmi: 0.3,
    lst: 0.3,
    total: 0.95,
  }

  return (
    <section className="space-y-4">
      <div className="rounded-lg border bg-white p-4">
        <h2 className="text-lg font-semibold text-slate-800">Methodology Panel</h2>
        <p className="mt-1 text-sm text-slate-600">Method transparency for reproducibility, GIS correctness, and risk model interpretability.</p>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="text-sm font-semibold text-slate-700">Grid Generation Workflow</div>
        <div className="mt-2 space-y-2 text-sm text-slate-700">
          <div>1. Generate a 10m × 10m metric grid over the field bounding box.</div>
          <div>2. Intersect each candidate cell with the field polygon.</div>
          <div>3. Include partial overlap cells to preserve edge coverage.</div>
        </div>
        <div className="mt-3 rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-900">
          Grid count exceeds area/100 due to partial overlaps (standard GIS method).
        </div>
      </div>

      <div className="rounded-lg border bg-white p-4">
        <div className="text-sm font-semibold text-slate-700">Risk Model Explainability</div>
        <div className="mt-2 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
          Risk Score = weighted combination of NDVI (low vegetation), NDMI (moisture stress), and LST (heat stress)
          <InfoBadge label="Risk Score" description="Composite stress metric used for grid-level prioritization." />
        </div>

        <div className="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-3 text-sm">
          <div className="rounded-md border border-orange-200 bg-orange-50 px-3 py-2 text-orange-900">
            NDVI contribution
            <InfoBadge label="NDVI" description="Vegetation greenness proxy from red and NIR bands." />
            : +{formatValue(exampleContrib.ndvi, 1)}
          </div>
          <div className="rounded-md border border-yellow-200 bg-yellow-50 px-3 py-2 text-yellow-900">
            NDMI contribution
            <InfoBadge label="NDMI" description="Moisture-sensitive index combining NIR and SWIR information." />
            : +{formatValue(exampleContrib.ndmi, 1)}
          </div>
          <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-red-900">
            LST contribution
            <InfoBadge label="LST" description="Surface thermal stress indicator." />
            : +{formatValue(exampleContrib.lst, 1)}
          </div>
        </div>

        <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm font-semibold text-emerald-900">
          Final Risk (worked example): {formatValue(exampleContrib.total, 2)}
        </div>

        <div className="mt-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          Current selected grid live risk: {liveRisk === null ? 'Unavailable' : formatValue(liveRisk)}
        </div>

        <details className="mt-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          <summary className="cursor-pointer font-medium">Model assumptions and traceability</summary>
          <div className="mt-2">
            The worked example above is illustrative. When backend-normalized terms are available, this panel can be bound directly to per-indicator contribution outputs for audit-grade traceability.
          </div>
        </details>
      </div>
    </section>
  )
}
