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

function readValue(value, fallbackValue) {
  if (value === null || value === undefined || value === '') {
    return { value: fallbackValue, fallback: true }
  }
  return { value, fallback: false }
}

function SourceValue({ label, content, fallback }) {
  return (
    <div className="flex items-start justify-between gap-3 rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
      <span className="text-sm font-medium text-slate-700">{label}</span>
      <div className="text-right">
        <div className="text-sm text-slate-800">{content}</div>
        {fallback && <div className="text-xs text-amber-700">demo fallback</div>}
      </div>
    </div>
  )
}

export default function EvidencePanel({ selectedGrid, latestDate, provenance }) {
  const properties = selectedGrid?.properties || {}

  const satelliteSource = readValue(provenance?.satellite_source, 'Sentinel-2 L2A (ESA)')
  const acquisitionDate = readValue(provenance?.acquisition_date ?? latestDate, '2026-03-14')
  const tileId = readValue(provenance?.tile_id ?? properties?.tile_id, 'T44QKF')
  const cloudCoverage = readValue(
    provenance?.cloud_coverage_pct ?? properties?.cloud_coverage_pct,
    '11.8'
  )

  return (
    <section className="space-y-4">
      <div className="rounded-lg border bg-white p-4">
        <h2 className="text-lg font-semibold text-slate-800">Scientific Evidence and Provenance</h2>
        <p className="mt-1 text-sm text-slate-600">
          End-to-end evidence trail for source data, equations, and geospatial transformation steps.
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <div className="rounded-lg border bg-white p-4 space-y-2">
          <div className="text-sm font-semibold text-slate-700">Data Provenance</div>
          <SourceValue label="Satellite Source" content={satelliteSource.value} fallback={satelliteSource.fallback} />
          <SourceValue label="Acquisition Date" content={acquisitionDate.value} fallback={acquisitionDate.fallback} />
          <SourceValue label="Tile ID" content={tileId.value} fallback={tileId.fallback} />
          <SourceValue label="Cloud Coverage (%)" content={cloudCoverage.value} fallback={cloudCoverage.fallback} />
          <SourceValue label="Resolution" content="10m" fallback={false} />

          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            Bands used:
            <div className="mt-1">B04 (665 nm) <InfoBadge label="B04" description="Red spectral band for vegetation contrast." /></div>
            <div>B08 (842 nm) <InfoBadge label="B08" description="Near-infrared band sensitive to leaf cellular structure." /></div>
          </div>
        </div>

        <div className="rounded-lg border bg-white p-4 space-y-3">
          <div className="text-sm font-semibold text-slate-700">Scientific Formulas and CRS</div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-sm text-slate-700">
            <div>NDVI = (B08 - B04) / (B08 + B04)</div>
            <div className="mt-2">NDMI = (NIR - SWIR) / (NIR + SWIR)</div>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
            CRS transformation: EPSG:4326 → EPSG:32644
            <InfoBadge
              label="CRS"
              description="WGS84 geographic coordinates transformed to UTM Zone 44N metric coordinates for area/length-accurate processing."
            />
          </div>

          <div className="rounded-md border border-emerald-200 bg-emerald-50 p-3">
            <div className="text-sm font-semibold text-emerald-800">Why this is reliable</div>
            <ul className="mt-1 list-disc pl-5 text-sm text-emerald-900">
              <li>ESA open Sentinel-2 L2A data is globally referenced and reproducible.</li>
              <li>NDVI and NDMI are peer-reviewed, standard indices in remote sensing science.</li>
              <li>Processing chain follows standard GIS and Earth observation practices.</li>
            </ul>
          </div>

          <details className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            <summary className="cursor-pointer font-medium">Provenance checklist</summary>
            <div className="mt-2 space-y-1">
              <div>1. Verify acquisition date against the reported scene metadata.</div>
              <div>2. Confirm tile identifier and cloud threshold used for scene selection.</div>
              <div>3. Confirm CRS conversion chain before metric computations.</div>
            </div>
          </details>
        </div>
      </div>
    </section>
  )
}
