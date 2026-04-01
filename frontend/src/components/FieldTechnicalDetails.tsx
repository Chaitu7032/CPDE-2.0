import React, { useEffect, useMemo, useState } from 'react'
import axios from 'axios'

interface FieldTechnicalDetailsProps {
  geometry: any | null
}

interface TechnicalDetailsResponse {
  success: boolean
  warnings: string[]
  validation: {
    is_valid: boolean
    geometry_type: string | null
    shapely_valid: boolean | null
    validity_reason: string | null
  }
  crs: {
    input_epsg: string
    processing_epsg: string
    transform_ok: boolean
  }
  metrics: {
    area_sqm: number | null
    centroid_wgs84: { lon: number | null; lat: number | null }
    centroid_utm: { x: number | null; y: number | null }
    bbox_wgs84: { min_lon: number | null; min_lat: number | null; max_lon: number | null; max_lat: number | null }
    bbox_utm: {
      min_x: number | null
      min_y: number | null
      max_x: number | null
      max_y: number | null
      width_m: number | null
      height_m: number | null
    }
  }
  pixel_coverage: {
    sentinel2_10m: {
      pixel_size_m: number
      pixel_area_sqm: number
      estimated_pixels_area_based: number | null
      estimated_pixels_area_based_ceiling: number | null
      estimated_pixels_bbox_based_upper_bound: number | null
    } | null
    modis_1000m: {
      pixel_size_m: number
      pixel_area_sqm: number
      estimated_pixels_area_based: number | null
      estimated_pixels_area_based_ceiling: number | null
      estimated_pixels_bbox_based_upper_bound: number | null
    } | null
    notes: string[]
  }
}

function formatNumber(value: number | null, fractionDigits = 3): string {
  if (value === null || Number.isNaN(value) || !Number.isFinite(value)) return '–'
  return value.toLocaleString(undefined, { maximumFractionDigits: fractionDigits })
}

function formatCoordinate(value: number | null): string {
  if (value === null || Number.isNaN(value) || !Number.isFinite(value)) return '–'
  return value.toFixed(8)
}

function formatIntegerLike(value: number | null): string {
  if (value === null || Number.isNaN(value) || !Number.isFinite(value)) return '–'
  return Math.round(value).toLocaleString()
}

function getTechnicalDetailsErrorMessage(err: any): string {
  const detail = err?.response?.data?.detail

  if (typeof detail === 'string' && detail.trim().length > 0) {
    return detail
  }

  if (err?.code === 'ERR_NETWORK') {
    return 'Technical details API is unreachable. Ensure backend is running on port 8000.'
  }

  const status = Number(err?.response?.status)
  if (status === 404) {
    return 'Technical details endpoint not found. Restart frontend to refresh proxy settings.'
  }

  if (status >= 500) {
    return 'Technical details service error on backend. Check API logs for details.'
  }

  return 'Technical details are temporarily unavailable.'
}

function getDrawnCoordinatesWgs84(geometry: any): Array<{ lon: number; lat: number }> {
  if (!geometry || geometry.type !== 'Polygon') return []
  const ring = geometry.coordinates?.[0]
  if (!Array.isArray(ring)) return []

  const coords: Array<{ lon: number; lat: number }> = []
  for (const pt of ring) {
    if (!Array.isArray(pt) || pt.length < 2) continue
    const lon = Number(pt[0])
    const lat = Number(pt[1])
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue
    coords.push({ lon, lat })
  }
  return coords
}

export default function FieldTechnicalDetails({ geometry }: FieldTechnicalDetailsProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [details, setDetails] = useState<TechnicalDetailsResponse | null>(null)
  const [requestError, setRequestError] = useState<string | null>(null)

  const hasGeometry = useMemo(() => !!geometry, [geometry])
  const drawnCoords = useMemo(() => getDrawnCoordinatesWgs84(geometry), [geometry])

  useEffect(() => {
    if (!geometry) {
      setDetails(null)
      setRequestError(null)
      setLoading(false)
      return
    }

    const controller = new AbortController()

    const fetchTechnicalDetails = async () => {
      setLoading(true)
      setRequestError(null)
      try {
        const response = await axios.post<TechnicalDetailsResponse>(
          '/field/technical-details',
          { geometry },
          { signal: controller.signal }
        )
        setDetails(response.data)
      } catch (err: any) {
        if (err?.code === 'ERR_CANCELED') return
        setRequestError(getTechnicalDetailsErrorMessage(err))
        setDetails(null)
      } finally {
        setLoading(false)
      }
    }

    fetchTechnicalDetails()

    return () => {
      controller.abort()
    }
  }, [geometry])

  return (
    <div className="mt-4 rounded-lg border border-sky-200 bg-sky-50">
      <button
        type="button"
        onClick={() => setIsOpen((prev) => !prev)}
        className="flex w-full items-center justify-between px-3 py-2 text-left"
        aria-expanded={isOpen}
      >
        <span className="text-sm font-semibold text-sky-900">📊 Field Technical Details</span>
        <span className="text-xs text-sky-700">{isOpen ? 'Hide' : 'Show'}</span>
      </button>

      {isOpen && (
        <div className="border-t border-sky-200 bg-white px-3 py-3 text-sm">
          {!hasGeometry && <div className="text-gray-600">Draw a polygon to view field technical details.</div>}

          {hasGeometry && loading && <div className="text-gray-600">Computing technical details…</div>}

          {hasGeometry && requestError && (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-2 py-2 text-amber-800">{requestError}</div>
          )}

          {hasGeometry && (
            <div className="mt-3 rounded-md border border-gray-200 p-2">
              <div className="font-medium text-gray-700">Drawn Coordinates (WGS84)</div>
              {drawnCoords.length === 0 ? (
                <div className="mt-1 text-gray-600">No polygon coordinates available.</div>
              ) : (
                <div className="mt-1 max-h-40 overflow-auto rounded bg-gray-50 p-2 text-xs text-gray-700">
                  {drawnCoords.map((pt, idx) => (
                    <div key={`${pt.lon}-${pt.lat}-${idx}`}>
                      P{idx + 1}: Lon {formatCoordinate(pt.lon)}, Lat {formatCoordinate(pt.lat)}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {hasGeometry && details && (
            <div className="space-y-3">
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <div className="rounded-md border border-gray-200 p-2">
                  <div className="font-medium text-gray-700">Validation</div>
                  <div className="mt-1 text-gray-700">Geometry type: {details.validation.geometry_type || '–'}</div>
                  <div className="text-gray-700">Polygon valid: {details.validation.is_valid ? 'Yes' : 'No'}</div>
                  <div className="text-gray-700">Shapely valid: {details.validation.shapely_valid === null ? '–' : details.validation.shapely_valid ? 'Yes' : 'No'}</div>
                  <div className="text-gray-700">Reason: {details.validation.validity_reason || '–'}</div>
                </div>

                <div className="rounded-md border border-gray-200 p-2">
                  <div className="font-medium text-gray-700">CRS</div>
                  <div className="mt-1 text-gray-700">Input: {details.crs.input_epsg}</div>
                  <div className="text-gray-700">Processing: {details.crs.processing_epsg}</div>
                  <div className="text-gray-700">Transform status: {details.crs.transform_ok ? 'OK' : 'Unavailable'}</div>
                </div>
              </div>

              <div className="rounded-md border border-gray-200 p-2">
                <div className="font-medium text-gray-700">Geometry Metrics</div>
                <div className="mt-1 grid grid-cols-1 gap-1 sm:grid-cols-2">
                  <div className="text-gray-700">Area (m²): {formatNumber(details.metrics.area_sqm, 2)}</div>
                  <div className="text-gray-700">Centroid WGS84: ({formatCoordinate(details.metrics.centroid_wgs84.lon)}, {formatCoordinate(details.metrics.centroid_wgs84.lat)})</div>
                  <div className="text-gray-700">Centroid UTM: ({formatNumber(details.metrics.centroid_utm.x, 2)}, {formatNumber(details.metrics.centroid_utm.y, 2)})</div>
                  <div className="text-gray-700">
                    BBox WGS84: [{formatCoordinate(details.metrics.bbox_wgs84.min_lon)}, {formatCoordinate(details.metrics.bbox_wgs84.min_lat)}] → [{formatCoordinate(details.metrics.bbox_wgs84.max_lon)}, {formatCoordinate(details.metrics.bbox_wgs84.max_lat)}]
                  </div>
                  <div className="text-gray-700">
                    BBox UTM size (m): {formatNumber(details.metrics.bbox_utm.width_m, 2)} × {formatNumber(details.metrics.bbox_utm.height_m, 2)}
                  </div>
                </div>
              </div>

              <div className="rounded-md border border-gray-200 p-2">
                <div className="font-medium text-gray-700">Satellite Pixel Coverage (Estimate)</div>
                <div className="mt-1 grid grid-cols-1 gap-1 sm:grid-cols-2">
                  <div className="text-gray-700">
                    Sentinel-2 (10m): ~{formatIntegerLike(details.pixel_coverage.sentinel2_10m?.estimated_pixels_area_based_ceiling ?? null)} pixels
                  </div>
                  <div className="text-gray-700">
                    Sentinel-2 upper bound (bbox): {formatIntegerLike(details.pixel_coverage.sentinel2_10m?.estimated_pixels_bbox_based_upper_bound ?? null)} pixels
                  </div>
                  <div className="text-gray-700">
                    MODIS (1000m): ~{formatIntegerLike(details.pixel_coverage.modis_1000m?.estimated_pixels_area_based_ceiling ?? null)} pixels
                  </div>
                  <div className="text-gray-700">
                    MODIS upper bound (bbox): {formatIntegerLike(details.pixel_coverage.modis_1000m?.estimated_pixels_bbox_based_upper_bound ?? null)} pixels
                  </div>
                </div>
                {details.pixel_coverage.notes?.length > 0 && (
                  <div className="mt-2 rounded bg-gray-50 p-2 text-xs text-gray-600">
                    {details.pixel_coverage.notes.join(' ')}
                  </div>
                )}
              </div>

              {details.warnings?.length > 0 && (
                <div className="rounded-md border border-amber-200 bg-amber-50 p-2 text-amber-800">
                  <div className="font-medium">Warnings</div>
                  <ul className="list-disc pl-5">
                    {details.warnings.map((warning, idx) => (
                      <li key={idx}>{warning}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
