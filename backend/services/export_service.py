"""
CPDE v2 Research Dataset Export Service
Streams clean, publication-ready datasets in CSV, GeoJSON, and Apache Parquet formats.
"""

from __future__ import annotations
import io
import json
from typing import Any, Literal
import pandas as pd
from sqlalchemy import text

from backend.db.connection import async_session


async def export_field_dataset(
    field_id: int,
    format_type: Literal["csv", "geojson", "parquet"] = "csv",
) -> tuple[bytes, str, str]:
    """Export all grid cells and multi-temporal observations for a field.
    
    Returns:
      (content_bytes, media_type, filename)
    """
    async with async_session() as session:
        q = text("""
            SELECT c.field_id, c.grid_id, c.row_idx, c.col_idx, c.area_sqm,
                   ST_AsGeoJSON(ST_Transform(c.geom, 4326)) AS geojson,
                   o.date, o.b02, o.b03, o.b04, o.b05, o.b08, o.b11,
                   o.ndvi, o.ndmi, o.ndre, o.evi, o.savi, o.gci,
                   o.quality_score, o.confidence_score, o.health_status
            FROM field_grid_cells c
            LEFT JOIN grid_observations o ON c.grid_id = o.grid_id
            WHERE c.field_id = :fid
            ORDER BY o.date DESC, c.grid_num ASC
        """)
        res = await session.execute(q, {"fid": field_id})
        rows = res.fetchall()

        if format_type == "geojson":
            features = []
            for r in rows:
                geom = json.loads(r[5]) if r[5] else None
                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "field_id": r[0],
                        "grid_id": r[1],
                        "row": r[2],
                        "col": r[3],
                        "area_sqm": r[4],
                        "date": r[6].isoformat() if r[6] else None,
                        "b02": r[7], "b03": r[8], "b04": r[9], "b05": r[10], "b08": r[11], "b11": r[12],
                        "ndvi": r[13], "ndmi": r[14], "ndre": r[15], "evi": r[16], "savi": r[17], "gci": r[18],
                        "quality_score": r[19],
                        "confidence_score": r[20],
                        "health_status": r[21],
                    },
                })
            fc = {"type": "FeatureCollection", "features": features}
            data = json.dumps(fc, indent=2).encode("utf-8")
            return data, "application/geo+json", f"cpde_field_{field_id}_dataset.geojson"

        # Tabular: DataFrame
        cols = [
            "field_id", "grid_id", "row", "col", "area_sqm", "geojson",
            "date", "b02", "b03", "b04", "b05", "b08", "b11",
            "ndvi", "ndmi", "ndre", "evi", "savi", "gci",
            "quality_score", "confidence_score", "health_status",
        ]
        df = pd.DataFrame(rows, columns=cols)
        # Drop raw geojson from tabular exports to keep files lightweight
        df = df.drop(columns=["geojson"])

        if format_type == "parquet":
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            return buf.getvalue(), "application/octet-stream", f"cpde_field_{field_id}_dataset.parquet"

        # Default: CSV
        csv_str = df.to_csv(index=False)
        return csv_str.encode("utf-8"), "text/csv", f"cpde_field_{field_id}_dataset.csv"
