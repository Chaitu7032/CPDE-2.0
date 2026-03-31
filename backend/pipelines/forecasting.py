import os
import math
import joblib
from datetime import datetime, timedelta
from typing import Tuple, Optional, List

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

from sqlalchemy import text

from backend.db.connection import async_session

# Paths
MODELS_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
os.makedirs(MODELS_DIR, exist_ok=True)


def _ndvi_slope(series: pd.Series) -> float:
    # linear fit slope over series index
    if len(series) < 2:
        return 0.0
    x = np.arange(len(series))
    y = series.values
    A = np.vstack([x, np.ones_like(x)]).T
    m, _ = np.linalg.lstsq(A, y, rcond=None)[0]
    return float(m)


async def _assemble_feature_table(land_id, grid_id, end_date: str, lookback_days: int = 14) -> pd.DataFrame:
    """Assemble recent features for a grid ending at `end_date` (inclusive).

    Features: ndvi_slope (last lookback_days), ndvi, ndmi_z, lst_z, precip_sum_7d, t2m_mean_7d
    """
    land_id = int(land_id)
    grid_id = str(grid_id)
    end = datetime.fromisoformat(end_date).date()
    start = end - timedelta(days=lookback_days - 1)

    async with async_session() as session:
        # NDVI timeseries
        q = "SELECT date, ndvi FROM land_daily_indices WHERE grid_id = :gid AND date BETWEEN :start AND :end ORDER BY date"
        res = await session.execute(text(q), {"gid": grid_id, "start": start, "end": end})
        rows = res.fetchall()

        ndvi_df = pd.DataFrame(rows, columns=["date", "ndvi"]).set_index("date") if rows else pd.DataFrame()

        # anomalies: NDMI and LST zscores
        q2 = "SELECT date, variable, zscore FROM land_anomalies WHERE grid_id = :gid AND date BETWEEN :start AND :end AND variable IN ('ndmi','lst')"
        res2 = await session.execute(text(q2), {"gid": grid_id, "start": start, "end": end})
        rows2 = res2.fetchall()
        anom_df = pd.DataFrame(rows2, columns=["date", "variable", "zscore"]) if rows2 else pd.DataFrame()

        # weather
        q3 = "SELECT date, t2m, prectotcorr FROM land_daily_weather WHERE land_id = (SELECT land_id FROM land_grid_cells WHERE grid_id = :gid LIMIT 1) AND date BETWEEN :start AND :end ORDER BY date"
        res3 = await session.execute(text(q3), {"gid": grid_id, "start": start, "end": end})
        rows3 = res3.fetchall()
        weather_df = pd.DataFrame(rows3, columns=["date", "t2m", "prectotcorr"]).set_index("date") if rows3 else pd.DataFrame()

    # Build feature vector for end_date
    features = {}
    # NDVI slope
    if not ndvi_df.empty:
        s = ndvi_df["ndvi"].astype(float)
        # ensure index covers lookback days
        # compute slope
        features["ndvi_slope"] = _ndvi_slope(s)
        features["ndvi_last"] = float(s.iloc[-1])
    else:
        features["ndvi_slope"] = 0.0
        features["ndvi_last"] = float("nan")

    # NDMI z and LST z: take the most recent available within window
    if not anom_df.empty:
        anom_df["date"] = pd.to_datetime(anom_df["date"])
        anom_df = anom_df.pivot(index="date", columns="variable", values="zscore")
        anom_df = anom_df.sort_index()
        features["ndmi_z"] = float(anom_df["ndmi"].dropna().iloc[-1]) if "ndmi" in anom_df and anom_df["ndmi"].dropna().size > 0 else float("nan")
        features["lst_z"] = float(anom_df["lst"].dropna().iloc[-1]) if "lst" in anom_df and anom_df["lst"].dropna().size > 0 else float("nan")
    else:
        features["ndmi_z"] = float("nan")
        features["lst_z"] = float("nan")

    # weather aggregated features (7-day sum/mean)
    if not weather_df.empty:
        wd = weather_df.astype(float)
        last7 = wd.tail(7)
        features["precip_7d"] = float(last7["prectotcorr"].sum())
        features["t2m_mean_7d"] = float(last7["t2m"].mean())
    else:
        features["precip_7d"] = float("nan")
        features["t2m_mean_7d"] = float("nan")

    df = pd.DataFrame([features])
    return df


async def train_logistic_model(land_id, grid_id, lookback_days: int = 14, predict_horizon: int = 14, history_days: int = 365 * 3) -> str:
    """Train logistic regression model using historical data for a grid.

    Label is 1 if NDVI collapse occurs within next `predict_horizon` days, defined as NDVI dipping relative to climatology.
    Returns model file path.
    """
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    grid_id = str(grid_id)
    # Build a dataset by sliding window over history_days
    end_date = datetime.utcnow().date()
    start_history = end_date - timedelta(days=history_days)

    X_records = []
    y_records = []
    lead_times = []

    async with async_session() as session:
        # fetch NDVI timeseries for the grid across history
        q = "SELECT date, ndvi FROM land_daily_indices WHERE grid_id = :gid AND date BETWEEN :start AND :end ORDER BY date"
        res = await session.execute(text(q), {"gid": grid_id, "start": start_history, "end": end_date})
        ndvi_rows = res.fetchall()

    if not ndvi_rows:
        raise RuntimeError("No historical NDVI data available for training")

    ndvi_ts = pd.DataFrame(ndvi_rows, columns=["date", "ndvi"]).set_index("date").astype(float)
    ndvi_ts.index = pd.to_datetime(ndvi_ts.index)

    dates = ndvi_ts.index.date
    for i in range(lookback_days, len(dates) - predict_horizon):
        end_feat_date = dates[i]
        # assemble features
        feat_df = await _assemble_feature_table(land_id, grid_id, end_feat_date.isoformat(), lookback_days=lookback_days)
        if feat_df.isnull().any(axis=1).iloc[0]:
            continue
        # label: find min NDVI in next predict_horizon days and compare to climatology via z-score stored in land_climatology
        future_window = ndvi_ts.iloc[i + 1: i + 1 + predict_horizon]
        if future_window.empty:
            continue
        ndvi_min = float(future_window["ndvi"].min())

        # compute climatology mean/std for DOY of min date
        min_date = future_window["ndvi"].idxmin().date()
        doy = min_date.timetuple().tm_yday
        async with async_session() as session:
            res = await session.execute(
                text("SELECT mean, std FROM land_climatology WHERE grid_id = :gid AND variable = 'ndvi' AND day_of_year = :doy"),
                {"gid": grid_id, "doy": doy},
            )
            clim = res.first()

        if not clim or clim[0] is None or clim[1] is None or clim[1] == 0:
            continue
        mean, std = float(clim[0]), float(clim[1])
        z = (ndvi_min - mean) / std
        # label collapse if z below -2 (statistical rule; adjustable) — this parameter can be exposed
        label = 1 if z <= -2.0 else 0
        lead_time = (min_date - end_feat_date).days

        X_records.append(feat_df.iloc[0].to_dict())
        y_records.append(label)
        lead_times.append(lead_time)

    if not X_records:
        raise RuntimeError("No training examples generated; need more data or relax filters")

    X = pd.DataFrame(X_records)
    y = np.array(y_records)
    lead_times = np.array(lead_times)

    # handle NaNs: simple imputation with column mean
    X = X.fillna(X.mean())

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    model = LogisticRegression(class_weight='balanced', max_iter=1000)
    model.fit(Xs, y)

    # store training positives' features + lead_times for nearest-neighbor lead-time estimation
    pos_idx = np.where(y == 1)[0]
    pos_features = Xs[pos_idx] if pos_idx.size > 0 else np.empty((0, Xs.shape[1]))
    pos_leads = lead_times[pos_idx] if pos_idx.size > 0 else np.array([])

    model_pkg = {
        "scaler": scaler,
        "model": model,
        "pos_features": pos_features,
        "pos_leads": pos_leads,
        "feature_columns": list(X.columns),
    }

    model_name = f"forecast_{land_id}_{grid_id}.joblib"
    model_path = os.path.join(MODELS_DIR, model_name)
    joblib.dump(model_pkg, model_path)
    return model_path


async def predict_risk(land_id, grid_id, date: str, model_path: Optional[str] = None, n_neighbors: int = 5) -> dict:
    # load model
    land_id = int(land_id)  # ensure integer for asyncpg type safety
    grid_id = str(grid_id)
    if model_path is None:
        model_path = os.path.join(MODELS_DIR, f"forecast_{land_id}_{grid_id}.joblib")
    if not os.path.exists(model_path):
        raise FileNotFoundError("Model not found; train first")

    pkg = joblib.load(model_path)
    scaler = pkg["scaler"]
    model = pkg["model"]
    pos_features = pkg.get("pos_features", np.empty((0, 0)))
    pos_leads = pkg.get("pos_leads", np.array([]))
    cols = pkg["feature_columns"]

    feat_df = await _assemble_feature_table(land_id, grid_id, date)
    feat_df = feat_df.fillna(feat_df.mean())
    Xs = scaler.transform(feat_df[cols])
    prob = float(model.predict_proba(Xs)[0, 1])

    expected_lead = None
    if pos_features.size > 0 and pos_leads.size > 0:
        nbrs = NearestNeighbors(n_neighbors=min(n_neighbors, pos_features.shape[0])).fit(pos_features)
        dists, idxs = nbrs.kneighbors(Xs)
        neighbors_idx = idxs[0]
        expected_lead = float(np.mean(pos_leads[neighbors_idx]))

    return {"probability": prob, "expected_lead_time": expected_lead, "model_path": model_path}
