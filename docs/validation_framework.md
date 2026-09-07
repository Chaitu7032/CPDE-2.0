# CPDE 2.0 Scientific Validation Framework & Benchmarking Standards

## 1. Multi-Model Benchmark Hierarchy

To rigorously establish model validity under peer-review standards, all candidate algorithms are evaluated sequentially against standard baselines:

```
                  ┌── 0. Naive Persistence Baseline (y_t = y_{t-1})
                  │
                  ├── 1. Rule-Based Expert Threshold (NDVI < 0.35, NDMI < 0.0)
                  │
                  ├── 2. Calibrated Logistic Regression (Platt Scaling)
                  │
                  ├── 3. Random Forest Classifier (Optical + Thermal)
                  │
                  ├── 4. Temporal Sequence Model (LSTM / Moving Window)
                  │
                  └── 5. Physics-Informed Multi-Sensor Fusion Engine
```

---

## 2. Spatial Block Holdout Protocol (Preventing Data Leakage)

To avoid optimistic bias caused by spatial and temporal autocorrelation:
- **Spatial Field-Level Holdout**:
  - Sample fields across Bapatla District are partitioned strictly by Field ID ($70\%$ Train, $15\%$ Validation, $15\%$ Test).
  - Pixels from the same farm boundary are **never split** across training and testing sets.
- **Temporal Multi-Season Holdout**:
  - Training on historical seasons ($2024 - 2025$).
  - Testing on unseen subsequent seasons ($2026$).

### Data Leakage Audit Checklist
- [x] **Zero Spatial Overlap**: Train field set $\cap$ Test field set $= \emptyset$.
- [x] **Zero Temporal Lookahead**: Historical rolling baselines strictly exclude observations from $t \ge t_0$.
- [x] **Causal Meteorological Forcing**: Predictors strictly use pre-event and current-day weather variables; future weather forecast data is isolated.

---

## 3. Evaluation Metrics Suite

### A. Classification & Stress Recall
- **Recall for Stressed Fields ($Recall_{stress}$)**: $\frac{TP}{TP + FN}$ (Prioritized to avoid missing genuine crop water deficit).
- **Precision**: $\frac{TP}{TP + FP}$
- **F1-Score**: $2 \times \frac{Precision \times Recall}{Precision + Recall}$
- **PR-AUC & ROC-AUC**: Precision-Recall and Receiver Operating Characteristic Area Under Curve.

### B. Probability Calibration & Reliability
- **Brier Score**:
  $$BS = \frac{1}{N} \sum_{i=1}^{N} (P_i - y_i)^2$$
- **Expected Calibration Error (ECE)**:
  $$ECE = \sum_{m=1}^{M} \frac{|B_m|}{N} \left| \text{acc}(B_m) - \text{conf}(B_m) \right|$$
- **Reliability Diagrams**: Calibration curves plotting mean predicted probability vs observed empirical frequency.

---

## 4. Empirical Lead-Time Measurement (Rule 6)

Claims of "early warning" or "lead time" are calculated exclusively from empirical temporal timelines:
$$\text{Empirical Lead Time} = t_{\text{field\_symptom}} - t_{\text{satellite\_anomaly}}$$
- **Hypothesis**: Red-edge (NDRE) and canopy moisture (NDMI) anomalies provide early indication before visible canopy wilting.
- **Reporting**: Median and interquartile range of observed lead times across all validated Bapatla field events.
