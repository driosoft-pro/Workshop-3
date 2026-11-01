-- ============================================================================
-- kpis.sql — analytical views consumed by the dashboard (Superset).
-- The dashboard MUST query these views (PostgreSQL), never CSV files.
-- Loaded automatically at database init (02_kpis.sql) or manually:
--   psql "$DATABASE_URL" -f sql/kpis.sql
-- ============================================================================

-- KPI 1 — average prediction error, RMSE and signed bias over all predictions
CREATE OR REPLACE VIEW vw_kpi_avg_error AS
SELECT ROUND(AVG(abs_prediction_error), 4) AS mae,
       ROUND(SQRT(AVG(prediction_error * prediction_error)), 4) AS rmse,
       ROUND(AVG(prediction_error), 4) AS bias,
       COUNT(*) AS n_predictions
FROM fact_predictions;

-- KPI 2 — predictions (and their error) per country
CREATE OR REPLACE VIEW vw_kpi_predictions_by_country AS
SELECT c.country_name,
       c.region,
       COUNT(*) AS n_predictions,
       ROUND(AVG(f.abs_prediction_error), 4) AS mae
FROM fact_predictions f
JOIN dim_country c USING (country_id)
GROUP BY 1, 2;

-- KPI 3 — predicted vs actual score, one point per prediction
CREATE OR REPLACE VIEW vw_kpi_pred_vs_actual AS
SELECT c.country_name,
       f.observation_year,
       f.actual_score,
       f.predicted_score,
       f.prediction_error
FROM fact_predictions f
JOIN dim_country c USING (country_id);

-- KPI 4 — trend over time (historical observation year)
CREATE OR REPLACE VIEW vw_kpi_trend_by_year AS
SELECT observation_year,
       ROUND(AVG(actual_score), 3) AS avg_actual,
       ROUND(AVG(predicted_score), 3) AS avg_predicted,
       ROUND(AVG(abs_prediction_error), 3) AS mae,
       COUNT(*) AS n_predictions
FROM fact_predictions
GROUP BY 1
ORDER BY 1;

-- Pipeline health — shows contract validation in action (all raw states)
CREATE OR REPLACE VIEW vw_pipeline_status AS
SELECT processing_status,
       COUNT(*) AS n_events
FROM raw_happiness_events
GROUP BY 1;

-- Extra — volume per model version (uses dim_model, justified dimension)
CREATE OR REPLACE VIEW vw_kpi_by_model AS
SELECT m.model_version,
       m.algorithm,
       COUNT(*) AS n_predictions,
       ROUND(AVG(f.abs_prediction_error), 4) AS mae
FROM fact_predictions f
JOIN dim_model m USING (model_id)
GROUP BY 1, 2;
