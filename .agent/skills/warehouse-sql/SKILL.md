---
name: warehouse-sql
description: Modelo dimensional PostgreSQL (grano, DDL idempotente, dimensiones, hechos) y vistas SQL de KPIs. Usar en sql/create_tables.sql, sql/kpis.sql y al cargar datos desde el consumer.
---
# Grano
`raw_happiness_events`: 1 mensaje Kafka (topic+partition+offset), válido o no · `fact_predictions`: 1 predicción por evento válido (país-año predicho por una versión de modelo) · `dim_country`: 1 país · `dim_date`: 1 día (de `event_time`) · `dim_model`: 1 versión de modelo.

# DDL (`sql/create_tables.sql`, todo `IF NOT EXISTS`)
```sql
CREATE TABLE IF NOT EXISTS raw_happiness_events(
 raw_event_id BIGSERIAL PRIMARY KEY, kafka_topic TEXT NOT NULL, kafka_partition INT NOT NULL,
 kafka_offset BIGINT NOT NULL, kafka_key TEXT, kafka_timestamp TIMESTAMPTZ, event_id TEXT,
 raw_payload TEXT NOT NULL, payload_json JSONB,
 processing_status TEXT NOT NULL DEFAULT 'RECEIVED' CHECK (processing_status IN
  ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR')),
 error_detail TEXT, ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(kafka_topic,kafka_partition,kafka_offset));
CREATE TABLE IF NOT EXISTS dim_country(country_id SERIAL PRIMARY KEY, country_name TEXT NOT NULL UNIQUE, region TEXT);
CREATE TABLE IF NOT EXISTS dim_date(date_id INT PRIMARY KEY, full_date DATE NOT NULL UNIQUE,
 year SMALLINT NOT NULL, quarter SMALLINT NOT NULL, month SMALLINT NOT NULL, day SMALLINT NOT NULL); -- date_id=YYYYMMDD
CREATE TABLE IF NOT EXISTS dim_model(model_id SERIAL PRIMARY KEY, model_version TEXT NOT NULL UNIQUE,
 algorithm TEXT NOT NULL, trained_at TIMESTAMPTZ, mae NUMERIC(8,4), rmse NUMERIC(8,4), r2 NUMERIC(8,4));
CREATE TABLE IF NOT EXISTS fact_predictions(
 prediction_id BIGSERIAL PRIMARY KEY,
 raw_event_id BIGINT NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id),
 country_id INT NOT NULL REFERENCES dim_country, date_id INT NOT NULL REFERENCES dim_date,
 model_id INT NOT NULL REFERENCES dim_model, observation_year SMALLINT NOT NULL,
 actual_score NUMERIC(6,3) NOT NULL, predicted_score NUMERIC(6,3) NOT NULL,
 prediction_error NUMERIC(6,3) NOT NULL, abs_prediction_error NUMERIC(6,3) NOT NULL,
 event_time TIMESTAMP NOT NULL, prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS ix_raw_status ON raw_happiness_events(processing_status);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS ix_fact_year ON fact_predictions(observation_year);
```
Notas: `raw_payload` TEXT conserva el mensaje exacto aunque no sea JSON; `prediction_error = predicho − real` (con signo); `observation_year` = año histórico (la tendencia con sentido sale por año, porque todos los `event_time` caen el mismo día); poblar `dim_model` al arrancar el consumer desde `model_metadata.json` (la FK lo exige). Cargas idempotentes con `ON CONFLICT`.

# KPIs (`sql/kpis.sql`, vistas `CREATE OR REPLACE VIEW`)
- `vw_kpi_avg_error`: `AVG(abs_prediction_error)` mae, `SQRT(AVG(prediction_error^2))` rmse, `AVG(prediction_error)` bias, `COUNT(*)`.
- `vw_kpi_predictions_by_country`: join `dim_country`, count y mae por país.
- `vw_kpi_pred_vs_actual`: país, año, actual, predicho, error.
- `vw_kpi_trend_by_year`: por `observation_year` avg actual/predicho/mae.
- `vw_pipeline_status`: count por `processing_status` (muestra la validación).
Verificar: `psql "$DATABASE_URL" -c "select * from vw_kpi_avg_error"`.
