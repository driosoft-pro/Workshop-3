-- ============================================================================
-- create_tables.sql — dimensional model for Workshop 3 (streaming predictions)
-- Idempotent: safe to run repeatedly on an empty or existing database.
-- Loaded automatically by PostgreSQL at first boot
-- (docker-entrypoint-initdb.d/01_create_tables.sql) and runnable manually:
--   psql "$DATABASE_URL" -f sql/create_tables.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- RAW / STAGING: one row per Kafka message consumed (valid or not).
-- Grain: (kafka_topic, kafka_partition, kafka_offset) = 1 consumed message.
-- Keeps the payload exactly as it arrived for audit/debug/reprocessing.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    kafka_topic       TEXT        NOT NULL,
    kafka_partition   INT         NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    kafka_key         TEXT,
    kafka_timestamp   TIMESTAMPTZ,
    event_id          TEXT,                   -- NULL until the JSON parses
    raw_payload       TEXT        NOT NULL,   -- original message, untouched
    payload_json      JSONB,                  -- NULL if not valid JSON
    processing_status TEXT        NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN
               ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR')),
    error_detail      TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- idempotency under at-least-once delivery: redeliveries are ignored
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_raw_event_id ON raw_happiness_events(event_id);
CREATE INDEX IF NOT EXISTS ix_raw_status   ON raw_happiness_events(processing_status);

-- ----------------------------------------------------------------------------
-- DIMENSIONS
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE,
    region       TEXT                -- 'Unknown' when the source has no region
);

-- One calendar day, derived from event_time (pipeline time, not history)
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INT PRIMARY KEY,       -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year      SMALLINT NOT NULL,
    quarter   SMALLINT NOT NULL,
    month     SMALLINT NOT NULL,
    day       SMALLINT NOT NULL
);

-- One model version (traceability + version comparison)
CREATE TABLE IF NOT EXISTS dim_model (
    model_id      SERIAL PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    algorithm     TEXT NOT NULL,
    trained_at    TIMESTAMPTZ,
    mae           NUMERIC(8,4),
    rmse          NUMERIC(8,4),
    r2            NUMERIC(8,4)
);

-- ----------------------------------------------------------------------------
-- FACT: one prediction per valid event.
-- Grain: 1 valid Kafka event -> 1 predicted country-year scored by 1 model.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT NOT NULL UNIQUE
                             REFERENCES raw_happiness_events(raw_event_id),
    country_id           INT    NOT NULL REFERENCES dim_country(country_id),
    date_id              INT    NOT NULL REFERENCES dim_date(date_id),
    model_id             INT    NOT NULL REFERENCES dim_model(model_id),
    observation_year     SMALLINT NOT NULL,   -- historical year (degenerate dim)
    actual_score         NUMERIC(6,3) NOT NULL,
    predicted_score      NUMERIC(6,3) NOT NULL,
    prediction_error     NUMERIC(6,3) NOT NULL,  -- predicted - actual (signed)
    abs_prediction_error NUMERIC(6,3) NOT NULL,  -- for the MAE KPI
    event_time           TIMESTAMP NOT NULL,     -- when the producer emitted
    prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS ix_fact_year    ON fact_predictions(observation_year);
