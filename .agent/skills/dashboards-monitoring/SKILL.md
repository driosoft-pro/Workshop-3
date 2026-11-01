---
name: dashboards-monitoring
description: Superset (KPIs de negocio sobre PostgreSQL), Prometheus y Grafana (salud del pipeline) con Podman. Usar al montar dashboards, métricas del consumer y screenshots de entrega.
---
# Reparto de responsabilidades
- **Superset** = lo que pide la rúbrica: conecta a PostgreSQL y usa solo vistas `vw_*` (nunca CSV).
- **Grafana + Prometheus** = salud operativa (bonus): eventos/seg por estado, latencia p95, inválidos.
- Si el tiempo aprieta: Superset primero.

# Superset
- La imagen oficial no trae driver Postgres: Dockerfile mínimo `FROM docker.io/apache/superset` + `pip install psycopg2-binary`. Init: `superset db upgrade` → `superset fab create-admin` → `superset init`. Definir `SUPERSET_SECRET_KEY` por `.env`.
- URI desde contenedor: `postgresql+psycopg2://happiness:***@postgres:5432/happiness` (host = nombre del servicio, no localhost).
- Gráficos: Big Number (MAE, bias) · barras por país (top N) o mapa · scatter real vs predicho (línea y=x) · líneas por `observation_year` · barras de `vw_pipeline_status`.
- Entrega: screenshots en `dashboards/`, export (`superset export-dashboards`) o instrucciones de importación, y explicación de cada KPI (qué mide, de qué vista, cómo leerlo).

# Prometheus / Grafana
- `monitoring/prometheus.yml`: job a `host.containers.internal:8000` (consumer en el host). Si el consumer corre en compose, usar su nombre de servicio.
- Consumer expone con `prometheus_client.start_http_server(8000)`: `events_consumed_total{status}`, `prediction_latency_seconds` (Histogram), `prediction_abs_error` (Histogram).
- Consultas Grafana: `sum by(status)(rate(events_consumed_total[1m]))`, `histogram_quantile(0.95, sum(rate(prediction_latency_seconds_bucket[5m])) by (le))`. Provisionar datasource en `monitoring/grafana/`.
- Verificar: `curl -s localhost:8000/metrics | head -20`.
