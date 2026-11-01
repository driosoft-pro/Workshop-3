---
name: delivery-readme
description: Entorno Nix/Git, README final obligatorio, instrucciones de ejecución, checklist de entrega y guion de presentación. Usar en Fase 0 y Fase 10, o antes de entregar.
---
# Fase 0 — Entorno
`flake.nix` con `devShell`: python312.withPackages [pandas numpy scikit-learn joblib pyarrow matplotlib seaborn confluent-kafka sqlalchemy psycopg2 great-expectations prometheus-client jupyterlab ipykernel jupytext pytest] + podman, podman-compose, git, postgresql. Versionar `flake.lock`. Generar `requirements.txt` con versiones (la rúbrica lo exige aunque se use Nix). `.env.example` versionado, `.env` ignorado. Versionar `models/model.pkl` y `data/raw/`.
Notebooks: par jupytext (`jupytext --set-formats ipynb,py:percent notebooks/*.ipynb`); los entregables `.ipynb` deben quedar **ejecutados**.

# README final (obligatorio, en español)
1 Descripción · 2 Arquitectura (diagrama + decisiones) · 3 Estructura · 4 Decisiones de limpieza (tabla Problema→Decisión→Justificación) · 5 Feature engineering (features, exclusiones, fuga, split, métricas) · 6 Kafka y contrato (tabla + JSON ejemplo + estados) · 7 Esquema BD (ER + grano) · 8 Dashboard y KPIs (+screenshots) · 9 Ejecución · 10 Limitaciones.
Ejecución de referencia: `nix develop` → `podman-compose up -d kafka kafka-init postgres` → batch (DAG o `python -m src.harmonize && python -m src.train`) → `python kafka/consumer.py` → `python kafka/producer.py --delay 0.5` → Superset `:8088`, Grafana `:3000`. **Probar el README desde un clon limpio.**

# Checklist de entrega
flake+lock+requirements · eda.ipynb · model_training.ipynb · model.pkl+metadata · compose (topic 3p/RF1) · producer · consumer · create_tables.sql+kpis.sql · estados inválidos demostrados · dashboard sobre Postgres con 4 KPIs + screenshots · tests del contrato · README verificado · ningún archivo de agentes ni secretos en el repo (`git ls-files | grep -Ei 'agents|claude|gemini|\.env'` debe devolver solo `.env.example`).

# Presentación (10-12 min)
Problema y datos 1 · EDA y limpieza 2 · Arquitectura 2 · **Demo en vivo** (producer→consumer→BD→dashboard, con eventos inválidos) 4 · KPIs y conclusiones 1 · Limitaciones 1. Grabar video de respaldo.
Limitaciones honestas: modelo simple, 782 observaciones, escala 2017 distinta, stream = datos históricos simulados.
