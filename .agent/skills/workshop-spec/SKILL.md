---
name: workshop-spec
description: Requisitos exactos del Workshop 3 (rúbrica, JSON del evento, contrato, estados, tablas, KPIs, entregables). Usar al verificar que algo cumple el enunciado o antes de entregar; evita releer el PDF.
---
# Enunciado condensado (fuente de verdad)
**Objetivo:** pipeline streaming ETL con Kafka + modelo de regresión preentrenado, predicciones en tiempo real, guardado en BD y dashboard. Se evalúa integración, no precisión.

## Rúbrica (proyecto 70 %, presentación 30 %)
Data Integration & Cleaning **1.0** · Feature Eng 0.5 · ML Pipeline 0.5 · Producer 0.5 · Consumer 0.5 · Contract & Validation 0.5 · DB Design & Loading 0.5 · Dashboard & KPIs 0.5 · Docs & Reproducibility 0.5.

## Parte A
EDA (nulos, duplicados, nombres, tipos, esquemas por año, outliers) → esquema unificado → limpieza justificada → features sin fuga → split sugerido 70/30 → métricas MAE/RMSE/R² → `model.pkl`. Si hay preprocesamiento aprendido: scikit-learn `Pipeline` serializado junto al modelo.

## Parte B
- Producer: registro a registro, JSON, key=country, retardo configurable, topic `happiness-events` (3p, RF1).
- Consumer (grupo `happiness-ml-team`), en orden: recibir → **guardar raw** → validar contrato → mismo orden/preprocesamiento del entrenamiento → cargar modelo → predecir → guardar resultado.
- Inválidos: guardados en raw con estado, saltados de la predicción, **no rompen** el pipeline.
- Validar: campos requeridos, faltantes, tipos, valores numéricos, features que exige el modelo.

## Contrato mínimo
`event_id` str no vacío único · `country` str no vacío (=key) · `year` int · `gdp,family,health,freedom,generosity,corruption` numéricos · `actual_happiness_score` numérico (solo evaluación) · `event_time` ISO-8601 (momento de emisión).

## Parte C (BD)
`raw_happiness_events` (staging, no es dimensión) · `fact_predictions` (prediction_id, raw_event_id, country_id, date_id, actual_score, predicted_score, prediction_error, prediction_timestamp; guardar también event_time) · `dim_country` · `dim_date`. PostgreSQL recomendado.
**KPIs mínimos:** 1) error promedio 2) predicciones por país 3) predicho vs real 4) tendencia en el tiempo. Dashboard consulta la BD.

## Entregables
Repo GitHub: notebooks ETL, producer/consumer, modelo serializado, SQL, dashboard (archivos/screenshots), requirements.txt, README.md **obligatorio** (descripción, arquitectura, decisiones de limpieza, de features, Kafka+contrato, esquema BD, dashboard, ejecución). Dashboard: screenshots + archivo/link + explicación de KPIs.
