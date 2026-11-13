# Guion de presentación — Workshop 3: Streaming ETL with Apache Kafka + ML

> **Duración sugerida:** 12–15 minutos (10 min proyecto + 3–5 min demo en vivo + preguntas)
> **Ponderación del taller:** Proyecto 70% · Presentación 30% (Claridad y estructura + Comunicación y profesionalismo)
> **Regla de oro:** el taller NO es de optimización de ML; es de **pipeline integrado, confiable y reproducible**. Toda frase del guion debe reforzar ese mensaje.

---

## 0. Antes de presentar (checklist técnico — 5 min)

```bash
./run.sh start-all        # Kafka (KRaft) + PostgreSQL arriba
./run.sh status           # todo verde
```

- [ ] Topic `happiness-events` creado (3 particiones, RF 1): `kafka-topics --describe --topic happiness-events`
- [ ] `models/model.pkl` y `model_metadata.json` existen
- [ ] Screenshots del dashboard listos en `dashboards/screenshots/` (plan B si falla Superset)
- [ ] README.md abierto en el navegador (es obligatorio entregarlo)
- [ ] Pestaña de GitHub del repo lista

---

## 1. Apertura — Contexto y objetivo (≈1 min)

**Qué decir:**

> "Buenos días. Somos el grupo G01 y este es nuestro Workshop 3: *Streaming ETL with Apache Kafka and Machine Learning*.
>
> El objetivo del taller era **diseñar e implementar un pipeline de ETL en streaming capaz de generar predicciones en tiempo real** usando Apache Kafka y un modelo de ML previamente entrenado.
>
> La idea central: pasar del ETL por lotes que ya veníamos haciendo (Airflow, Data Warehouse) a un **pipeline dirigido por eventos**, donde cada registro fluye por Kafka, se valida contra un contrato, se le aplica inferencia al instante y se persiste para analítica."

**Mostrar:** diagrama de arquitectura (README §2 o diapositiva). Señalar con el dedo las dos mitades: **Offline** (izq.) y **Streaming** (der.).

---

## 2. Arquitectura general (≈1.5 min)

**Qué decir:**

> "Nuestra arquitectura replica la pedida en el enunciado, en dos procesos:
>
> **Proceso offline (Parte A):** partimos de los 5 CSV históricos de World Happiness (2015–2019) → EDA + limpieza + armonización de esquemas → feature engineering → entrenamiento de una regresión lineal → serialización de **todo** (imputer + scaler + modelo) en un solo `sklearn.Pipeline` guardado en `model.pkl`.
>
> **Proceso streaming (Parte B):** el producer lee los registros históricos uno a uno y los publica como eventos JSON en el topic `happiness-events` → el consumer los recibe en el grupo `happiness-ml-team` y ejecuta el flujo **raw → validar → features → predecir → guardar → commit**.
>
> **Parte C:** los resultados viven en PostgreSQL (modelo dimensional con tabla raw + fact + dimensiones) y el dashboard los consulta **desde las vistas SQL, nunca desde CSV**."

**Mostrar:** esquema de carpetas del repo rápido (`data/`, `notebooks/`, `kafka/`, `models/`, `sql/`, `dashboards/`, `airflow/`) para probar que la estructura sigue la recomendada.

---

## 3. Parte A — EDA, limpieza y armonización (≈2 min) ← *criterio con mayor peso (1.0)*

**Qué decir:**

> "El criterio con más peso es la integración y limpieza de datos, y arranca con un problema real: **los 5 CSV no comparten esquema**. Nombres de columnas distintos (por ejemplo `Family` vs `Family..GDP per capita.`), tipos distintos y valores faltantes.
>
> En el notebook de EDA analizamos valores nulos, duplicados, nombres inconsistentes, tipos, diferencias de esquema entre años y outliers. De ahí salen nuestras **decisiones de limpieza, todas justificadas** en el README §4:
>
> 1. **Esquema unificado** → un solo módulo `src/harmonize.py` con nombres canónicos en snake_case (`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`, `happiness_score`).
> 2. **Normalización por año**: las columnas de scores dependen del año en estos datasets → se normalizan a un rango comparable.
> 3. **Nulos**: se eliminan solo cuando son pocos y verificablemente incompletos; el resultado final quedó en **≈782 filas, 0 nulos, con (country, year) único**.
> 4. Todo se valida con **Great Expectations**: si el perfil de calidad no pasa, el pipeline no avanza.
>
> El resultado es `data/processed/unified_happiness.parquet`, un único dataset listo para ML."

**Mostrar:**
- `notebooks/eda.py` (par jupytext del notebook) → celdas de perfiles y esquemas por año.
- Comando de verificación corto (1 solo):
  ```bash
  python -c "import pandas as pd; df=pd.read_parquet('data/processed/unified_happiness.parquet'); print(df.shape, df.isna().sum().sum(), df.duplicated(['country','year']).sum())"
  ```
  → debe imprimir algo como ` (782, …) 0 0`.

---

## 4. Feature engineering y modelo (≈1.5 min)

**Qué decir:**

> "Para el modelo usamos **6 features fijas, en un orden fijo**: `gdp, family, health, freedom, generosity, corruption`. El target es `happiness_score`.
>
> Tres decisiones importantes:
>
> 1. **Sin target leakage**: `actual_happiness_score` llega en el evento pero **jamás se usa como feature** — solo para evaluación. El contrato lo garantiza.
> 2. **Preprocesamiento aprendido serializado con el modelo**: `SimpleImputer(median) + StandardScaler + LinearRegression` dentro de un único `Pipeline` de scikit-learn. Así el consumer aplica *exactamente* el mismo preprocesamiento que el entrenamiento.
> 3. **Split por grupo de país** (`GroupShuffleSplit`, 70/30, random_state=42) para evitar que un país aparezca en train y test a la vez.
>
> Métricas finales (`model_metadata.json`): **MAE 0.4318 · RMSE 0.5619 · R² 0.7838**. Comparamos contra Random Forest y split aleatorio y elegimos la regresión lineal con split por grupos. Recuerden: **el foco es la integración del pipeline, no maximizar la precisión** — un modelo simple y reproducible es exactamente lo pedido."

**Mostrar:** `models/model_metadata.json` (las 4 líneas de métricas + el `pipeline` declarado).

---

## 5. Parte B — Kafka: producer, contrato y consumer (≈4 min) ← *3 criterios de 0.5 c/u*

### 5.1 Producer (≈1 min)

**Qué decir:**

> "El producer lee `data/streaming/stream_events.csv` **registro por registro**, serializa a JSON y publica al topic `happiness-events` con **key = country**, que es lo que garantiza particionado consistente por país. Tiene delay configurable (`--delay`, 0.5 s por defecto) para simular la llegada en el tiempo, `--limit` para demos cortas y `--run-id` para trazabilidad.
>
> El topic cumple el requerimiento: **3 particiones, replication factor 1** (entorno local de broker único)."

**Mostrar:** el comando real que correrá en vivo:
```bash
python kafka/producer.py --limit 15 --delay 0.3 --inject-invalid
```

### 5.2 Event contract (≈1 min)

**Qué decir:**

> "Antes del consumer definimos el **contrato del evento** en un solo módulo, `src/contract.py`, que comparten producer, consumer y tests. Valida:
>
> - campos requeridos presentes y no nulos (`event_id`, `country`, `year`, features, `actual_happiness_score`, `event_time`);
> - tipos correctos (`year` entero, features numéricas, `event_time` ISO-8601);
> - rangos de dominio (features ≥ 0, score en rango válido);
> - y distingue **`INVALID_SCHEMA`** (falta un campo / tipo mal) de **`INVALID_VALUES`** (el valor está fuera de dominio).
>
> Filosofía clave del enunciado: *Kafka transporta el registro, pero la aplicación es responsable de interpretar y validar los campos de negocio*. Y un inválido **nunca tumba el pipeline**."

**Mostrar:** `pytest tests/test_contract.py` en una terminal aparte (verde).

### 5.3 Consumer (≈2 min) — pieza central

**Qué decir:**

> "El consumer trabaja en el grupo `happiness-ml-team` con **auto-commit desactivado**: el offset se confirma solo después de persistir, así no perdemos eventos si algo falla a mitad de mensaje.
>
> El orden por mensaje es crítico y cumple el requisito del enunciado:
>
> 1. **Guardar el evento crudo tal cual llegó** en `raw_happiness_events` — *antes* de cualquier transformación. Así queda la trazabilidad, la auditoría y la posibilidad de reprocesar si cambian las reglas.
> 2. Validar contra el contrato.
> 3. Construir las features **en el mismo orden fijo** de entrenamiento.
> 4. Predecir con `model.pkl`.
> 5. Guardar el resultado en `fact_predictions`, enlazado a la raw por `raw_event_id`.
> 6. Recién ahí hacer commit del offset.
>
> Los registros inválidos **se guardan en la tabla raw con su estado** (`INVALID_SCHEMA` / `INVALID_VALUES`), se saltan la predicción y el consumer sigue vivo. Si la predicción falla, se marca `PREDICTION_ERROR`. Estados posibles: `RECEIVED → VALID | INVALID_SCHEMA | INVALID_VALUES | PREDICTION_ERROR`.
>
> Además expone métricas en `/metrics` para Prometheus."

**Mostrar (demo en vivo, paso a paso):**

Terminal A:
```bash
python kafka/consumer.py
```
Terminal B:
```bash
python kafka/producer.py --limit 15 --delay 0.3 --inject-invalid
```

Luego, la prueba de humo (1 comando):
```sql
SELECT processing_status, COUNT(*) FROM raw_happiness_events GROUP BY 1;
```
→ Se ven **todas** las categorías, incluyendo las 10 inválidas inyectadas (falta de campo, year no entero, feature no numérica, gdp negativo, score fuera de rango, event_time no ISO, JSON malformado…).

Y el vínculo raw → fact:
```sql
SELECT COUNT(*) FROM fact_predictions;          -- debe igualar los VALID
SELECT COUNT(*) FROM fact_predictions f
JOIN raw_happiness_events r USING (raw_event_id); -- trazabilidad completa
```

---

## 6. Parte C — Modelo dimensional y carga (≈1.5 min)

**Qué decir:**

> "El esquema analítico (`sql/create_tables.sql`, idempotente con `CREATE TABLE IF NOT EXISTS`) tiene:
>
> - **`raw_happiness_events`** — staging: el mensaje original + `processing_status`. No es dimensión, es para trazabilidad.
> - **`fact_predictions`** — `prediction_id, raw_event_id, country_id, date_id, model_id, actual_score, predicted_score, prediction_error, prediction_timestamp`.
> - **`dim_country`** y **`dim_date`** (mínimo requerido), más **`dim_model`** que agregamos justificando: permite comparar versiones de modelo en el dashboard.
>
> Cada predicción se enlaza a la raw con `raw_event_id`: **puedo rastrear cualquier predicción hasta exactamente el evento que la produjo**. Todo el proceso de inserción usa transacciones y la BD es PostgreSQL."

**Mostrar:** el `CREATE TABLE` de `fact_predictions` con las FKs visibles (pantalla 3 segundos).

---

## 7. Dashboard y KPIs (≈1.5 min)

**Qué decir:**

> "Los 4 KPIs mínimos pedidos están como **vistas de PostgreSQL** en `sql/kpis.sql`:
>
> | # | KPI | Vista |
> |---|-----|-------|
> | 1 | Error promedio de predicción (MAE/RMSE/bias) | `vw_kpi_avg_error` |
> | 2 | Predicciones por país | `vw_kpi_predictions_by_country` |
> | 3 | Predicho vs real | `vw_kpi_pred_vs_actual` |
> | 4 | Tendencia en el tiempo | `vw_kpi_trend_by_year` |
>
> + `vw_pipeline_status` como salud del pipeline.
>
> **El dashboard consulta estas vistas en PostgreSQL, jamás archivos CSV** — lo cual verán en el SQL de cada chart. Tenemos dos capas:
>
> 1. **Superset** (contenedor en el profile `dashboards` del compose) con el dashboard embebido.
> 2. `dashboards/kpi_charts.py`, que regenera los **screenshots** en `dashboards/screenshots/` desde las mismas vistas — son los que quedan como evidencia en el repo.
>
> Y lo más importante para el taller: **los gráficos cambian cuando entran eventos nuevos**. Después de cada corrida regenero los PNG y las cifras se mueven."

**Mostrar:** las 5 imágenes de `dashboards/screenshots/` (KPI1–KPI4 + pipeline status), 2–3 s cada una. Si Superset está arriba, mostrar el dashboard real y un refresh.

**Para probarlo en vivo (opcional, si hay tiempo):**
```bash
python dashboards/kpi_charts.py   # regenera los PNG desde las vistas
```

---

## 8. Airflow, reproducibilidad y documentación (≈1 min)

**Qué decir:**

> "El pipeline batch de la Parte A también quedó orquestado en **Airflow** con dos DAGs: el de entrenamiento normal y uno de **fallo controlado** para demostrar el manejo de errores.
>
> Reproducibilidad:
> - Entorno con **Nix** (`nix develop`) y `requirements.txt` fijado;
> - **`.env.example` versionado, ningún secreto en el repo**;
> - `./run.sh` unificado (`start-all`, `test`, `status`, `airflow-success`, `airflow-fail`) — el mismo comando funciona en Linux y hay `run.bat` para Windows;
> - Tests con pytest (`test_contract.py`, `test_harmonize.py`);
> - README **en inglés y en español** con descripción, arquitectura, decisiones de limpieza, feature engineering, contrato de eventos, esquema de BD, dashboard e instrucciones de ejecución — todo lo que pide el enunciado punto 9.
>
> Commits convencionales y pequeños, rama `main`: la trazabilidad del desarrollo también está en Git."

**Mostrar:** `git log --oneline | head -8` (demuestra evolución limpia) o la lista de secciones del README.

---

## 9. Cierre (≈30 s)

**Qué decir:**

> "Para cerrar, mapeemos con lo evaluado:
>
> - ✅ Integración y limpieza → esquema unificado, 782 filas, GE pasando
> - ✅ Feature engineering → 6 features justificadas, sin leakage, pipeline serializado
> - ✅ ML pipeline → `model.pkl` reproducible, MAE 0.43 / R² 0.78
> - ✅ Producer → JSON, key=country, delay configurable, topic 3p/RF1
> - ✅ Consumer → grupo `happiness-ml-team`, crudo-antes-de-transformar, commit al final
> - ✅ Contrato y validación → `src/contract.py` + tests, 5 estados de procesamiento
> - ✅ BD → raw + fact + dim_country/date/model, trazabilidad por `raw_event_id`
> - ✅ Dashboard → 4 KPIs + pipeline status desde vistas SQL
> - ✅ Documentación → README bilingual, scripts unificados, Nix
>
> Un pipeline de streaming **integrado, confiable y reproducible** — que era justo el objetivo del taller. Gracias; preguntas."

---

## Anexo A — Preguntas frecuentes y respuestas

| Pregunta | Respuesta sugerida |
|---|---|
| ¿Por qué regresión lineal si hay modelos mejores? | El enunciado dice explícitamente que el foco es la integración del pipeline, no la precisión. Comparémoslos igual: Linear R² 0.78 vs Random Forest 0.75 (con split por grupos) — la lineal gana además por simpleza y por ser el mismo objeto serializado en el Pipeline. |
| ¿Por qué el split por grupos y no aleatorio? | Evita data leakage entre países: un país en train y test a la vez infla la métrica. Con split aleatorio R² baja a 0.73 — se ve que el aleatorio era el optimista. |
| ¿Por qué guardar lo crudo antes de validar? | Requisito del enunciado + trazabilidad: si mañana cambia la reglas de validación o de predicción, puedo reprocesar desde `raw_happiness_events` sin volver a Kafka. |
| ¿Qué pasa si el consumer muere a mitad de un mensaje? | Auto-commit desactivado: el offset no se confirmó, así que al reiniciar el mensaje se re-entrega. La raw puede tener un duplicado controlado, pero **no se pierde** ningún evento. |
| ¿Por qué key=country? | Particionado estable: todos los eventos de un país van a la misma partición, lo que preserva el orden por país y distribuye la carga. |
| ¿Cómo prueban que un evento inválido no tumba el consumer? | Inyectamos 10 eventos rotos con `--inject-invalid` (JSON malformado, campo faltante, tipos, rangos, timestamp no ISO). Todos aparecen en la raw con su estado, ninguno genera predicción, y el consumer sigue recibiendo el resto. |
| ¿El dashboard lee CSV? | No. Las vistas `vw_*` son SQL sobre PostgreSQL; `kpi_charts.py` y Superset consumen esas vistas. |
| ¿Y la demo si falla el vivo? | Plan B: screenshots de `dashboards/screenshots/` + `logs/latest.log` + el SQL de verificación ejecutado en la terminal. |

## Anexo B — Tiempos (resumen)

| Sección | Tiempo |
|---|---|
| 1. Apertura | 1:00 |
| 2. Arquitectura | 1:30 |
| 3. EDA + limpieza | 2:00 |
| 4. Features + modelo | 1:30 |
| 5. Kafka (producer/contrato/consumer) + demo | 4:00 |
| 6. BD | 1:30 |
| 7. Dashboard | 1:30 |
| 8. Airflow + reproducibilidad | 1:00 |
| 9. Cierre | 0:30 |
| **Total** | **≈14:30** |
