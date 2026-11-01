# Workshop 3 — Streaming ETL con Kafka + ML (World Happiness 2015-2019)

> Guía paso a paso para construir el proyecto con tu stack: **Nix · Python · Airflow · Kafka · PostgreSQL · Podman · Great Expectations · Prometheus · Grafana · Superset**.
> Este documento está pensado para que lo uses como plan de trabajo y luego lo conviertas en el `README.md` final del repositorio (la rúbrica lo exige: descripción, arquitectura, limpieza, features, Kafka + contrato, esquema BD, dashboard, instrucciones de ejecución).

---

## 0. Qué te piden realmente (y qué no)

El taller **no evalúa la precisión del modelo**; evalúa un pipeline integrado, confiable y reproducible. Los pesos de la rúbrica te dicen dónde invertir tiempo:

| Criterio | Peso | Dónde se resuelve en esta guía |
|---|---|---|
| Data Integration & Cleaning | **1.0** | Fase 2 |
| Feature Engineering | 0.5 | Fase 3 |
| ML Pipeline | 0.5 | Fase 3 |
| Kafka Producer | 0.5 | Fase 6 |
| Kafka Consumer | 0.5 | Fase 7 |
| Event Contract & Validation | 0.5 | Fase 5 |
| Database Design & Loading | 0.5 | Fase 4 y 7 |
| Dashboard & KPIs | 0.5 | Fase 8 |
| Documentación & Reproducibilidad | 0.5 | Fase 0, 9 y 10 |

Proyecto = 70 %, presentación = 30 %. La limpieza e integración vale el doble que cualquier otro criterio: dedícale tiempo.

---

## 1. Hallazgos reales de tus CSV (úsalos en el EDA)

Perfilé los cinco archivos. Esto es lo que vas a encontrar, para que tu EDA y tus decisiones de limpieza estén justificadas con evidencia.

### 1.1 Esquemas distintos por año

| Concepto | 2015 | 2016 | 2017 | 2018 / 2019 | **Nombre unificado** |
|---|---|---|---|---|---|
| País | `Country` | `Country` | `Country` | `Country or region` | `country` |
| Score | `Happiness Score` | `Happiness Score` | `Happiness.Score` | `Score` | `happiness_score` |
| PIB | `Economy (GDP per Capita)` | igual | `Economy..GDP.per.Capita.` | `GDP per capita` | `gdp` |
| Apoyo social | `Family` | `Family` | `Family` | `Social support` | `family` |
| Salud | `Health (Life Expectancy)` | igual | `Health..Life.Expectancy.` | `Healthy life expectancy` | `health` |
| Libertad | `Freedom` | `Freedom` | `Freedom` | `Freedom to make life choices` | `freedom` |
| Generosidad | `Generosity` | `Generosity` | `Generosity` | `Generosity` | `generosity` |
| Corrupción | `Trust (Government Corruption)` | igual | `Trust..Government.Corruption.` | `Perceptions of corruption` | `corruption` |
| Región | ✅ | ✅ | ❌ | ❌ | `region` (ver 1.3) |
| Columnas solo en algunos años | `Standard Error`, `Dystopia Residual` | `Lower/Upper Confidence Interval`, `Dystopia Residual` | `Whisker.high/low`, `Dystopia.Residual` | — | **se descartan** (ver 1.4) |
| Año | no existe en el archivo | | | | `year` (se toma del nombre del archivo) |

Además: `Happiness Rank` / `Overall rank` cambia de nombre y es derivado del score.

### 1.2 Calidad de datos

- Filas por año: 158 / 157 / 155 / 156 / 156 → **782 filas** en total.
- **Duplicados:** 0 filas duplicadas y 0 duplicados `(country, year)`.
- **Nulos:** un solo valor faltante en todo el conjunto: `corruption` de **United Arab Emirates en 2018**.
- **Tipos:** todos los indicadores son `float64` (el problema real son los nombres, no los tipos). Aun así, documenta y fuerza los tipos en el esquema unificado.
- **Ceros exactos** en componentes (`health`, `gdp`, etc.; p. ej. Sierra Leone, Lesotho 2017, Swaziland 2019). Son valores publicados en el mínimo de la escala, no nulos disfrazados → no los elimines; márcalos en el EDA y justifica.
- **Posible outlier:** `generosity` llega a ≈0.838 (2017), muy por encima del resto (media ≈0.22). Detéctalo con IQR/z-score, **no lo borres**: es un dato real de un país.
- **Escalas:** las columnas viven en rangos similares (≈0–2 para gdp, ≈0–1.6 para family, etc.), pero 2017 usa un cálculo ligeramente distinto al de 2018-19. Anótalo como limitación en el README.

### 1.3 Nombres de países inconsistentes (el problema más importante)

Hay ~170 nombres crudos distintos pero solo 141 países aparecen en los 5 años. Parte de la diferencia son **el mismo país escrito distinto**:

| Variantes encontradas | Nombre canónico sugerido |
|---|---|
| `Trinidad & Tobago` / `Trinidad and Tobago` | `Trinidad and Tobago` |
| `Hong Kong S.A.R., China` / `Hong Kong` | `Hong Kong` |
| `Taiwan Province of China` / `Taiwan` | `Taiwan` |
| `Macedonia` / `North Macedonia` | `North Macedonia` |
| `North Cyprus` / `Northern Cyprus` | `North Cyprus` |
| `Somaliland Region` / `Somaliland region` | `Somaliland region` |

Haz un diccionario `COUNTRY_ALIASES` versionado (CSV o YAML en `data/reference/`), aplícalo **antes** del merge y luego verifica con `groupby('country').year.nunique()` que no queden variantes. Si no lo haces, `dim_country` quedará con duplicados y el KPI "predicciones por país" saldrá mal.

> `region` solo existe en 2015-2016. Construye `dim_country(country, region)` a partir de esos dos años y propágalo. **No uses `region` como feature**: el contrato del evento no la incluye, así que el consumer no podría calcularla en streaming (ver Fase 3).

### 1.4 Fuga de información (target leakage)

El score de la fuente es la **suma** de los seis componentes + `Dystopia Residual`. Por eso:

- `Dystopia Residual`, `Standard Error`, `Whisker.*`, `*Confidence Interval` y `Happiness Rank` se **excluyen**: son derivados del score o de su incertidumbre.
- `actual_happiness_score` viaja en el evento **solo para evaluar**, jamás entra al modelo (lo exige el contrato).

### 1.5 Línea base esperada (para saber si tu pipeline "huele bien")

Con los 6 features y un `Pipeline(imputer → scaler → modelo)` obtuve, como referencia:

| Modelo / split | MAE | RMSE | R² |
|---|---|---|---|
| LinearRegression, split aleatorio 70/30 | ≈0.45 | ≈0.58 | ≈0.73 |
| LinearRegression, split por país 70/30 | ≈0.39 | ≈0.52 | ≈0.80 |
| RandomForest, split aleatorio 70/30 | ≈0.41 | ≈0.52 | ≈0.78 |

Correlación con el score: `gdp` 0.79, `health` 0.74, `family` 0.65, `freedom` 0.55, `corruption` 0.40, `generosity` 0.14. Si te sale R² ≈ 0.99 sospecha de fuga; si sale < 0.5 sospecha de un error de alineación de columnas.

---

## 2. Arquitectura recomendada

```
                     OFFLINE (batch, orquestado por Airflow)
 CSV 2015-2019 → ingest → harmonize → Great Expectations → features/split → train → model.pkl
                                  │                                   │
                                  └─► data/processed/unified.parquet  └─► data/streaming/stream_events.csv

                     STREAMING (servicios de larga duración)
 producer.py ──► Kafka topic `happiness-events` (3 particiones, key = country)
                         │
                         ▼
                  consumer.py  (group `happiness-ml-team`)
                   1. INSERT raw_happiness_events (status RECEIVED)
                   2. validar contrato → VALID / INVALID_SCHEMA / INVALID_VALUES
                   3. features en el mismo orden del entrenamiento
                   4. model.pkl → predicción (o PREDICTION_ERROR)
                   5. INSERT fact_predictions (+ dim_country, dim_date, dim_model)
                   6. commit manual del offset
                         │                       │
                         ▼                       ▼
                  PostgreSQL ──► Superset   /metrics ──► Prometheus ──► Grafana
                  (KPIs de negocio)         (salud operativa del pipeline)
```

**Decisiones de diseño que debes explicar en la presentación:**

1. **Airflow orquesta el batch (Parte A), no el consumer.** El consumer es un proceso continuo; Airflow está pensado para tareas con inicio y fin. El consumer corre como servicio (compose o proceso en tu shell de Nix).
2. **Superset = dashboard de negocio** (lo que pide la rúbrica, consultando PostgreSQL). **Grafana = salud del pipeline** (eventos/seg, latencia, inválidos, lag) leyendo Prometheus. No son redundantes: responden preguntas distintas. Si el tiempo aprieta, prioriza Superset; Grafana es bonus.
3. **Great Expectations valida el dataset batch** (Fase 2). La validación **por evento** se hace con una función Python ligera (Fase 5): GE por mensaje sería lento y excesivo.
4. **`confluent-kafka` en lugar de `kafka-python`:** está mejor mantenida, es más rápida, tiene commits manuales y entrega confiable (`delivery report`). Está disponible en nixpkgs.

---

## 3. Estructura del repositorio

Es la sugerida por el taller, con extras justificados (`src/`, `airflow/`, `monitoring/`, `great_expectations/`, `docs/`):

```
project/
├── flake.nix  flake.lock
├── docker-compose.yml          # funciona con podman-compose
├── requirements.txt            # exigido por la rúbrica (aunque uses Nix)
├── README.md
├── .gitignore  .env.example
├── data/
│   ├── raw/                    # 2015.csv … 2019.csv (intactos)
│   ├── reference/              # country_aliases.csv, country_region.csv
│   ├── processed/              # unified_happiness.parquet / .csv
│   └── streaming/              # stream_events.csv (+ invalid_events.csv para la demo)
├── notebooks/
│   ├── eda.ipynb
│   └── model_training.ipynb
├── src/
│   ├── config.py               # FEATURES, TOPIC, rangos, DSN (desde variables de entorno)
│   ├── harmonize.py            # renombrado + alias de países + tipos + merge
│   ├── contract.py             # validación del evento (Fase 5)
│   └── metrics.py              # contadores Prometheus
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── airflow/dags/
│   └── happiness_batch_training.py
├── great_expectations/         # suite + data docs
├── models/
│   ├── model.pkl
│   └── model_metadata.json     # versión, features, métricas, fecha
├── sql/
│   ├── create_tables.sql
│   └── kpis.sql
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/                # provisioning de datasources/dashboards
├── dashboards/                 # export de Superset + screenshots
├── docs/                       # diagrama, decisiones de limpieza
└── tests/
    └── test_contract.py
```

---

## 4. Plan de trabajo por fases

Cada fase termina con un **criterio de "hecho"** verificable. No pases a la siguiente sin cumplirlo.

### Fase 0 — Entorno reproducible (Nix + Podman + Git)

**Pasos**

1. `git init`, crea el repo en GitHub, añade `.gitignore` (`__pycache__`, `.env`, `.ipynb_checkpoints`, volúmenes de datos). **Sí** versiona `models/model.pkl` y `data/raw/` (lo piden como entregable y son pequeños).
2. Crea `flake.nix` con un `devShell`:

```nix
{
  description = "Workshop 3 - Streaming ETL con Kafka y ML";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";   # fija con flake.lock

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      py = pkgs.python312.withPackages (ps: with ps; [
        pandas numpy scikit-learn joblib pyarrow
        matplotlib seaborn
        confluent-kafka sqlalchemy psycopg2
        great-expectations prometheus-client
        jupyterlab ipykernel pytest
      ]);
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [ py pkgs.podman pkgs.podman-compose pkgs.git pkgs.postgresql ];
        shellHook = ''
          export PYTHONPATH=$PWD
          echo "Entorno listo: python, podman, psql"
        '';
      };
    };
}
```

3. `nix develop` y verifica `python -c "import confluent_kafka, sklearn, great_expectations"`.
4. Genera `requirements.txt` con las versiones reales (`pip freeze` dentro del shell, o escríbelo a mano con las librerías clave). Así quien no use Nix también puede reproducir.
5. **Airflow:** instálalo aparte (imagen `apache/airflow` en compose, o un venv con el *constraints file* oficial). No lo mezcles con el shell de Nix: Airflow tiene muchas dependencias fijadas que suelen chocar con las de ML. Monta `airflow/dags/` y el repo como volumen, e instala `great-expectations`, `scikit-learn` y `pandas` en esa imagen.

**Notas de Podman (rootless)**

- Usa nombres de imagen completos (`docker.io/...`) para evitar el prompt de registro.
- Si tienes SELinux activo, añade `:Z` a los volúmenes bind.
- Kafka necesita **dos listeners** (uno interno para contenedores, otro externo para tu host); es el error más común (ver Fase 1).

**Hecho cuando:** otra persona puede clonar el repo, ejecutar `nix develop` y tener todo el entorno.

---

### Fase 1 — Infraestructura con `podman-compose`

Servicios mínimos: `kafka` (modo KRaft, sin Zookeeper), `kafka-init` (crea el topic), `postgres`. Opcionales: `prometheus`, `grafana`, `superset`, `airflow`.

```yaml
services:
  kafka:
    image: docker.io/apache/kafka:3.8.0
    container_name: kafka
    ports: ["29092:29092"]          # listener para tu host
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1"]
      interval: 10s
      retries: 10

  kafka-init:
    image: docker.io/apache/kafka:3.8.0
    depends_on: { kafka: { condition: service_healthy } }
    restart: "no"
    entrypoint: ["/bin/sh","-c"]
    command:
      - /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists
        --topic happiness-events --partitions 3 --replication-factor 1

  postgres:
    image: docker.io/library/postgres:16
    environment:
      POSTGRES_USER: happiness
      POSTGRES_PASSWORD: happiness
      POSTGRES_DB: happiness
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./sql/create_tables.sql:/docker-entrypoint-initdb.d/01_create_tables.sql:Z

volumes:
  pgdata:
```

Notas:

- Las versiones de imagen (`apache/kafka:3.8.0`, `postgres:16`) son un punto de partida razonable; verifica que existan en tu registro y fíjalas.
- **Superset:** la imagen oficial no trae el driver de PostgreSQL. Crea un `Dockerfile` mínimo (`FROM apache/superset` + `pip install psycopg2-binary`) y construye esa imagen. Inicialización: `superset db upgrade`, `superset fab create-admin`, `superset init`.
- **Prometheus:** `monitoring/prometheus.yml` con un *scrape job* a `host.containers.internal:8000` (el puerto donde el consumer expone `/metrics`).
- Levanta por partes para ahorrar RAM: Kafka + Postgres primero; Airflow, Superset y Grafana solo cuando los necesites (usa `profiles:` si tu versión de `podman-compose` lo soporta).

**Verificación**

```bash
podman-compose up -d kafka kafka-init postgres
podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic happiness-events
psql postgresql://happiness:happiness@localhost:5432/happiness -c '\dt'
```

**Hecho cuando:** el topic `happiness-events` existe con **3 particiones y réplica 1** y Postgres tiene las tablas.

---

### Fase 2 — EDA, limpieza y armonización (peso 1.0)

Notebook `notebooks/eda.ipynb` + módulo reutilizable `src/harmonize.py` (el notebook lo importa; así Airflow ejecuta exactamente la misma lógica).

**Paso a paso**

1. **Carga cruda** de los 5 archivos, sin modificar, e imprime por año: `shape`, `dtypes`, nulos, duplicados. Guarda estas tablas: son tu "Data quality observations".
2. **Comparación de esquemas**: tabla año × columna (la de la sección 1.1) generada con código.
3. **Mapeo de columnas** con un diccionario por año → nombres unificados (`snake_case`). Añade `year` desde el nombre del archivo.
4. **Selección de columnas**: conserva `country, year, gdp, family, health, freedom, generosity, corruption, happiness_score` (+ `region` para la dimensión).
5. **Normalización de países**: `strip()`, colapsar espacios, aplicar `COUNTRY_ALIASES`. Valida que `country` no quede con variantes.
6. **Tipos**: `country` y `region` → `string`; `year` → `int`; resto → `float64`. Falla con error claro si algo no convierte (`pd.to_numeric(..., errors="raise")`).
7. **Faltantes:** imputa `corruption` de UAE-2018. Recomendación defendible: **imputar con el promedio de UAE en 2017 y 2019** en la limpieza, y dejar un `SimpleImputer(median)` en el `Pipeline` del modelo como red de seguridad ante eventos futuros. Justifícalo: eliminar la fila pierde un país completo de 2018 y un solo dato no justifica descartar 5 indicadores válidos.
8. **Duplicados:** verifica `(country, year)` único; si apareciera uno, conserva el primero y registra.
9. **Outliers:** IQR por columna y boxplots; reporta (p. ej. `generosity` 2017) pero **conserva**. Justificación: son valores reales de la fuente, el modelo es simple y el objetivo no es optimizar.
10. **Merge:** `pd.concat` de los cinco años (formato largo, una fila por país-año). No hagas un merge ancho por país: rompería el grano.
11. **Validación con Great Expectations** sobre el dataset unificado (ver abajo).
12. **Persiste** `data/processed/unified_happiness.parquet` (y CSV por comodidad).

**Suite mínima de Great Expectations**

| Expectation | Columna |
|---|---|
| `expect_table_columns_to_match_ordered_list` | todas |
| `expect_column_values_to_not_be_null` | `country`, `year`, `happiness_score` y los 6 features |
| `expect_column_values_to_be_in_set` | `year` ∈ {2015…2019} |
| `expect_column_values_to_be_between` | `gdp` 0–2.5 · `family` 0–2 · `health` 0–1.5 · `freedom` 0–1 · `generosity` 0–1 · `corruption` 0–1 · `happiness_score` 0–10 |
| `expect_compound_columns_to_be_unique` | (`country`, `year`) |
| `expect_column_values_to_be_of_type` | tipos del esquema |

> La API de GE cambió bastante entre 0.x y 1.x. Revisa la documentación de la versión que instale tu flake y fíjala en `requirements.txt`.

**Tabla de decisiones (cópiala al README final):** *Problema → Decisión → Justificación → Impacto* (una fila por cada ítem de arriba).

**Hecho cuando:** el unificado tiene ≈782 filas, 0 nulos, `(country, year)` único, y la suite de GE pasa.

---

### Fase 3 — Feature engineering y modelo (peso 0.5 + 0.5)

Notebook `notebooks/model_training.ipynb`.

**Features (6, en este orden fijo):** `gdp, family, health, freedom, generosity, corruption`.

Justificación:
- Son exactamente los campos que trae el evento del contrato, así el consumer puede reproducir el input sin información extra.
- Tienen relación lógica y estadística con el score (ver correlaciones en 1.5).
- Excluidos por **fuga**: `Dystopia Residual`, `rank`, `std error`, intervalos/whiskers.
- Excluidos por **no estar disponibles en streaming**: `region` (si la quisieras, tendría que ir en el contrato).
- `year` y `country`: no los uses como features. `country` es de alta cardinalidad y haría que el modelo memorice; `year` solo aporta tendencia temporal que un taller de pipeline no necesita. Si quisieras demostrar manejo de categóricas, hazlo en el EDA y explica por qué no entra.
- Sobre `generosity`: correlaciona poco (0.14). Puedes mantenerla (el contrato la exige) y comentar que aporta poco; es un buen punto para la presentación.

**Pipeline reproducible (todo el preprocesamiento viaja dentro del `.pkl`):**

```python
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.compose import ColumnTransformer

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

pipe = Pipeline([
    ("prep", ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("sc", StandardScaler())]), FEATURES)
    ], remainder="drop")),
    ("model", LinearRegression()),
])
```

Con un `ColumnTransformer` por nombre de columna, el modelo selecciona y ordena las columnas del DataFrame que le pases; aun así, el consumer debe construir el DataFrame con `FEATURES` explícito.

**Split 70/30 — el punto que más se pasa por alto:** un país aparece hasta 5 veces (una por año). Un `train_test_split` aleatorio pone al mismo país en train y test, lo que normalmente altera la estimación del error. Usa **`GroupShuffleSplit(test_size=0.3, groups=country, random_state=42)`** y documenta por qué. (En mis pruebas el split por país no empeoró las métricas del modelo lineal, pero es el diseño metodológicamente correcto; compáralo tú en tu notebook y reporta ambos.)

**Entrenamiento y guardado**

1. Entrena con el 70 %; calcula **MAE, RMSE, R²** en el 30 %.
2. (Opcional) compara LinearRegression vs RandomForest con una tabla; elige el más simple si la diferencia es pequeña.
3. Guarda `joblib.dump(pipe, "models/model.pkl")` y un `models/model_metadata.json` con: `model_version`, `algorithm`, `features`, métricas, fecha y versión de scikit-learn.
4. **Genera el stream** a partir del conjunto de **test**: `data/streaming/stream_events.csv` con las columnas del contrato. Razón: si haces streaming del dataset completo, el 70 % de las predicciones serán sobre datos de entrenamiento y tu KPI de error será optimista. Alternativa válida: transmitir todo, pero entonces **dilo explícitamente** en el README.
5. Genera también `data/streaming/invalid_events.csv` (o una bandera `--inject-invalid` en el producer) con eventos deliberadamente malos para demostrar los estados `INVALID_*` en vivo.

**Hecho cuando:** `joblib.load("models/model.pkl").predict(df[FEATURES])` funciona en una sesión limpia y las métricas están en `model_metadata.json`.

---

### Fase 4 — Modelo dimensional y base de datos (peso 0.5)

#### Grano (granularity) — decláralo explícitamente

| Tabla | Grano |
|---|---|
| `raw_happiness_events` | **Un mensaje Kafka consumido** (identificado por `topic + partition + offset`), válido o no. |
| `fact_predictions` | **Una predicción por evento válido**: un país-año emitido por el producer, predicho por una versión del modelo. |
| `dim_country` | Un país. |
| `dim_date` | Un día calendario (derivado de `event_time`). |
| `dim_model` | Una versión del modelo (dimensión extra justificada: trazabilidad y comparar versiones). |

#### Esquema propuesto (`sql/create_tables.sql`)

```sql
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    kafka_topic       TEXT        NOT NULL,
    kafka_partition   INT         NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    kafka_key         TEXT,
    kafka_timestamp   TIMESTAMPTZ,
    event_id          TEXT,                     -- NULL si el evento no lo trae
    raw_payload       TEXT        NOT NULL,     -- mensaje original decodificado, sin tocar
    payload_json      JSONB,                    -- NULL si no es JSON válido
    processing_status TEXT        NOT NULL DEFAULT 'RECEIVED'
        CHECK (processing_status IN ('RECEIVED','VALID','INVALID_SCHEMA','INVALID_VALUES','PREDICTION_ERROR')),
    error_detail      TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)   -- idempotencia ante reentregas
);
CREATE INDEX IF NOT EXISTS ix_raw_event_id ON raw_happiness_events(event_id);
CREATE INDEX IF NOT EXISTS ix_raw_status   ON raw_happiness_events(processing_status);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE,
    region       TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id   INT PRIMARY KEY,            -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year      SMALLINT NOT NULL,
    quarter   SMALLINT NOT NULL,
    month     SMALLINT NOT NULL,
    day       SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_model (
    model_id      SERIAL PRIMARY KEY,
    model_version TEXT NOT NULL UNIQUE,
    algorithm     TEXT NOT NULL,
    trained_at    TIMESTAMPTZ,
    mae           NUMERIC(8,4),
    rmse          NUMERIC(8,4),
    r2            NUMERIC(8,4)
);

CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT NOT NULL UNIQUE REFERENCES raw_happiness_events(raw_event_id),
    country_id           INT    NOT NULL REFERENCES dim_country(country_id),
    date_id              INT    NOT NULL REFERENCES dim_date(date_id),
    model_id             INT    NOT NULL REFERENCES dim_model(model_id),
    observation_year     SMALLINT NOT NULL,          -- año histórico (degenerate dimension)
    actual_score         NUMERIC(6,3) NOT NULL,
    predicted_score      NUMERIC(6,3) NOT NULL,
    prediction_error     NUMERIC(6,3) NOT NULL,      -- predicho − real (con signo)
    abs_prediction_error NUMERIC(6,3) NOT NULL,
    event_time           TIMESTAMP NOT NULL,         -- cuándo emitió el producer
    prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_country ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS ix_fact_year    ON fact_predictions(observation_year);
```

**Decisiones a justificar en el README**

- **`raw_payload` como TEXT + `payload_json` como JSONB:** el taller exige guardar el mensaje "exactamente como llegó". Un JSON inválido (o sin `event_id`) no cabría en columnas tipadas; el TEXT lo conserva siempre.
- **`UNIQUE (topic, partition, offset)`:** si Kafka reentrega un mensaje tras una caída, `INSERT ... ON CONFLICT DO NOTHING` evita duplicados (semántica *at-least-once* + idempotencia).
- **`prediction_error` con signo** (predicho − real) para ver sesgo; `abs_prediction_error` para el KPI de error promedio.
- **Dos "tiempos":** `observation_year` es el tiempo histórico (sirve para tendencias); `event_time` / `prediction_timestamp` son el tiempo del pipeline (sirven para latencia y volumen). En la simulación todos los `event_time` caen en el mismo día, por lo que la **tendencia con sentido de negocio se hace por `observation_year`**; explícalo en el dashboard.
- **`dim_date`** se alimenta (upsert) desde el `event_time` del evento; si prefieres poblarla de antemano, hazlo con `generate_series`.
- **Estado de la raw:** `RECEIVED` (transitorio) → `VALID` | `INVALID_SCHEMA` | `INVALID_VALUES` | `PREDICTION_ERROR`. Un evento `VALID` es el que cumple el contrato; su predicción se verifica por la existencia de la fila en `fact_predictions`.
- **`dim_model`:** debes poblarla antes (o al arrancar el consumer) desde `model_metadata.json`; si no, el `INSERT` en `fact_predictions` fallará por la FK.

**Hecho cuando:** `create_tables.sql` corre limpio sobre una BD vacía, de forma repetible (`IF NOT EXISTS`).

---

### Fase 5 — Contrato del evento y validación (peso 0.5)

Defínelo **antes** de escribir producer/consumer, en `src/contract.py`, y úsalo desde ambos lados.

**Contrato**

| Campo | Regla | Estado si falla |
|---|---|---|
| `event_id` | string no vacío | `INVALID_SCHEMA` |
| `country` | string no vacío (y = key de Kafka) | `INVALID_SCHEMA` |
| `year` | entero (no bool) | `INVALID_SCHEMA` |
| `gdp, family, health, freedom, generosity, corruption` | numéricos (no bool), finitos, dentro de rango | tipo → `INVALID_SCHEMA`; rango/NaN → `INVALID_VALUES` |
| `actual_happiness_score` | numérico, finito, 0–10; **nunca** entra al modelo | igual que arriba |
| `event_time` | ISO-8601 parseable | `INVALID_VALUES` |
| Campo ausente / `null` / JSON ilegible | — | `INVALID_SCHEMA` |
| Error al predecir | excepción en `predict` | `PREDICTION_ERROR` |

Regla práctica: **forma/tipo/presencia → `INVALID_SCHEMA`; valor fuera de dominio → `INVALID_VALUES`**. Los rangos de la tabla son propuestos: ajústalos con los mínimos/máximos que observes en tu EDA.

```python
# src/contract.py
import math
from datetime import datetime

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
RANGES = {
    "gdp": (0, 2.5), "family": (0, 2.0), "health": (0, 1.5),
    "freedom": (0, 1.0), "generosity": (0, 1.0), "corruption": (0, 1.0),
    "actual_happiness_score": (0, 10),
}
REQUIRED = ["event_id", "country", "year", *FEATURES, "actual_happiness_score", "event_time"]
YEAR_RANGE = (2015, 2019)          # configurable

class ContractError(Exception):
    def __init__(self, status: str, detail: str):
        super().__init__(detail)
        self.status, self.detail = status, detail

def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)

def validate(p) -> dict:
    if not isinstance(p, dict):
        raise ContractError("INVALID_SCHEMA", "payload is not a JSON object")
    missing = [k for k in REQUIRED if p.get(k) is None]
    if missing:
        raise ContractError("INVALID_SCHEMA", f"missing/null fields: {missing}")
    for k in ("event_id", "country"):
        if not isinstance(p[k], str) or not p[k].strip():
            raise ContractError("INVALID_SCHEMA", f"{k} must be a non-empty string")
    if isinstance(p["year"], bool) or not isinstance(p["year"], int):
        raise ContractError("INVALID_SCHEMA", "year must be an integer")
    if not YEAR_RANGE[0] <= p["year"] <= YEAR_RANGE[1]:
        raise ContractError("INVALID_VALUES", f"year out of range: {p['year']}")
    for k, (lo, hi) in RANGES.items():
        v = p[k]
        if not _is_num(v):
            raise ContractError("INVALID_SCHEMA", f"{k} must be numeric")
        if not math.isfinite(v) or not lo <= v <= hi:
            raise ContractError("INVALID_VALUES", f"{k}={v} outside [{lo}, {hi}]")
    if not isinstance(p["event_time"], str):
        raise ContractError("INVALID_SCHEMA", "event_time must be a string")
    try:
        datetime.fromisoformat(p["event_time"])
    except ValueError:
        raise ContractError("INVALID_VALUES", "event_time is not ISO-8601")
    return p
```

Escribe `tests/test_contract.py` con un caso por fila de la tabla (campo faltante, string en vez de número, `NaN`, negativo, fecha mala, `null`, `bool`). Son pocas líneas y muestran rigor en la rúbrica.

> Python acepta `NaN` al hacer `json.loads`, por eso se comprueba `math.isfinite`.

**Hecho cuando:** los tests pasan y cada tipo de fallo produce el estado esperado.

---

### Fase 6 — Producer (peso 0.5)

Requisitos: leer registro por registro · JSON · **key = country** · retardo configurable · topic `happiness-events` (3 particiones, réplica 1).

```python
# kafka/producer.py
import argparse, json, time
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer

TOPIC = "happiness-events"

def on_delivery(err, msg):
    if err:
        print(f"[ERROR] {err}")
    else:
        print(f"[OK] {msg.key().decode()} -> p{msg.partition()} @{msg.offset()}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/streaming/stream_events.csv")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--delay", type=float, default=0.5)     # segundos entre eventos
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.bootstrap,
                  "enable.idempotence": True, "acks": "all"})
    df = pd.read_csv(args.file)
    if args.limit:
        df = df.head(args.limit)

    for i, row in enumerate(df.to_dict("records"), start=1):
        event = {
            "event_id": f"E{i:04d}",
            "country": row["country"],
            "year": int(row["year"]),
            **{k: float(row[k]) for k in ["gdp","family","health","freedom","generosity","corruption"]},
            "actual_happiness_score": float(row["happiness_score"]),
            "event_time": datetime.now().isoformat(timespec="seconds"),
        }
        p.produce(TOPIC, key=event["country"].encode(),
                  value=json.dumps(event).encode(), on_delivery=on_delivery)
        p.poll(0)
        time.sleep(args.delay)
    p.flush()

if __name__ == "__main__":
    main()
```

Puntos a explicar:
- **Key = country** → todos los eventos de un país caen en la misma partición → orden garantizado por país. (Con 3 particiones la distribución depende del hash de cada key; no esperes un reparto exactamente igual.)
- `event_id` único y reproducible; si ejecutas el producer dos veces, combina un prefijo de corrida (`run_id`) para no chocar.
- **Importante:** `event_time` es el momento de emisión, **no** la fecha histórica (esa es `year`).

**Prueba rápida:** `podman exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic happiness-events --from-beginning --max-messages 5`.

**Hecho cuando:** los mensajes aparecen en el topic con key = país y repartidos en las 3 particiones.

---

### Fase 7 — Consumer con inferencia en tiempo real (peso 0.5)

Requisitos: grupo **`happiness-ml-team`** · guardar raw **antes** de validar/predecir · validar · mismo orden de features · cargar modelo · predecir · guardar · **los inválidos no rompen el pipeline**.

**Flujo por mensaje**

```
poll() → INSERT raw (RECEIVED) [commit]
       → json.loads        ─ error → INVALID_SCHEMA
       → validate(payload) ─ error → INVALID_SCHEMA / INVALID_VALUES
       → DataFrame(payload)[FEATURES] → model.predict
                           ─ excepción → PREDICTION_ERROR
       → upsert dim_country, dim_date; INSERT fact_predictions; UPDATE raw → VALID  [1 transacción]
       → consumer.commit(msg)   ← solo al final
```

```python
# kafka/consumer.py (esqueleto)
import json, joblib, pandas as pd
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
from src.contract import validate, ContractError, FEATURES

consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "happiness-ml-team",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,            # commit manual: sin pérdida de eventos
})
consumer.subscribe(["happiness-events"])
model = joblib.load("models/model.pkl")      # se carga UNA vez, no por mensaje
engine = create_engine("postgresql+psycopg2://happiness:happiness@localhost:5432/happiness")

def set_status(conn, raw_id, status, detail=None):
    conn.execute(text("UPDATE raw_happiness_events SET processing_status=:s, error_detail=:d WHERE raw_event_id=:i"),
                 {"s": status, "d": detail, "i": raw_id})

def process(msg):
    raw_text = msg.value().decode("utf-8", errors="replace")
    # 1) RAW primero, en su propia transacción
    with engine.begin() as conn:
        raw_id = conn.execute(text("""
            INSERT INTO raw_happiness_events
              (kafka_topic,kafka_partition,kafka_offset,kafka_key,raw_payload)
            VALUES (:t,:p,:o,:k,:raw)
            ON CONFLICT (kafka_topic,kafka_partition,kafka_offset) DO NOTHING
            RETURNING raw_event_id"""),
            {"t": msg.topic(), "p": msg.partition(), "o": msg.offset(),
             "k": msg.key().decode() if msg.key() else None, "raw": raw_text}).scalar()
    if raw_id is None:                       # reentrega: ya procesado
        return
    # 2) JSON + contrato
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as e:
        with engine.begin() as c: set_status(c, raw_id, "INVALID_SCHEMA", f"bad JSON: {e}")
        return
    try:
        validate(payload)
    except ContractError as e:
        with engine.begin() as c: set_status(c, raw_id, e.status, e.detail)
        return
    # 3) Predicción
    try:
        X = pd.DataFrame([payload])[FEATURES]          # mismo orden que en el entrenamiento
        pred = float(model.predict(X)[0])
    except Exception as e:
        with engine.begin() as c: set_status(c, raw_id, "PREDICTION_ERROR", str(e))
        return
    # 4) Carga transaccional: dims + fact + status VALID
    with engine.begin() as conn:
        ...  # upsert dim_country / dim_date, INSERT fact_predictions, payload_json y status = VALID

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error()); continue
    try:
        process(msg)
        consumer.commit(message=msg, asynchronous=False)
    except Exception as e:                  # fallo de infraestructura (p. ej. BD caída)
        print(f"[infra error] {e}")         # NO se hace commit → se reintenta tras reiniciar
```

> Este es un esqueleto, no código listo para producción: en el bloque `except` de infraestructura, añade reintento con backoff o termina el proceso para que el supervisor lo reinicie (si sigues leyendo el siguiente mensaje y haces commit después, el offset anterior quedaría cubierto igualmente). Además, el `event_id` y `payload_json` de la fila raw deben actualizarse en cuanto el JSON se parsee.

**Reglas de fiabilidad (explícalas en la presentación)**

- **Entrega *at-least-once* + idempotencia** (`ON CONFLICT` sobre `topic/partition/offset`) = efecto práctico *exactly-once* en la BD.
- **Commit del offset solo después de persistir.** Si el consumer se cae a la mitad, Kafka reentrega.
- **Diferenciar dos tipos de error:** un *evento inválido* es un resultado de negocio (se guarda con estado y se sigue); una *BD caída* es de infraestructura (no se hace commit y se reintenta con backoff).
- **Misma lógica de features** que el entrenamiento: importa `FEATURES` desde un único módulo (`src/contract.py`/`config.py`), no lo copies.
- **Metadata del modelo:** al arrancar, registra/obtiene `dim_model` desde `model_metadata.json` para poblar `model_id`.
- **Métricas Prometheus** (en `src/metrics.py`, con `prometheus_client`, puerto 8000):
  - `events_consumed_total{status}` (Counter)
  - `prediction_latency_seconds` (Histogram)
  - `prediction_abs_error` (Histogram)
  - opcional: lag por partición.

**Prueba de robustez obligatoria:** mezcla eventos válidos con `invalid_events.csv` y verifica que (a) todos aparezcan en `raw_happiness_events`, (b) los inválidos tengan el estado correcto, (c) no haya filas en `fact_predictions` para ellos y (d) el consumer siga vivo. Prueba también: matar el consumer a mitad, reiniciarlo y confirmar que no hay duplicados ni pérdidas.

**Hecho cuando:** `SELECT processing_status, count(*) FROM raw_happiness_events GROUP BY 1;` muestra todos los estados esperados y `count(raw VALID) = count(fact_predictions)`.

---

### Fase 8 — KPIs y dashboards (peso 0.5)

**El dashboard debe consultar la base de datos, no CSV** (explícito en el taller). Crea **vistas SQL** en `sql/kpis.sql` y conecta Superset a ellas.

```sql
-- KPI 1: error promedio (y sesgo)
CREATE OR REPLACE VIEW vw_kpi_avg_error AS
SELECT ROUND(AVG(abs_prediction_error),4) AS mae,
       ROUND(SQRT(AVG(prediction_error^2)),4) AS rmse,
       ROUND(AVG(prediction_error),4) AS bias,
       COUNT(*) AS n_predictions
FROM fact_predictions;

-- KPI 2: predicciones por país
CREATE OR REPLACE VIEW vw_kpi_predictions_by_country AS
SELECT c.country_name, c.region, COUNT(*) AS n_predictions,
       ROUND(AVG(f.abs_prediction_error),4) AS mae
FROM fact_predictions f JOIN dim_country c USING (country_id)
GROUP BY 1,2;

-- KPI 3: predicho vs real
CREATE OR REPLACE VIEW vw_kpi_pred_vs_actual AS
SELECT c.country_name, f.observation_year, f.actual_score, f.predicted_score, f.prediction_error
FROM fact_predictions f JOIN dim_country c USING (country_id);

-- KPI 4: tendencia por año de observación
CREATE OR REPLACE VIEW vw_kpi_trend_by_year AS
SELECT observation_year,
       ROUND(AVG(actual_score),3) AS avg_actual,
       ROUND(AVG(predicted_score),3) AS avg_predicted,
       ROUND(AVG(abs_prediction_error),3) AS mae
FROM fact_predictions GROUP BY 1 ORDER BY 1;

-- Extra de calidad: estado de eventos (muestra la validación en acción)
CREATE OR REPLACE VIEW vw_pipeline_status AS
SELECT processing_status, COUNT(*) AS n FROM raw_happiness_events GROUP BY 1;
```

**Gráficos sugeridos en Superset**

| KPI | Gráfico |
|---|---|
| Error promedio | Big Number (MAE) + Big Number (bias) |
| Predicciones por país | Barras horizontales (top N) o mapa mundial |
| Predicho vs real | Scatter (x = real, y = predicho) con línea y = x |
| Tendencia | Líneas por `observation_year` (real vs predicho) |
| Salud del pipeline | Pie/barras de `vw_pipeline_status` |

**Grafana (opcional pero luce):** panel con `rate(events_consumed_total[1m])` por estado, latencia p95 y errores inválidos. Aprovecha para decir que *Superset responde "¿qué tan bien predice?" y Grafana "¿está sano el pipeline?"*.

**Entregables:** screenshots en `dashboards/`, export de Superset (`superset export-dashboards`) o instrucciones de importación, y una explicación de cada KPI en el README (qué mide, de qué vista sale, cómo se interpreta).

**Hecho cuando:** al enviar nuevos eventos, los gráficos cambian tras refrescar.

---

### Fase 9 — Orquestación con Airflow (la parte "batch" bien hecha)

DAG `happiness_batch_training` (diario o manual) que ejecuta la Parte A con la misma lógica que los notebooks:

```
extract_raw ─► harmonize ─► validate_with_GE ─► build_features_split ─► train ─► evaluate ─► save_model_and_stream_files
                                   │ (si falla, el DAG se detiene)
```

```python
# airflow/dags/happiness_batch_training.py (esqueleto)
from datetime import datetime
from airflow.decorators import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["etl", "ml"])
def happiness_batch_training():
    @task
    def harmonize(): ...          # llama src.harmonize.build_unified()
    @task
    def validate(path): ...       # corre la suite de GE; lanza excepción si falla
    @task
    def train(path): ...          # entrena, guarda model.pkl + metadata
    @task
    def export_stream(): ...      # genera data/streaming/*.csv
    train(validate(harmonize())) >> export_stream()

happiness_batch_training()
```

Opcional: un segundo DAG `bootstrap_infra` que cree las tablas y verifique el topic. **No metas el consumer en un DAG.**

---

### Fase 10 — Documentación y presentación (0.5 + 30 % de la nota)

**README final (obligatorio)** — usa este índice:

1. Descripción del proyecto y objetivo
2. Arquitectura (diagrama + decisiones)
3. Estructura del repo
4. Decisiones de limpieza (tabla Problema → Decisión → Justificación)
5. Feature engineering (features, exclusiones, fuga, split, métricas)
6. Pipeline Kafka y **contrato del evento** (tabla + ejemplo JSON + estados)
7. Esquema de base de datos (diagrama ER + grano de cada tabla)
8. Dashboard y explicación de cada KPI (+ screenshots)
9. **Instrucciones de ejecución** (comandos exactos, en orden)
10. Limitaciones y trabajo futuro

**Instrucciones de ejecución de referencia:**

```bash
git clone <repo> && cd project
nix develop                                   # 1. entorno
podman-compose up -d kafka kafka-init postgres  # 2. infraestructura
# 3. Batch (Airflow o notebooks/CLI): genera models/model.pkl y data/streaming/*.csv
python -m src.harmonize && python -m src.train   # (o dispara el DAG en Airflow)
python kafka/consumer.py                      # 4. terminal A
python kafka/producer.py --delay 0.5          # 5. terminal B
# 6. Superset: http://localhost:8088   Grafana: http://localhost:3000
```

**Presentación (30 %)** — estructura de 10-12 minutos:
1. Problema y datos (1 min) · 2. Hallazgos del EDA y decisiones de limpieza (2) · 3. Arquitectura (2) · 4. **Demo en vivo**: producer → consumer → BD → dashboard, inyectando eventos inválidos (4) · 5. KPIs y conclusiones (1) · 6. Limitaciones (1).
Graba un video corto de respaldo de la demo por si falla la red/podman en vivo.

---

## 5. Checklist final de entrega

- [ ] `flake.nix` + `flake.lock` + `requirements.txt`
- [ ] `eda.ipynb` ejecutado con observaciones de calidad y propuesta de esquema unificado
- [ ] `model_training.ipynb` con métricas MAE/RMSE/R²
- [ ] `models/model.pkl` (Pipeline completo) + `model_metadata.json`
- [ ] `docker-compose.yml` que levanta Kafka (topic 3p/RF1), Postgres y servicios opcionales
- [ ] `kafka/producer.py` (key = country, retardo configurable, JSON del contrato)
- [ ] `kafka/consumer.py` (grupo `happiness-ml-team`, raw primero, validación, predicción, commit manual)
- [ ] `sql/create_tables.sql` + `sql/kpis.sql`
- [ ] Estados `VALID / INVALID_SCHEMA / INVALID_VALUES / PREDICTION_ERROR` demostrados
- [ ] Dashboard conectado a PostgreSQL con los 4 KPIs + screenshots en `dashboards/`
- [ ] Tests del contrato
- [ ] README completo con instrucciones de ejecución verificadas desde cero

---

## 6. Errores frecuentes (y cómo evitarlos)

| Error | Consecuencia | Prevención |
|---|---|---|
| No unificar nombres de países | Dimensión duplicada, KPIs por país erróneos | `COUNTRY_ALIASES` + verificación `nunique` |
| Usar `Dystopia Residual` o `rank` como feature | R² ≈ 1, fuga de información | Lista explícita de exclusiones |
| `train_test_split` aleatorio con países repetidos | Estimación de error poco fiable | `GroupShuffleSplit` por país |
| Streamear datos de entrenamiento sin avisar | KPI de error optimista | Stream del conjunto de test, o declararlo |
| Escalar fuera del `Pipeline` | Sesgo train/serving | Preprocesamiento dentro del `.pkl` |
| Orden de columnas distinto en consumer | Predicciones silenciosamente incorrectas | `FEATURES` único y `df[FEATURES]` |
| Validar antes de guardar la raw | Se pierden eventos inválidos | Insert raw primero, siempre |
| `enable.auto.commit=True` | Pérdida de eventos al caer el consumer | Commit manual tras persistir |
| `advertised.listeners` mal configurado | El host no conecta a Kafka en Podman | Listeners INTERNAL/EXTERNAL separados |
| Superset sin `psycopg2` | No conecta a Postgres | Imagen derivada con el driver |
| Dashboard leyendo CSV | Incumple el requisito | Solo vistas SQL sobre Postgres |
| No fijar versiones | "En mi máquina funciona" | `flake.lock` + `requirements.txt` con versiones |

---

## 7. Orden sugerido y estimación de esfuerzo

| Orden | Fase | Esfuerzo relativo |
|---|---|---|
| 1 | 0 Entorno · 1 Infra | ▓▓░░░ |
| 2 | 2 EDA y limpieza | ▓▓▓▓░ |
| 3 | 3 Features y modelo | ▓▓░░░ |
| 4 | 4 BD · 5 Contrato | ▓▓░░░ |
| 5 | 6 Producer · 7 Consumer | ▓▓▓░░ |
| 6 | 8 KPIs/dashboard | ▓▓░░░ |
| 7 | 9 Airflow | ▓▓░░░ |
| 8 | 10 README y presentación | ▓▓▓░░ |

**Estrategia:** primero un *camino delgado de punta a punta* (1 evento: producer → consumer → raw → fact → una gráfica) y luego engordas cada fase. Evita terminar perfecto el EDA sin haber probado Kafka.

---

## 8. Puntos a confirmar con el docente

1. ¿`date_id` debe referirse a `event_time` (fecha de emisión) o al año histórico de la observación? Esta guía usa `event_time` para `dim_date` y conserva `observation_year` en la tabla de hechos.
2. ¿Se permite streamear solo el conjunto de test, o debe ser el dataset completo?
3. ¿Se acepta Superset/Grafana en lugar de Power BI/Looker/Tableau (que son los sugeridos)? Son herramientas "sugeridas", pero conviene confirmarlo.
4. ¿Es obligatorio Docker Compose o se admite Podman Compose? (Es compatible con el mismo archivo.)