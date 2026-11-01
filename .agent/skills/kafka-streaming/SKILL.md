---
name: kafka-streaming
description: Infra Kafka KRaft con podman-compose, contrato del evento (src/contract.py), producer y consumer confluent-kafka con raw-first, commit manual e idempotencia. Usar en docker-compose.yml, kafka/*.py y tests del contrato.
---
# Infra (podman-compose, rootless)
- Imágenes con registro completo (`docker.io/apache/kafka`, `docker.io/library/postgres:16`). Volúmenes bind con `:Z` si hay SELinux. Verificar que la versión de imagen exista y fijarla.
- Kafka KRaft, **dos listeners**: `INTERNAL://kafka:9092` (contenedores) y `EXTERNAL://localhost:29092` (host). Desde el host usar `localhost:29092`. Error típico: advertised.listeners único.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` + servicio `kafka-init` que ejecuta `kafka-topics.sh --create --if-not-exists --topic happiness-events --partitions 3 --replication-factor 1`.
- Verificar: `podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic happiness-events`.
- Levantar por partes (kafka+postgres primero); Airflow/Superset/Grafana solo cuando se necesiten (RAM).

# Contrato (`src/contract.py`) — forma/tipo/presencia → `INVALID_SCHEMA`; valor fuera de dominio → `INVALID_VALUES`
`validate(payload)` lanza `ContractError(status, detail)`. Reglas: dict; `event_id`,`country` str no vacíos; `year` int (no bool) en 2015-2019; features y `actual_happiness_score` numéricos (no bool), `math.isfinite`, rangos gdp 0-2.5, family 0-2, health 0-1.5, freedom/generosity/corruption 0-1, score 0-10; `event_time` str con `datetime.fromisoformat`. `json.loads` acepta NaN ⇒ comprobar `isfinite`. Tests: un caso por regla (`tests/test_contract.py`). Ajustar rangos con el min/max del EDA.

# Producer (`kafka/producer.py`, confluent-kafka)
CLI `--file --bootstrap --delay --limit [--inject-invalid]`. `Producer({"enable.idempotence":True,"acks":"all"})`; `produce(topic,key=country.encode(),value=json.dumps(evt).encode(),on_delivery=cb)`; `poll(0)`; `sleep(delay)`; `flush()` al final. `event_id` único (`E0001`… + `run_id` si se repite). `event_time` = emisión (`datetime.now().isoformat(timespec="seconds")`), no la fecha histórica.

# Consumer (`kafka/consumer.py`)
Config: `group.id=happiness-ml-team`, `enable.auto.commit=False`, `auto.offset.reset=earliest`. Cargar `model.pkl` **una vez**. Por mensaje:
1. `INSERT raw_happiness_events` (topic/partition/offset/key/raw_payload, status `RECEIVED`) `ON CONFLICT (topic,partition,offset) DO NOTHING RETURNING raw_event_id`; si no devuelve id → ya procesado, saltar. Transacción propia.
2. `json.loads` (falla ⇒ `INVALID_SCHEMA`); rellenar `event_id` y `payload_json` en raw.
3. `validate()` ⇒ `INVALID_*` y continuar.
4. `DataFrame([payload])[FEATURES]` → `predict`; excepción ⇒ `PREDICTION_ERROR`.
5. Una transacción: upsert `dim_country`/`dim_date`, insert `fact_predictions` (error = predicho − real, abs_error, model_id), raw → `VALID`.
6. `consumer.commit(message=msg, asynchronous=False)` **solo después** de persistir.
- Evento inválido = resultado de negocio (guardar y seguir). BD caída = error de infraestructura (no commit, backoff/reinicio).
- Métricas Prometheus en puerto 8000: `events_consumed_total{status}`, `prediction_latency_seconds`, `prediction_abs_error`.
## Prueba obligatoria
Mezclar válidos + `invalid_events.csv`: todos en raw con estado correcto, 0 filas fact para inválidos, consumer vivo; matar y reiniciar consumer ⇒ sin duplicados ni pérdidas.
`SELECT processing_status,count(*) FROM raw_happiness_events GROUP BY 1;` y `count(VALID)=count(fact_predictions)`.
