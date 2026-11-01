---
name: airflow-batch
description: DAG de Airflow para la Parte A (extract, harmonize, validar con Great Expectations, features, entrenar, exportar stream). Usar al crear airflow/dags o montar Airflow en contenedor.
---
# Principios
- Airflow orquesta SOLO el batch. El consumer es un proceso continuo: **no va en un DAG**.
- La lógica vive en `src/` (harmonize, train); las tareas solo la invocan, igual que los notebooks → una sola implementación.
- Instalar Airflow aparte (imagen `docker.io/apache/airflow` o venv con constraints oficiales), no en el shell de Nix (conflictos de dependencias). Instalar en esa imagen: pandas, scikit-learn, great-expectations, pyarrow. Montar el repo como volumen y definir `PYTHONPATH`.
- Pasar rutas entre tareas (no DataFrames por XCom).

# DAG `happiness_batch_training` (`schedule=None`, `catchup=False`)
`harmonize → validate_ge (falla ⇒ DAG se detiene) → train(+evaluate, escribe model.pkl y model_metadata.json) → export_stream (stream_events.csv, invalid_events.csv)`.
API TaskFlow: `from airflow.decorators import dag, task`. Opcional: DAG `bootstrap_infra` (crear tablas, verificar topic).
Verificar: `airflow dags test happiness_batch_training` (o disparar en la UI) y comprobar que `models/model.pkl` se regenera.
