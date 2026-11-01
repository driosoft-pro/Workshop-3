# Airflow image with the libraries the batch DAG needs.
# Airflow itself stays out of requirements.txt (dependency conflicts with the
# ML stack): it lives here, isolated, exactly like the Nix/venv separation.
FROM docker.io/apache/airflow:2.10.5

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
