# Superset + PostgreSQL driver.
# The official image only ships with SQLite/MySQL support, so we derive a
# thin image with psycopg2 to connect to the happiness database.
FROM docker.io/apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10"
USER superset
