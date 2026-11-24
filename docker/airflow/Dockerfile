# The image for this project: Airflow, plus the pipeline in its own virtual environment.
#
# Airflow needs many Python packages, and so will dbt in milestone 4. Their versions do not
# always agree. So the pipeline does not share Airflow's Python. It gets its own environment,
# /opt/lotus-venv, and Airflow starts it as a command:
#     /opt/lotus-venv/bin/python -m lotus_pipeline load --day 2026-08-24
FROM apache/airflow:3.4.0

USER root
RUN python -m venv /opt/lotus-venv && chown -R airflow /opt/lotus-venv \
    && mkdir -p /var/lotus/landing && chown -R airflow:0 /var/lotus && chmod -R g+rwX /var/lotus
USER airflow

COPY requirements.txt /tmp/lotus-requirements.txt
RUN /opt/lotus-venv/bin/pip install --no-cache-dir -r /tmp/lotus-requirements.txt
