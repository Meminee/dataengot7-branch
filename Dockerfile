FROM apache/airflow:2.8.1-python3.8

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

ADD requirements.txt .
RUN pip install -r requirements.txt
