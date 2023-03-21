FROM apache/airflow:2.4.3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY . /opt/airflow/src
RUN pip install -r /opt/airflow/src/requirements.txt