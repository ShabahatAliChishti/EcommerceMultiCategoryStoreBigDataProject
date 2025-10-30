FROM apache/airflow:2.4.2

USER root
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow
RUN pip install --no-cache-dir pyspark
