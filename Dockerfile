FROM apache/airflow:2.4.2

USER root
# Install Java and ps command
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for container
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy the PostgreSQL JDBC driver 
COPY drivers/postgresql-42.7.8.jar /opt/airflow/drivers/postgresql-42.7.8.jar

USER airflow
# Install PySpark, Transformers, and Torch (these layers will use cache)
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir transformers==4.25.1
RUN pip install --no-cache-dir torch==1.13.1
