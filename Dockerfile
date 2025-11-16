FROM apache/airflow:2.6.3-python3.10

user root

# Installing Java 17 (removing cached apt metadata to minimize image size)
RUN apt-get update && \
    apt-get install -y curl openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set Java, Spark, Airflow Home 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=4.0.0
ENV SPARK_HOME=/opt/spark
ENV AIRFLOW_HOME=/opt/airflow

# Installing spark - 4.0.0
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

# Add Spark to path
ENV PATH="${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"
ENV PYTHONPATH=.:$AIRFLOW_HOME:$PYTHONPATH

WORKDIR $AIRFLOW_HOME

# Copy DAGs and cfg
COPY airflow/dags/ $AIRFLOW_HOME/dags/

# Copy spark job (can be removed next build due to mounting)
COPY jobs/ $AIRFLOW_HOME/jobs/

# Copy utils scripts (can be removed next build due to mounting)
COPY utils/ $AIRFLOW_HOME/utils/

USER airflow
# Copy requirements and install them
COPY airflow/requirements.txt $AIRFLOW_HOME/
RUN pip install --no-cache-dir -r $AIRFLOW_HOME/requirements.txt


