# Spark 3.5 + Iceberg (runtime + bundle AWS pour S3FileIO) + hadoop-aws (lecture s3a://)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2
# Level 3 : source/sink Kafka de Structured Streaming (versions alignées sur Spark 3.5.3)
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

USER root

# Les JAR sont intégrés à l'image (pas de --packages au runtime : démarrage rapide,
# pas de dépendance réseau lors de l'exécution des jobs).
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# Checkpoints des jobs Structured Streaming (volume nommé : hérite de ce propriétaire)
RUN mkdir -p /checkpoints && chown -R spark:spark /checkpoints

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --default-timeout=300 --retries 10 -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs /opt/waba/jobs
COPY tests /opt/waba/tests

ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}" \
    PYTHONPATH="/opt/waba/jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip" \
    SPARK_NO_DAEMONIZE=true

WORKDIR /opt/waba
USER spark
