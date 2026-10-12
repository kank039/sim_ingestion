# Flink with the Kafka SQL connector and the Approach 3 job definition baked in.
ARG FLINK_IMAGE=flink:1.18.1-scala_2.12
FROM ${FLINK_IMAGE}
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ADD --chmod=644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/
COPY sql/ /opt/flink/sql/
