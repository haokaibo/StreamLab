# syntax=docker/dockerfile:1
# Flink session-cluster image with the connectors the SQL jobs need.
# Versions are build args so they can be bumped in one place (.env); the Kafka/JDBC
# connector versions must match the Flink minor version.
#
# Flink 2.x note: the JDBC connector was split into separate artifacts. Since 4.x there is
# no shaded `flink-connector-jdbc` bundle for the SQL client -- you need `jdbc-core` plus the
# per-dialect jar (`jdbc-postgres`) instead.
ARG FLINK_VERSION=2.2.1

FROM curlimages/curl:8.11.1 AS fetch
ARG FLINK_VERSION
ARG FLINK_KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG FLINK_JDBC_CONNECTOR_VERSION=4.1.0-2.2
ARG POSTGRES_JDBC_VERSION=42.7.7
# The 4.x JDBC connector links against OpenLineage but does not bundle it; without these
# jars job B dies with ClassNotFoundException: JdbcExtractor. Version matches the
# openlineage.version property of flink-connector-jdbc-parent (K-19).
ARG OPENLINEAGE_VERSION=1.32.0
USER root
WORKDIR /jars
RUN set -eux; \
    M=https://repo1.maven.org/maven2; \
    curl -fsSLO "$M/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/flink/flink-connector-jdbc-core/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${FLINK_JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "$M/org/apache/flink/flink-connector-jdbc-postgres/${FLINK_JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${FLINK_JDBC_CONNECTOR_VERSION}.jar"; \
    curl -fsSLO "$M/io/openlineage/openlineage-java/${OPENLINEAGE_VERSION}/openlineage-java-${OPENLINEAGE_VERSION}.jar"; \
    curl -fsSLO "$M/io/openlineage/openlineage-sql-java/${OPENLINEAGE_VERSION}/openlineage-sql-java-${OPENLINEAGE_VERSION}.jar"; \
    curl -fsSLO "$M/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM flink:${FLINK_VERSION}-scala_2.12-java17
ARG FLINK_VERSION
COPY --from=fetch /jars/*.jar /opt/flink/lib/
# S3-compatible checkpoints and file sink go through the bundled Hadoop S3 plugin.
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-${FLINK_VERSION}.jar
