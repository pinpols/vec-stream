# Flink → Paimon 样板镜像。Paimon 是 Flink 原生湖表;paimon-s3 自包含(自带 hadoop-aws+aws-sdk),
# 不需要 flink-shaded-hadoop / 手拼依赖(比 Flink→Iceberg/Hudi 干净得多)。
FROM flink:1.19.3-scala_2.12-java17

ARG PAIMON_VERSION=1.1.1
ARG KAFKA_CONNECTOR=3.3.0-1.19
ARG M2=https://repo1.maven.org/maven2

USER root
RUN set -eux; cd /opt/flink/lib; \
    curl -fL --retry 6 --retry-delay 3 --retry-all-errors "${M2}/org/apache/paimon/paimon-flink-1.19/${PAIMON_VERSION}/paimon-flink-1.19-${PAIMON_VERSION}.jar" -o paimon-flink.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-all-errors "${M2}/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar" -o paimon-s3.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-all-errors "${M2}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar" -o flink-sql-connector-kafka.jar; \
    # Paimon catalog 框架要 un-shaded org.apache.hadoop.conf.Configuration(paimon-s3 只带 S3,不带它)
    curl -fL --retry 6 --retry-delay 3 --retry-all-errors "${M2}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" -o flink-shaded-hadoop.jar
# 可观测(批3):Flink 自带 PrometheusReporter(随发行版在 opt/),挪到 lib 即生效,无需下载。
RUN cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/lib/
USER flink
