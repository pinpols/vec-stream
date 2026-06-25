# 统一 Spark lakehouse 引擎:一个 Spark 镜像同时写 Hudi 和 Iceberg(各自主流引擎)。
# 脚本走挂载(compose volume),改脚本免重建镜像。
FROM spark:3.5.1-scala2.12-java17-ubuntu

USER root

ARG HUDI_VERSION=0.15.0
ARG ICEBERG_VERSION=1.9.2
ARG SPARK_VERSION=3.5.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG M2=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates python3; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /opt/lake-jars; cd /opt/lake-jars; \
    # Hudi(Spark 3.5 bundle)
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/hudi/hudi-spark3.5-bundle_2.12/${HUDI_VERSION}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar" -o hudi-spark-bundle.jar; \
    # Iceberg(Spark 3.5 runtime + AWS bundle for S3FileIO)
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" -o iceberg-spark-runtime.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" -o iceberg-aws-bundle.jar; \
    # S3A(Hudi 经 hadoop-aws 写 MinIO)
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o hadoop-aws.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o aws-sdk-bundle.jar; \
    # Kafka source
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" -o spark-sql-kafka.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" -o spark-token-provider-kafka.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" -o kafka-clients.jar; \
    curl -fL --retry 6 --retry-delay 3 --retry-connrefused --retry-all-errors "${M2}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" -o commons-pool2.jar

ENV LAKE_JARS=/opt/lake-jars/hudi-spark-bundle.jar,/opt/lake-jars/iceberg-spark-runtime.jar,/opt/lake-jars/iceberg-aws-bundle.jar,/opt/lake-jars/hadoop-aws.jar,/opt/lake-jars/aws-sdk-bundle.jar,/opt/lake-jars/spark-sql-kafka.jar,/opt/lake-jars/spark-token-provider-kafka.jar,/opt/lake-jars/kafka-clients.jar,/opt/lake-jars/commons-pool2.jar

# 脚本挂载在 /opt/spark-lake(compose volume),改脚本免重建
ENTRYPOINT ["bash", "/opt/spark-lake/run.sh"]
