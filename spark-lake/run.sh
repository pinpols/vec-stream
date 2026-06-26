#!/usr/bin/env bash
# 统一 Spark lakehouse 入口:分派 Hudi / Iceberg 写入与查询。
#   run.sh hudi <table>            批量:cdc.public.<table> → Hudi
#   run.sh iceberg <table>         批量:cdc.public.<table> → Iceberg(MERGE INTO)
#   run.sh hudi-stream <table>     连续流:Structured Streaming → Hudi
#   run.sh iceberg-stream <table>  连续流:Structured Streaming → Iceberg
#   run.sh query-hudi <table> [id] | <table> <tenant_id> <id>
#   run.sh query-iceberg <table> [id] | <table> <tenant_id> <id>
set -euo pipefail

MODE="${1:-}"; shift || true

S3_ENDPOINT="${HUDI_S3_ENDPOINT:-http://minio:9000}"
# 凭据只走环境变量(批4 安全):不放进 spark-submit 命令行 / Spark UI Environment 页,
# 否则 secret 会泄露在 `ps` 和 4040/environment。EnvironmentVariableCredentialsProvider
# 从 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 读;这里兜底导出(兼容只设了 MINIO_ROOT_* 的本地跑)。
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${MINIO_ROOT_USER:-minioadmin}}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD:-minioadmin123}}"

S3_CONF=(
  --conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT"
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
)

# 可观测(批3):Spark 原生 PrometheusServlet,driver UI(4040)暴露 /metrics/prometheus;
# spark.ui.prometheus.enabled 加 executor 指标;streaming.metricsEnabled 加每查询的
# 输入/处理速率、批时长、水位线等流指标。Prometheus 抓 4040,无需额外 jar。
OBS_CONF=(
  --conf spark.ui.prometheus.enabled=true
  --conf spark.sql.streaming.metricsEnabled=true
  --conf spark.metrics.namespace=spark_lake
  --conf "spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet"
  --conf "spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus"
  --conf "spark.metrics.conf.applications.sink.prometheusServlet.path=/metrics/applications/prometheus"
)

# 可靠性(批4):优雅停机,容器收 SIGTERM 时让当前微批落完 checkpoint 再退,
# 避免半个批 + 重启从 checkpoint 重做(配合 restart:unless-stopped + S3 checkpoint = 崩溃自愈)。
REL_CONF=(
  --conf spark.streaming.stopGracefullyOnShutdown=true
)
HUDI_CONF=(
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
)
ICE_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.ice.type=rest
  --conf spark.sql.catalog.ice.uri="${ICEBERG_REST_URI:-http://iceberg-rest:8181}"
  --conf spark.sql.catalog.ice.warehouse=s3://warehouse
  --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  --conf spark.sql.catalog.ice.s3.endpoint="$S3_ENDPOINT"
  --conf spark.sql.catalog.ice.s3.path-style-access=true
  # 不传 s3.access-key-id/secret(批4 安全):S3FileIO 走 DefaultAWSCredentialsProviderChain,
  # 自动读 AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 环境变量,secret 不落命令行 / Spark UI。
  --conf spark.sql.catalog.ice.client.region="${AWS_REGION:-us-east-1}"
)

submit() { exec /opt/spark/bin/spark-submit --master "${SPARK_MASTER:-local[2]}" "$@"; }

case "$MODE" in
  hudi)            submit "${HUDI_CONF[@]}" "${S3_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/cdc_to_hudi.py "$@" ;;
  iceberg)         submit "${ICE_CONF[@]}"  "${S3_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/cdc_to_iceberg.py "$@" ;;
  hudi-stream)     export STREAM_MODE=true; submit "${HUDI_CONF[@]}" "${S3_CONF[@]}" "${OBS_CONF[@]}" "${REL_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/cdc_to_hudi.py "$@" ;;
  iceberg-stream)  export STREAM_MODE=true; submit "${ICE_CONF[@]}"  "${S3_CONF[@]}" "${OBS_CONF[@]}" "${REL_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/cdc_to_iceberg.py "$@" ;;
  query-hudi)      submit "${HUDI_CONF[@]}" "${S3_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/query.py hudi "$@" ;;
  query-iceberg)   submit "${ICE_CONF[@]}"  "${S3_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/query.py iceberg "$@" ;;
  maintain-iceberg) submit "${ICE_CONF[@]}" "${S3_CONF[@]}" --jars "$LAKE_JARS" /opt/spark-lake/maintain.py "$@" ;;
  *) echo "usage: run.sh <hudi|iceberg|*-stream|query-*|maintain-iceberg> <table> [id]" >&2; exit 2 ;;
esac
