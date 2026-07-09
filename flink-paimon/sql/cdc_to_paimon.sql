-- Flink → Paimon(多表)。读 cdc.public.{article,product,comment}(debezium-json)→ Paimon 主键表。
-- STATEMENT SET 把三表 INSERT 合成一个作业。
--
-- 主键:与 Hudi/Iceberg 腿对齐用复合键 (tenant_id, id)——多租户下不同租户的同 id
-- 是不同业务行,单列 (id) 会互相覆盖。
--
-- ⚠️ 已知边界(TOAST,本腿未修,SQL 层做不了):pgoutput 对 UPDATE 中**未变更的
-- TOAST 大列**(如超长 body)在 after 镜像里填占位符 `__debezium_unavailable_value`
-- (REPLICA IDENTITY FULL 只保证 before 完整)。Flink debezium-json format 把
-- before/after 拆成 -U/+U 两条独立 changelog 行,无状态 SQL 拿不到 +U 对应的
-- before 值做回退,占位符会被当成新值写进 Paimon 大文本列。
-- Hudi/Iceberg 腿已在 Spark 里做占位符→before 回退(见 spark-lake/lakehouse_logic.py);
-- 本参考腿如需精确大列值,请以 Spark 腿为准,或改用 Paimon partial-update/自定义
-- DataStream 作业处理。详见 docs/DESIGN.md §3.3(a) 边界清单。

SET 'execution.checkpointing.interval' = '10s';

-- 源表(debezium-json changelog)
CREATE TABLE src_article (
  id BIGINT, tenant_id STRING, title STRING, body STRING, status STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.article','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-article','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

CREATE TABLE src_product (
  id BIGINT, tenant_id STRING, name STRING, description STRING, status STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.product','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-product','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

CREATE TABLE src_comment (
  id BIGINT, tenant_id STRING, article_id BIGINT, body STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.comment','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-comment','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

-- Paimon catalog(文件系统 catalog,warehouse 在 MinIO)
CREATE CATALOG paimon WITH (
  'type'='paimon','warehouse'='s3://warehouse/paimon',
  's3.endpoint'='http://minio:9000','s3.access-key'='__S3_ACCESS_KEY__','s3.secret-key'='__S3_SECRET_KEY__',
  's3.path.style.access'='true');

CREATE DATABASE IF NOT EXISTS paimon.lake;

CREATE TABLE IF NOT EXISTS paimon.lake.article (
  id BIGINT, tenant_id STRING, title STRING, body STRING, status STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');
CREATE TABLE IF NOT EXISTS paimon.lake.product (
  id BIGINT, tenant_id STRING, name STRING, description STRING, status STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');
CREATE TABLE IF NOT EXISTS paimon.lake.`comment` (
  id BIGINT, tenant_id STRING, article_id BIGINT, body STRING, updated_at STRING,
  PRIMARY KEY (tenant_id, id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');

-- 一个作业写三表
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO paimon.lake.article SELECT id, tenant_id, title, body, status, updated_at FROM src_article;
  INSERT INTO paimon.lake.product SELECT id, tenant_id, name, description, status, updated_at FROM src_product;
  INSERT INTO paimon.lake.`comment` SELECT id, tenant_id, article_id, body, updated_at FROM src_comment;
END;
