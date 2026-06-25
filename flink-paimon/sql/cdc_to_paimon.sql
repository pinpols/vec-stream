-- Flink → Paimon(多表)。读 cdc.public.{article,product,comment}(debezium-json)→ Paimon 主键表。
-- STATEMENT SET 把三表 INSERT 合成一个作业。

SET 'execution.checkpointing.interval' = '10s';

-- 源表(debezium-json changelog)
CREATE TABLE src_article (
  id BIGINT, tenant_id STRING, title STRING, body STRING, status STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.article','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-article','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

CREATE TABLE src_product (
  id BIGINT, tenant_id STRING, name STRING, description STRING, status STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.product','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-product','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

CREATE TABLE src_comment (
  id BIGINT, tenant_id STRING, article_id BIGINT, body STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='cdc.public.comment','properties.bootstrap.servers'='kafka:29092',
  'properties.group.id'='flink-paimon-comment','scan.startup.mode'='earliest-offset',
  'format'='debezium-json','debezium-json.schema-include'='false');

-- Paimon catalog(文件系统 catalog,warehouse 在 MinIO)
CREATE CATALOG paimon WITH (
  'type'='paimon','warehouse'='s3://warehouse/paimon',
  's3.endpoint'='http://minio:9000','s3.access-key'='minioadmin','s3.secret-key'='minioadmin123',
  's3.path.style.access'='true');

CREATE DATABASE IF NOT EXISTS paimon.lake;

CREATE TABLE IF NOT EXISTS paimon.lake.article (
  id BIGINT, tenant_id STRING, title STRING, body STRING, status STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');
CREATE TABLE IF NOT EXISTS paimon.lake.product (
  id BIGINT, tenant_id STRING, name STRING, description STRING, status STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');
CREATE TABLE IF NOT EXISTS paimon.lake.`comment` (
  id BIGINT, tenant_id STRING, article_id BIGINT, body STRING, updated_at STRING,
  PRIMARY KEY (id) NOT ENFORCED) WITH ('snapshot.num-retained.min'='5','snapshot.num-retained.max'='20','snapshot.time-retained'='1 h','full-compaction.delta-commits'='5');

-- 一个作业写三表
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO paimon.lake.article SELECT id, tenant_id, title, body, status, updated_at FROM src_article;
  INSERT INTO paimon.lake.product SELECT id, tenant_id, name, description, status, updated_at FROM src_product;
  INSERT INTO paimon.lake.`comment` SELECT id, tenant_id, article_id, body, updated_at FROM src_comment;
END;
