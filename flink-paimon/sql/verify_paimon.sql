-- 批量读回 Paimon 三表合并当前态行数(验证多表 upsert/delete == 源表)。
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG paimon WITH (
  'type'='paimon','warehouse'='s3://warehouse/paimon',
  's3.endpoint'='http://minio:9000','s3.access-key'='minioadmin','s3.secret-key'='minioadmin123',
  's3.path.style.access'='true');

SELECT 'article' AS tbl, COUNT(*) AS cnt FROM paimon.lake.article
UNION ALL SELECT 'product', COUNT(*) FROM paimon.lake.product
UNION ALL SELECT 'comment', COUNT(*) FROM paimon.lake.`comment`;
