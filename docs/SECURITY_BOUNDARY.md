# 安全边界

> 目标:本仓库默认适合本机开发和 pre-production reference;真实生产必须把管理面、凭据、网络入口交给目标平台治理。

## 已加固

- **RAG 租户边界**:`X-API-Key -> tenant_id`,请求体里的 `tenant_id` 被忽略;pgvector 后端用 RLS + `app.tenant` 强制隔离。
- **最小权限账号**:`vs_debezium` 只复制读,`vs_worker` 可信写入,`vs_rag` 只读且不 BYPASSRLS。
- **本地端口边界**:所有 compose host port 默认绑定 `127.0.0.1`,避免开发机把 Connect/Qdrant/MinIO/Grafana 等管理面暴露到局域网。
- **结构门禁**:`scripts/validate.sh` 会拒绝新增未绑定 loopback 的短端口映射,例如 `"8083:8083"`。
- **生产防呆**:`APP_ENV=production` 时,RAG/worker 会拒绝默认弱配置:
  - `dev-key-default`
  - `change-me-*`
  - `vs_rag:vs_rag@...` / `vs_worker:vs_worker@...`
  - `VECTOR_BACKEND=qdrant` 但未设置 `QDRANT_API_KEY`
  - `EMBED_PROVIDER=openai` 但未设置 `OPENAI_API_KEY`
- **enrich_sql 注入面收敛**:跨表反查 SQL 来自 `TABLES_JSON`(运维侧配置),启动期校验**必须是单条 SELECT**(禁分号/DDL/DML)且**必带 `%(tenant)s` 条件**,防止误配/被篡改的配置在 `vs_worker` 权限下注入任意 SQL 或泄漏跨租户数据;`fields` 非空校验避免运行时崩。
- **凭据脱敏**:DLQ header / 归档 / 日志里的异常字符串经 `_redact` 抹掉 DSN 密码(psycopg 报错常带完整连接串),避免密码扩散到 Kafka DLQ + PG archive + stdout 三处。
- **/healthz 不泄漏拓扑**:健康检查只回状态 + 后端类型,不再回 LLM 网关 URL;且真探测向量后端,依赖宕机回 503(K8s 探针可感知)。
- **跨租户源库约束**:`comment(tenant_id, article_id)` 外键指向 `article(tenant_id, id)`,防止跨租户脏引用进入跨表文档。
- **Qdrant API key 透传**:worker/rag 都支持 `QDRANT_API_KEY`;本地可不设,生产必须设。
- **Embedding 出境门闸**:`EMBED_PROVIDER=openai` 会把源文档发给 OpenAI-compatible embeddings 端点,默认不可用;必须显式设置 `EMBED_EGRESS_ALLOWED=true`。
- **LLM 出境门闸**:`/ask` 会把召回内容发给 OpenAI-compatible 端点,默认不可用;必须显式设置 `LLM_EGRESS_ALLOWED=true` 且配置 `OPENAI_API_KEY`。`/search` 不依赖 LLM,可保持纯内网检索。

## 仍然不是生产边界的组件

这些组件在本仓库里只作为本地样板:

- Kafka/Connect:单节点、PLAINTEXT、Connect REST 无内建鉴权。**含义**:任何能向 Kafka 生产消息的进程可伪造 CDC 事件——例如构造 `op=d` 且 `before.tenant_id=<受害租户>` 删掉他人向量(worker 删除只能信 before 镜像的租户,无从二次核验已删行)。这条跨租户风险由 Kafka SASL/鉴权关闭,属本边界内,不是代码可单独修。
- MinIO:本地对象存储样板,默认凭据只允许开发。
- Iceberg REST fixture:测试 fixture,无鉴权。
- Prometheus/Grafana/Jaeger/Spark/Flink UI:默认只绑 127.0.0.1,生产需统一放到受控管理网或反向代理后。

## 生产准入

生产部署必须满足:

1. Kafka SASL/TLS 或私网访问控制。
2. Connect REST 不暴露公网,由认证管理面代理。
3. Qdrant 启用 API key/TLS,或使用 per-tenant collection/托管向量库。
4. S3/MinIO 使用强凭据和最小权限 bucket policy。
5. Iceberg catalog 换成带认证的 Nessie/Polaris/自建 REST catalog。
6. Grafana/Prometheus/Jaeger/Spark UI 进入受控管理网。
7. Secret manager 替代 `.env` 文件。
8. 明确 embedding / LLM 数据出境策略:仅允许受控网关或合规 provider,并为外部调用保留审计日志。

开发机要对局域网开放端口时,必须显式改 compose 端口绑定;这属于越过本仓库默认安全边界。
