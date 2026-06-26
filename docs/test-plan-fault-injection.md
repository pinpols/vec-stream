# 湖腿故障注入测试方案 + 实测结果

针对"happy-path 全绿也照样在生产炸"的故障类场景,补一组**真故障注入**测试:真 `docker kill`
流容器、真重启 connector,验证流式入湖的可靠性(崩溃恢复 / 幂等 / slot 续传)。

脚本骨架见本文末;一次性可重放。环境:vec infra(postgres/kafka/connect/minio)+ `spark-lake-hudi-stream` 连续流。

## 场景与判定

| 编号 | 场景 | 注入手法 | 判定 | 结果(2026-06-25 实测) |
|---|---|---|---|---|
| T1 | **崩溃恢复不丢** | insert B 后立刻 `docker kill` 流容器,再重启 | B 经 checkpoint 重放仍落 Hudi | ✅ B 恢复,不丢 |
| T2 | **崩溃窗口内 update 传播** | `UPDATE` 后立刻 kill,再重启 | 变更不随崩溃丢失 | ✅ A=archived 仍传播 |
| T3 | **幂等(无重复)** | 多次崩溃重启后比对行数 | Hudi 行数 == 源库行数(无重/无漏) | ✅ PG=13 == Hudi=13,精确镜像 |
| T4 | **connector 重启续传** | `docker restart` connect 后再 insert | slot 续传,新变更仍落 | ✅ C 落 Hudi |

## 关键结论

- **崩溃恢复**:Structured Streaming checkpoint(`s3a://warehouse/_chk/hudi-all`)+ `restart:unless-stopped`
  让流容器被 kill 后从断点续跑,崩溃窗口内的 insert/update 都不丢(T1/T2)。
- **有效 exactly-once**:Hudi 按 `recordkey=tenant_id,id` upsert,重放同一 offset 区间只是再次 upsert 同一业务记录,
  不产生重复行——经 3 次流重启后 **Hudi 行数与源库精确相等(13==13)**,既不重也不漏(T3)。
  > 注:T3 脚本里"期望==2"是测试预期写错了——T0 清了 checkpoint 会从 earliest 重放**整个 topic 历史**,
  > 故表里是所有历史 id 而非本轮 2 行;改用"Hudi==PG"才是正确的不重不漏判据。
- **connector 容错**:Debezium replication slot 持久化消费位点,connect 容器重启后从 slot 续传,不丢变更(T4)。

## 仍未覆盖(诚实留白)

本组只打了"流/connector 崩溃重启"这一类。**未测**:规模/吞吐压测、真实 DDL schema 演进、
毒消息/DLQ 路径、MinIO/PG 不可达、PG failover、长稳/内存、多 writer 并发(见下)。

## 并发写说明(单 writer by design)

Hudi 默认无锁,多 writer 写同一表会损坏(实测)。本架构**流是唯一写者**,默认单 writer 安全;
**批量回填(`run.sh hudi <table>`)须在流停止时跑**。真要多 writer 才需要分布式锁——但
**S3/MinIO 不支持零依赖文件锁**(`FileSystemBasedLockProvider` 需原子 create,s3a 实测报
`Unsupported scheme :s3a`),只能用 ZooKeeper:设 `HUDI_LOCK_ZK_URL=<host:port>` 起用(需另起 ZK,**本地未验**)。

## 复跑

```bash
# 起 infra + 连续流后,执行故障注入脚本(scripts/ 下可固化;当前为一次性 /tmp 脚本)
# 核心步骤:insert → 等落 → docker kill <stream> → docker compose up -d <stream> → 验数据不丢
# 不重判据:docker compose run --rm spark-lake query-hudi article (COUNT) == SELECT count(*) FROM article
```
