# Topic 规划 · 统一 CDC 多 sink

## 原则

1. **单一事实源**:一个 Debezium connector、一个复制槽(slot),产一份 CDC 流。
2. **多 sink 扇出**:所有下游各自独立消费同一批 topic,独立消费组 / offset,互不阻塞;任一 sink 故障不影响其他。
3. **单格式 JSON**:`schemas.enable=false`,Debezium envelope(`op/before/after/source/ts_ms`)在消息顶层。
   - 选 JSON 不选 Avro:不动已实测绿的向量 worker(核心),只让新的湖腿迁就;湖侧表 schema 在各 sink 显式声明,不依赖 Schema Registry。真上规模再换 Avro 是受控的局部改动。

## 数据流

```
Postgres ─ Debezium(vec-stream-pg, JSON)─ cdc.public.<table> ─┬→ 向量 Sync Worker → 向量库 → RAG
  slot vec_stream_slot                                         ├→ iceberg-sync     → Iceberg 表
  publication vec_stream_pub                                   └→ hudi-spark       → Hudi 表
```

## Topic 清单

| topic | 产方 | 格式 | key | 消费方 |
|---|---|---|---|---|
| `cdc.public.article` | Debezium `vec-stream-pg` | JSON envelope | 主键 id | 向量 worker / iceberg-sync / hudi-spark |
| `cdc.public.product` | 同上 | JSON | id | 同上 |
| `cdc.public.comment` | 同上 | JSON | id | 同上 |
| `cdc.dlq` | 向量 worker | JSON | — | 人工 / 回投 |
| `_connect_*`(configs/offsets/status)| Kafka Connect | 内部 | — | Connect |

## 消费组(独立 offset)

| sink | 消费方式 |
|---|---|
| 向量 worker | 消费组 `vec-stream-worker`(常驻) |
| spark-lake(Hudi/Iceberg) | **默认连续流**(Structured Streaming,checkpoint 在 `s3a://warehouse/_chk/<engine>-<arg>` 管 offset,~10-20s);**批量回填**模式按需读全 topic(幂等重放,不依赖消费组/ checkpoint) |

## 约定

- **命名**:`cdc.<schema>.<table>`(Debezium `topic.prefix=cdc`)。
- **分区 key=主键**:同一行的变更进同一分区、有序,避免 update 乱序覆盖。
- **删除**:`tombstones.on.delete=true`;删除靠 op=d 事件(before 带主键),墓碑(null value)各 sink 跳过。
- **死信**:`cdc.dlq`(仅向量 worker 的数据性失败兜底)。

## 已退役(合并入 cdc.*)

- `lake.public.*`(Avro)+ connector `vec-stream-pg-lake` + slot `vec_stream_lake_slot`:统一前湖腿独用,
  现合并到 `cdc.public.*`,**省掉第二个复制槽 / 第二次 WAL 解码**。
- Avro + Schema Registry 在统一后湖腿不再需要(JSON 路径)。
- 探索期产物(HoodieStreamer / Flink→Iceberg/Hudi / pyiceberg iceberg-sync / Kafka Connect Iceberg Sink)均已退役,湖腿统一到 `spark-lake`(Spark 写 Hudi + Iceberg),见 git 历史。
  - Connect Iceberg Sink 实测结论:其控制 topic 两阶段提交协议与 broker group 协调强耦合,worker 控制面消费者持续 `UNKNOWN_MEMBER_ID` rebalance、每轮 `committed to 0 table(s)`,与 tasks.max / catalog 后端 / 环境洁净度均无关;Iceberg 改走 Spark 原生 commit 稳定。
