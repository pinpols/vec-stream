# vec_stream 监控告警(ENTERPRISE.md M2 领域四 · item 7)

Prometheus 告警规则 + SLO + 可选 Grafana 大盘。独立于主 `docker-compose.yml`,通过
`docker-compose.monitoring.yml` 覆盖文件单独启停,不影响 db/kafka/connect/qdrant/worker。

## 文件

| 文件 | 作用 |
| --- | --- |
| `prometheus.yml` | 抓取配置:抓宿主机 worker `host.docker.internal:9100/metrics`,加载告警规则 |
| `alerts.yml` | 4 类告警规则(同步延迟 / DLQ / slot / worker 掉线),每条含 SLO 注释 |
| `grafana-dashboard.json` | 同步延迟、事件速率、DLQ、slot lag 大盘 |
| `grafana/provisioning/` | Grafana 数据源 + dashboard 自动装载 |

## 起法

```bash
docker compose -f docker-compose.monitoring.yml up -d
```

- Prometheus:http://localhost:9090 （Alerts 页 `/alerts`,Targets 页 `/targets`）
- Grafana:http://localhost:3000 （admin / admin;Dashboards → "vec_stream — CDC 同步监控"）

worker 需在**宿主机**上以 `METRICS_PORT`(默认 9100)运行并暴露 `/metrics`。容器通过
`extra_hosts: host.docker.internal:host-gateway` 抓宿主机进程(Linux 上 host-gateway 解析,
macOS/Windows Docker Desktop 内置该主机名)。

停止:`docker compose -f docker-compose.monitoring.yml down`(加 `-v` 清数据卷)。

## 监控的真实指标(取自 `worker/src/vec_stream_worker/metrics.py`)

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `vec_stream_sync_delay_seconds` | Histogram | Debezium 事件时间 → worker 完成的端到端延迟 |
| `vec_stream_dlq_sent_total` | Counter | 投递到 DLQ 的消息数 |
| `vec_stream_dlq_backlog` | Gauge | DLQ 中未被 `{group}-dlq-replay` 消费的积压 |
| `vec_stream_slot_active` | Gauge | replication slot active(1/0),来自 `slot_monitor.py` |
| `vec_stream_slot_lag_bytes` | Gauge | replication slot WAL lag(字节),来自 `slot_monitor.py` |
| `vec_stream_events_total` | Counter | CDC 事件处理数(label:table/action) |
| `vec_stream_chunks_embedded_total` | Counter | 嵌入的 chunk 数(贵调用) |

## SLO 与告警

| 告警 | SLO | PromQL(摘要) | 阈值 | for | severity |
| --- | --- | --- | --- | --- | --- |
| `HighSyncDelay` | 同步延迟 p99 < 60s | `histogram_quantile(0.99, sum(rate(...sync_delay_seconds_bucket[5m])) by (le)) > 60` | 60s | 5m | warning |
| `DlqGrowing` | DLQ 投递速率 ≈ 0 | `sum(rate(...dlq_sent_total[5m])) > 0` | >0 持续 | 10m | warning |
| `DlqBacklogStuck` | 积压可被 replay 清空 | `max(...dlq_backlog) > 100` | 100 条 | 15m | warning |
| `SlotInactive` | slot 始终 active | `max(...slot_active) == 0` | active==0 | 2m | critical |
| `SlotLagHigh` | slot lag < 256MB | `max(...slot_lag_bytes) > 268435456` | 256MB | 5m | warning |
| `WorkerDown` | worker 可用 | `up{job="vec-stream-worker"} == 0` | 掉线 | 1m | critical |

### 阈值依据

- **同步延迟 p99 < 60s**:`SYNC_DELAY` histogram bucket 含 `60` 边界,p99 在 60s 可精确判定;
  CDC→向量"准实时"的可接受上界。
- **DLQ 速率 > 0**:DLQ 仅在事件反复失败被丢死信时写入,稳态应为 0;`for: 10m` 滤掉短暂尖刺,
  只对**持续**失败告警。
- **DLQ 积压 > 100 / 15m**:replay group 应能把死信清空;积压长期不降说明 replay 停滞。
- **slot active==0**:`slot_monitor.py` 明确 inactive 时 PG 无限堆 WAL 直到磁盘爆,高危,
  `for: 2m` 快速告警(critical)。
- **slot lag > 256MB**:对齐 worker 默认 `SLOT_LAG_WARN_MB=256`(256×1024×1024 = 268435456 字节)。
- **worker 掉线 1m**:`up` 抓取探针,1m 容忍一次抓取抖动后即报。

## 验证

本机无 `promtool`,用 Python 校验 YAML 合法性 + 人工核对 PromQL:

```bash
python3 -c "import yaml,sys; [yaml.safe_load(open(f)) for f in ('monitoring/alerts.yml','monitoring/prometheus.yml')]; print('YAML OK')"
```

有 `promtool` 时:

```bash
promtool check config monitoring/prometheus.yml
promtool check rules monitoring/alerts.yml
```
