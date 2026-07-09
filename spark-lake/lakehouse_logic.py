"""Small pure helpers shared by spark-lake scripts and local tests."""

from __future__ import annotations

import base64

EVENT_POS_FORMAT = "%020d:%020d"

# TOAST 边界:pgoutput 对 UPDATE 中**未变更的 TOAST 大列**不写 WAL,Debezium 在
# after 镜像里填占位符(connector 配置 `unavailable.value.placeholder` 的默认值);
# REPLICA IDENTITY FULL 只保证 before 镜像完整。字符串列占位符是字面量;
# bytea 列经 JSON converter base64 序列化后是其 base64 形态(两种都要挡)。
# 见 Debezium PostgreSQL connector 文档 "toasted values" 一节。
DEBEZIUM_UNAVAILABLE_VALUE = "__debezium_unavailable_value"
DEBEZIUM_UNAVAILABLE_VALUE_B64 = base64.b64encode(DEBEZIUM_UNAVAILABLE_VALUE.encode()).decode()
UNAVAILABLE_VALUES = (DEBEZIUM_UNAVAILABLE_VALUE, DEBEZIUM_UNAVAILABLE_VALUE_B64)


def resolve_toasted(after_value: str | None, before_value: str | None) -> str | None:
    """字符串列取值:after 是 TOAST 占位符 → 回退 before(RI FULL 下有真值)。

    此函数是 cdc_to_hudi / cdc_to_iceberg 中 Spark 列表达式
    `when(after.isin(*UNAVAILABLE_VALUES), before).otherwise(after)` 的纯 Python
    等价镜像,供本地单测锁定语义。"""
    if after_value in UNAVAILABLE_VALUES:
        return before_value
    return after_value


def format_event_pos(lsn_or_ts: int | None, offset: int) -> str:
    return EVENT_POS_FORMAT % (int(lsn_or_ts or 0), int(offset))


def normalize_hudi_record_key(value: str | None) -> str:
    return (value or "").replace(" ", "").strip("[]")


def parse_query_key_args(args: list[str]) -> tuple[str, str | None]:
    tenant_id = "default"
    pk = None
    if len(args) == 1:
        pk = args[0]
    elif len(args) >= 2:
        tenant_id = args[0]
        pk = args[1]
    return tenant_id, pk
