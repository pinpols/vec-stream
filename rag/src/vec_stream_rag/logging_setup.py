"""结构化日志初始化(企业级可采集需求)。

按环境变量切换日志格式,零新增第三方依赖(标准库 logging 实现 JSON formatter):
  LOG_FORMAT=text(默认)→ 人类可读,保持现有行为(向后兼容,本地开发友好)。
  LOG_FORMAT=json        → 每行一个 JSON,含 ts/level/logger/msg + record 的 extra 字段,
                           便于直接进 ELK / Loki。
  LOG_LEVEL=INFO(默认)  → 日志级别。

幂等:重复调用 setup_logging() 会先清空 root 已有 handler 再装配,避免重复输出。
"""

import datetime as _dt
import json
import logging
import os

# logging.LogRecord 自带的标准属性集合;凡不在此集合的 record 属性即视为业务 extra,
# JSON 模式下原样带出(支持 log.info("...", extra={"k": "v"}))。
_STD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """把 LogRecord 序列化成单行 JSON。

    固定字段 ts / level / logger / msg;record.getMessage() 完成 %-格式化
    (worker 多为 log.info("upserted %s pk=%s", ...) 形式,JSON 化结果即拼好的 message);
    异常信息进 exc_info 字段;其余非标准属性作为顶层 extra 字段带出。
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": _dt.datetime.fromtimestamp(record.created, _dt.UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # 业务 extra 字段(不覆盖固定字段)
        for key, value in record.__dict__.items():
            if key in _STD_ATTRS or key in payload:
                continue
            payload[key] = _json_safe(value)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False)


def _json_safe(value):
    """extra 值可能不可直接 JSON 序列化,兜底转字符串,避免日志本身抛错。"""
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except (TypeError, ValueError):
        return repr(value)


_TEXT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging() -> None:
    """根据 LOG_FORMAT / LOG_LEVEL 装配 root logger 的单个 StreamHandler。

    给 root 配 handler(而非具名 logger),从而同时覆盖本模块日志与
    第三方库(uvicorn/confluent_kafka 等)经 root 传播的日志。
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = os.getenv("LOG_FORMAT", "text").lower()

    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(_TEXT_FORMAT))

    root = logging.getLogger()
    # 幂等:清掉已有 handler(含 basicConfig / 重复调用残留),避免重复行
    for existing in root.handlers[:]:
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
