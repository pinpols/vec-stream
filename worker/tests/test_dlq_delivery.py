"""P2-2:DLQ produce 必须校验 delivery 结果。

此前 dlq.produce 后只 flush 不看结果:broker 拒收/超时时消息静默丢失,
调用方照常 commit offset → 毒消息既不在 DLQ 也不会再被消费,凭空蒸发。
现在注册 delivery callback 收集失败 + 检查 flush 返回的残留数,
任一失败都抛异常,让调用方不 commit(进程重启后重试)。
"""

import pytest

from vec_stream_worker.config import Config
from vec_stream_worker.main import handle_message


class FakeMsg:
    """毒消息:非法 JSON → 数据性错误,有限重试后走 DLQ 路径。"""

    def __init__(self):
        self._value = b"not-json{{"

    def topic(self):
        return "cdc.public.article"

    def partition(self):
        return 0

    def offset(self):
        return 42

    def key(self):
        return b"k"

    def value(self):
        return self._value

    def headers(self):
        return None


class FakeDlqProducer:
    """模拟 confluent_kafka.Producer 的 delivery callback 语义:
    produce 记下 callback,flush 时按预设结果回调并返回残留数。"""

    def __init__(self, deliver_error=None, flush_remaining=0):
        self.deliver_error = deliver_error
        self.flush_remaining = flush_remaining
        self.produced = []
        self._callbacks = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        if on_delivery is not None:
            self._callbacks.append(on_delivery)

    def flush(self, timeout=None):
        for cb in self._callbacks:
            cb(self.deliver_error, object())
        self._callbacks = []
        return self.flush_remaining


def _cfg():
    # Config 是 frozen dataclass → 用 dataclasses.replace 覆盖重试参数
    import dataclasses

    return dataclasses.replace(Config(), max_retries=1, retry_backoff_s=0.0)


def _handle(dlq):
    handle_message(FakeMsg(), _cfg(), embedder=None, sink=None, dlq=dlq, source_db=None)


def test_dlq_delivery_success_no_raise():
    dlq = FakeDlqProducer()
    _handle(dlq)  # 不抛 → 调用方正常 commit
    assert len(dlq.produced) == 1
    assert dlq.produced[0]["headers"]["source_offset"] == "42"


def test_dlq_delivery_error_raises():
    dlq = FakeDlqProducer(deliver_error="broker rejected")
    with pytest.raises(RuntimeError, match="DLQ"):
        _handle(dlq)


def test_dlq_flush_residual_raises():
    dlq = FakeDlqProducer(flush_remaining=1)
    with pytest.raises(RuntimeError, match="DLQ"):
        _handle(dlq)
