"""DLQ replay 关键路径测试:重投/归档/乒乓水位线/归档失败不拖垮(用 fake Consumer/Producer)。"""

import pytest

from vec_stream_worker import dlq_replay
from vec_stream_worker.config import Config


class FakeMsg:
    def __init__(self, partition, offset, headers, value=b"v", key=b"k"):
        self._p, self._o, self._h, self._v, self._k = partition, offset, headers, value, key

    def error(self):
        return None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def headers(self):
        return self._h

    def value(self):
        return self._v

    def key(self):
        return self._k


class _PartMeta:
    def __init__(self, partitions):
        self.partitions = partitions
        self.error = None


class _TopicsMeta:
    def __init__(self, topic, partitions):
        self.topics = {topic: _PartMeta(partitions)}


class FakeConsumer:
    def __init__(self, msgs, hi):
        self._msgs = list(msgs)
        self.hi = hi
        self.committed = []

    def list_topics(self, topic, timeout=10):
        return _TopicsMeta(topic, {0: None})

    def get_watermark_offsets(self, tp, timeout=10):
        return (0, self.hi)

    def assign(self, a):
        pass

    def poll(self, t):
        return self._msgs.pop(0) if self._msgs else None

    def commit(self, msg):
        self.committed.append(msg.offset())

    def close(self):
        pass


class FakeProducer:
    def __init__(self, *a, **k):
        self.produced = []

    def produce(self, topic, key, value, headers):
        self.produced.append((topic, dict(headers)))

    def flush(self, t):
        pass


@pytest.fixture
def wired(monkeypatch):
    """返回一个 (msgs, hi) → (consumer, producer) 的装配器,patch 掉 Consumer/Producer。"""
    holder = {}

    def setup(msgs, hi):
        fc, fp = FakeConsumer(msgs, hi), FakeProducer()
        monkeypatch.setattr(dlq_replay, "Consumer", lambda conf: fc)
        monkeypatch.setattr(dlq_replay, "Producer", lambda conf: fp)
        holder["c"], holder["p"] = fc, fp
        return fc, fp

    return setup


def test_under_limit_reproduces_with_incremented_count(wired):
    fc, fp = wired(
        [FakeMsg(0, 0, [("source_topic", b"cdc.public.article"), ("replay_count", b"1")])], hi=1
    )
    n = dlq_replay.replay(Config(), limit=None, dry_run=False)
    assert n == 1
    assert fp.produced[0][0] == "cdc.public.article"
    assert fp.produced[0][1]["replay_count"] == b"2"  # 重投计数 +1
    assert 0 in fc.committed


def test_over_limit_archives_not_reproduced(wired, monkeypatch):
    archived = []
    monkeypatch.setattr(dlq_replay, "_archive", lambda *a, **k: archived.append(a))
    fc, fp = wired([FakeMsg(0, 0, [("source_topic", b"t"), ("replay_count", b"5")])], hi=1)
    dlq_replay.replay(Config(), limit=None, dry_run=False)  # 默认 dlq_max_replays=5
    assert len(archived) == 1
    assert fp.produced == []  # 超限不再回投
    assert 0 in fc.committed


def test_skips_message_at_or_above_watermark(wired):
    # offset==hi 的消息(本次重投回流的)留给下次,防乒乓无限循环
    fc, fp = wired([FakeMsg(0, 1, [("source_topic", b"t"), ("replay_count", b"0")])], hi=1)
    dlq_replay.replay(Config(), limit=None, dry_run=False)
    assert fp.produced == []
    assert fc.committed == []


def test_archive_failure_does_not_commit(wired, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("dead_letter_archive 表不存在")

    monkeypatch.setattr(dlq_replay, "_archive", boom)
    fc, fp = wired([FakeMsg(0, 0, [("source_topic", b"t"), ("replay_count", b"5")])], hi=1)
    dlq_replay.replay(Config(), limit=None, dry_run=False)
    assert fc.committed == []  # 归档失败不 commit,留待下次重试,不丢
