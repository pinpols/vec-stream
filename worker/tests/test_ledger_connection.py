"""P2-5:BestEffortLedger 持有懒重建连接(不再每事件新建 PG 连接)。

原 record_best_effort 每条事件 psycopg.connect 一次,高吞吐下连接建立
(TCP/auth 往返)成为 Qdrant 后端的隐性开销。改为照 VectorSink 的连接
韧性模式:常驻连接懒创建,写失败丢弃重连再试一次,仍失败只 warn
(账本是审计参考,不拖累主写入)。"""

from unittest.mock import MagicMock

import psycopg
import pytest

from vec_stream_worker import ledger


@pytest.fixture
def fake_connect(monkeypatch):
    """替换 psycopg.connect,返回可编程的假连接序列。"""
    conns = []

    def factory(*a, **k):
        conn = MagicMock()
        conn.closed = False
        conns.append(conn)
        return conn

    monkeypatch.setattr(ledger.psycopg, "connect", factory)
    return conns


def test_connection_reused_across_events(fake_connect):
    led = ledger.BestEffortLedger("postgresql://ignored")
    log = MagicMock()
    led.record(("t", 0, 1), log)
    led.record(("t", 0, 2), log)
    assert len(fake_connect) == 1  # 只建一次连接
    assert fake_connect[0].execute.call_count == 2
    log.warning.assert_not_called()


def test_broken_connection_rebuilt_and_retried(fake_connect):
    led = ledger.BestEffortLedger("postgresql://ignored")
    log = MagicMock()
    led.record(("t", 0, 1), log)
    # 连接坏死:下一次 execute 抛,期望丢弃重连并重试成功(自愈,不 warn)
    fake_connect[0].execute.side_effect = psycopg.OperationalError("gone")
    led.record(("t", 0, 2), log)
    assert len(fake_connect) == 2
    fake_connect[1].execute.assert_called_once()
    log.warning.assert_not_called()


def test_persistent_failure_warns_but_does_not_raise(fake_connect):
    led = ledger.BestEffortLedger("postgresql://ignored")
    log = MagicMock()
    led.record(("t", 0, 1), log)
    fake_connect[0].execute.side_effect = psycopg.OperationalError("down")
    # 重连后的新连接也失败 → 只 warn 不抛
    orig = len(fake_connect)

    def always_fail(*a, **k):
        conn = MagicMock()
        conn.closed = False
        conn.execute.side_effect = psycopg.OperationalError("still down")
        fake_connect.append(conn)
        return conn

    ledger.psycopg.connect = always_fail
    led.record(("t", 0, 2), log)  # 不抛
    assert len(fake_connect) > orig
    log.warning.assert_called_once()


def test_none_dsn_or_offset_is_noop(fake_connect):
    log = MagicMock()
    ledger.BestEffortLedger(None).record(("t", 0, 1), log)
    ledger.BestEffortLedger("postgresql://x").record(None, log)
    assert fake_connect == []


def test_close_discards_connection(fake_connect):
    led = ledger.BestEffortLedger("postgresql://ignored")
    led.record(("t", 0, 1), MagicMock())
    led.close()
    fake_connect[0].close.assert_called_once()
    # close 后再记账 → 懒重建
    led.record(("t", 0, 2), MagicMock())
    assert len(fake_connect) == 2
