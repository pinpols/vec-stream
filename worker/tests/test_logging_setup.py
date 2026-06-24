import json
import logging

import pytest

from vec_stream_worker.logging_setup import JsonFormatter, setup_logging


@pytest.fixture(autouse=True)
def _restore_logging():
    """每个用例后还原 root logger,避免相互污染。"""
    root = logging.getLogger()
    saved = root.handlers[:]
    saved_level = root.level
    yield
    for h in root.handlers[:]:
        root.removeHandler(h)
    for h in saved:
        root.addHandler(h)
    root.setLevel(saved_level)


def _make_record(msg="hi", args=(), extra=None, name="x", exc_info=None):
    record = logging.LogRecord(
        name=name,
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


def test_json_formatter_parseable_with_required_fields():
    line = JsonFormatter().format(_make_record(msg="upserted %s pk=%s", args=("article", 42)))
    obj = json.loads(line)
    assert {"ts", "level", "logger", "msg"}.issubset(obj)
    assert obj["msg"] == "upserted article pk=42"
    assert obj["level"] == "INFO"
    assert obj["logger"] == "x"


def test_json_formatter_includes_extra_fields():
    line = JsonFormatter().format(_make_record(extra={"k": "v", "tenant": "t1"}))
    obj = json.loads(line)
    assert obj["k"] == "v"
    assert obj["tenant"] == "t1"


def test_json_formatter_non_serializable_extra_falls_back():
    line = JsonFormatter().format(_make_record(extra={"obj": object()}))
    obj = json.loads(line)  # 不应抛错
    assert isinstance(obj["obj"], str)


def test_json_formatter_includes_exc_info():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = _make_record(exc_info=sys.exc_info())
    obj = json.loads(JsonFormatter().format(rec))
    assert "ValueError" in obj["exc_info"]


def test_setup_logging_json_mode_emits_valid_json(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    setup_logging()
    logging.getLogger("x").info("hi", extra={"k": "v"})
    err = capsys.readouterr().err
    line = [ln for ln in err.splitlines() if ln.strip()][-1]
    obj = json.loads(line)
    assert obj["msg"] == "hi"
    assert obj["k"] == "v"


def test_setup_logging_text_mode_does_not_raise(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "text")
    setup_logging()
    logging.getLogger("x").info("plain %s", "msg")
    err = capsys.readouterr().err
    assert "plain msg" in err


def test_setup_logging_default_is_text(monkeypatch):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    setup_logging()
    handler = logging.getLogger().handlers[-1]
    assert not isinstance(handler.formatter, JsonFormatter)


def test_setup_logging_idempotent_single_handler(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    setup_logging()
    setup_logging()
    assert len(logging.getLogger().handlers) == 1


def test_setup_logging_respects_level(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    setup_logging()
    assert logging.getLogger().level == logging.WARNING
