"""LLM provider 可插拔分发(anthropic / openai 兼容协议)。"""
import sys
import types

import pytest

from vec_stream_rag import llm

SOURCES = [{"n": 1, "title": "T", "content": "C"}]


def test_dispatch_openai(monkeypatch):
    monkeypatch.setattr(llm, "LLM_PROVIDER", "openai")

    # 构造假 openai SDK 模块
    captured = {}

    class FakeUsage:
        prompt_tokens = 11
        completion_tokens = 7

    class FakeMsg:
        content = "答案 [1]"

    class FakeChoice:
        message = FakeMsg()

    class FakeResp:
        choices = [FakeChoice()]
        usage = FakeUsage()

    class FakeCompletions:
        def create(self, **kw):
            captured.update(kw)
            return FakeResp()

    class FakeChat:
        completions = FakeCompletions()

    class FakeOpenAI:
        def __init__(self, base_url=None):
            captured["base_url"] = base_url
            self.chat = FakeChat()

    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = FakeOpenAI
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    out = llm.generate_answer("Q", SOURCES)
    assert out["answer"] == "答案 [1]"
    assert out["usage"] == {"input_tokens": 11, "output_tokens": 7}
    # system + user 两条消息,prompt 含资料与问题
    msgs = captured["messages"]
    assert msgs[0]["role"] == "system" and msgs[1]["role"] == "user"
    assert "Q" in msgs[1]["content"] and "[1]" in msgs[1]["content"]


def test_dispatch_anthropic(monkeypatch):
    monkeypatch.setattr(llm, "LLM_PROVIDER", "anthropic")

    class FakeUsage:
        input_tokens = 5
        output_tokens = 3

    class FakeBlock:
        type = "text"
        text = "Claude 答 [1]"

    class FakeResp:
        stop_reason = "end_turn"
        content = [FakeBlock()]
        usage = FakeUsage()

    class FakeMessages:
        def create(self, **kw):
            return FakeResp()

    class FakeAnthropic:
        def __init__(self, *a, **k):
            self.messages = FakeMessages()

    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = FakeAnthropic
    monkeypatch.setitem(sys.modules, "anthropic", fake_mod)

    out = llm.generate_answer("Q", SOURCES)
    assert out["answer"] == "Claude 答 [1]"
    assert out["usage"] == {"input_tokens": 5, "output_tokens": 3}


def test_unknown_provider(monkeypatch):
    monkeypatch.setattr(llm, "LLM_PROVIDER", "grok")
    with pytest.raises(ValueError, match="未知 LLM_PROVIDER"):
        llm.generate_answer("Q", SOURCES)


def test_api_key_env_switches(monkeypatch):
    monkeypatch.setattr(llm, "LLM_PROVIDER", "openai")
    assert llm.api_key_env() == "OPENAI_API_KEY"
    monkeypatch.setattr(llm, "LLM_PROVIDER", "anthropic")
    assert llm.api_key_env() == "ANTHROPIC_API_KEY"
