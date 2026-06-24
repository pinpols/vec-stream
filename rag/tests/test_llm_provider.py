"""LLM 生成层:统一走 OpenAI 兼容协议(base_url 指 agent-ctl 网关或任意兼容服务)。"""
import sys
import types

from vec_stream_rag import llm

SOURCES = [{"n": 1, "title": "T", "content": "C"}]


def _install_fake_openai(monkeypatch, captured):
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


def test_generate_answer_openai_compatible(monkeypatch):
    captured = {}
    monkeypatch.setattr(llm, "LLM_MODEL", "deepseek-chat")
    monkeypatch.setattr(llm, "OPENAI_BASE_URL", "http://localhost:8400/v1")
    _install_fake_openai(monkeypatch, captured)

    out = llm.generate_answer("Q", SOURCES)
    assert out["answer"] == "答案 [1]"
    assert out["model"] == "deepseek-chat"
    assert out["usage"] == {"input_tokens": 11, "output_tokens": 7}
    # base_url 透传(指向网关);system + user 两条消息,user 含资料与问题
    assert captured["base_url"] == "http://localhost:8400/v1"
    msgs = captured["messages"]
    assert msgs[0]["role"] == "system" and msgs[1]["role"] == "user"
    assert "Q" in msgs[1]["content"] and "[1]" in msgs[1]["content"]


def test_api_key_env_is_openai():
    # 统一 OpenAI 兼容 → 始终查 OPENAI_API_KEY(指向网关时为占位)
    assert llm.api_key_env() == "OPENAI_API_KEY"


def test_active_provider_reflects_base_url(monkeypatch):
    monkeypatch.setattr(llm, "OPENAI_BASE_URL", "http://localhost:8400/v1")
    assert "8400" in llm.active_provider()
