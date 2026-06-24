"""RAG 生成层:召回结果拼 prompt → LLM 生成带 [n] 引用的回答。

可插拔后端(LLM_PROVIDER):
  - anthropic(默认):Anthropic Claude(原生 messages API + adaptive thinking)。
  - openai:**OpenAI 兼容协议**,靠 OPENAI_BASE_URL 切换任意兼容服务——
    OpenAI / DeepSeek / 通义千问 / Moonshot / 本地 Ollama(:11434/v1)/ vLLM 等都走这套。

prompt 与引用规则两后端共用;usage 统一归一成 {input_tokens, output_tokens}。
"""
import os

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic").lower()
# 模型名:优先通用 LLM_MODEL,回退各家默认
ANTHROPIC_MODEL = os.getenv("LLM_MODEL") or os.getenv("CLAUDE_MODEL", "claude-opus-4-8")
OPENAI_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
# OpenAI 兼容服务地址:留空走 openai SDK 默认(api.openai.com);
# DeepSeek=https://api.deepseek.com  通义=https://dashscope.aliyuncs.com/compatible-mode/v1
# Ollama=http://localhost:11434/v1  vLLM=http://localhost:8000/v1
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "") or None
MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "4096"))

SYSTEM_PROMPT = """\
你是一个严谨的知识库问答助手。规则:
1. 只依据「资料」中的内容回答,不使用资料之外的知识。
2. 在引用资料的句子末尾标注来源编号,如 [1]、[2];可以标注多个。
3. 如果资料不足以回答问题,直接说明"知识库中没有相关信息",不要编造。
4. 用中文回答,简洁直接。"""


def build_context(sources: list[dict]) -> str:
    """sources: [{n, title, content}, ...] → 编号资料块。"""
    blocks = []
    for s in sources:
        title = s.get("title") or ""
        blocks.append(f"[{s['n']}] {title}\n{s['content']}")
    return "\n\n".join(blocks)


def build_user_prompt(question: str, sources: list[dict]) -> str:
    return f"资料:\n\n{build_context(sources)}\n\n问题:{question}"


def active_provider() -> str:
    return LLM_PROVIDER


def api_key_env() -> str:
    """当前 provider 需要的 API key 环境变量名(/ask 可用性检查用)。"""
    return "ANTHROPIC_API_KEY" if LLM_PROVIDER == "anthropic" else "OPENAI_API_KEY"


def llm_available() -> bool:
    return bool(os.getenv(api_key_env()))


def _generate_anthropic(question: str, sources: list[dict]) -> dict:
    import anthropic

    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=max(MAX_TOKENS, 1024),
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_prompt(question, sources)}],
    )
    if resp.stop_reason == "refusal":
        return {"answer": "(模型拒绝回答该问题)", "model": ANTHROPIC_MODEL, "usage": {}}
    answer = "".join(b.text for b in resp.content if b.type == "text")
    return {
        "answer": answer,
        "model": ANTHROPIC_MODEL,
        "usage": {
            "input_tokens": resp.usage.input_tokens,
            "output_tokens": resp.usage.output_tokens,
        },
    }


def _generate_openai(question: str, sources: list[dict]) -> dict:
    from openai import OpenAI

    client = OpenAI(base_url=OPENAI_BASE_URL)  # api_key 从 OPENAI_API_KEY 读
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        max_tokens=MAX_TOKENS,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(question, sources)},
        ],
    )
    answer = resp.choices[0].message.content or ""
    usage = getattr(resp, "usage", None)
    return {
        "answer": answer,
        "model": OPENAI_MODEL,
        "usage": {
            "input_tokens": getattr(usage, "prompt_tokens", None),
            "output_tokens": getattr(usage, "completion_tokens", None),
        }
        if usage
        else {},
    }


def generate_answer(question: str, sources: list[dict]) -> dict:
    """按 LLM_PROVIDER 分发到对应后端。返回 {answer, model, usage}。"""
    if LLM_PROVIDER == "openai":
        return _generate_openai(question, sources)
    if LLM_PROVIDER == "anthropic":
        return _generate_anthropic(question, sources)
    raise ValueError(f"未知 LLM_PROVIDER={LLM_PROVIDER},支持 anthropic|openai(含 OpenAI 兼容服务)")
