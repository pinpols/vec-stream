"""RAG 生成层:召回结果拼 prompt → OpenAI 兼容 API 生成带 [n] 引用的回答。

**统一走 OpenAI 兼容协议**(一条路):
  - 推荐 `OPENAI_BASE_URL` 指向 **agent-ctl 网关**——自动获路由/回退/成本/缓存/全量捕获,
    且网关侧用原生 Anthropic SDK 处理 Claude(故 rag 无需自带 anthropic 分支)。
  - 也可直指任意 OpenAI 兼容服务:OpenAI / DeepSeek / 通义千问 / Moonshot / 本地 Ollama / vLLM。

模型由 `LLM_MODEL` 指定(网关 model_aliases 里的别名,或 provider/model 直连)。
"""

import os

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
# OpenAI 兼容服务地址:留空走 openai SDK 默认(api.openai.com)。指向 agent-ctl 网关 = http://host:8400/v1
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
    """当前生成目标(诊断/健康检查展示用)。统一 OpenAI 兼容,具体由 base_url / 网关决定。"""
    return f"openai-compatible({OPENAI_BASE_URL or 'api.openai.com'})"


def api_key_env() -> str:
    """生成调用需要的 API key 环境变量名。统一 OpenAI 兼容 → OPENAI_API_KEY
    (指向 agent-ctl 网关时为占位值,真 key 配在网关侧)。"""
    return "OPENAI_API_KEY"


def llm_egress_allowed() -> bool:
    """显式允许把召回资料发给 OpenAI-compatible 端点后,/ask 才可用。"""
    return os.getenv("LLM_EGRESS_ALLOWED", "false").lower() == "true"


def llm_available() -> bool:
    return llm_egress_allowed() and bool(os.getenv(api_key_env()))


def generate_answer(question: str, sources: list[dict]) -> dict:
    """走 OpenAI 兼容 API 生成。返回 {answer, model, usage}。"""
    if not llm_egress_allowed():
        raise PermissionError("LLM_EGRESS_ALLOWED=true 未配置,拒绝向 LLM 端点发送召回资料")
    from openai import OpenAI

    client = OpenAI(base_url=OPENAI_BASE_URL)  # api_key 从 OPENAI_API_KEY 读
    resp = client.chat.completions.create(
        model=LLM_MODEL,
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
        "model": LLM_MODEL,
        "usage": {
            "input_tokens": getattr(usage, "prompt_tokens", None),
            "output_tokens": getattr(usage, "completion_tokens", None),
        }
        if usage
        else {},
    }
