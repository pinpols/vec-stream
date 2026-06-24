"""RAG 生成层:召回结果拼 prompt → Claude 生成带 [n] 引用的回答。"""
import os

import anthropic

CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-8")

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


def generate_answer(question: str, sources: list[dict]) -> dict:
    """调 Claude 生成回答。环境变量 ANTHROPIC_API_KEY 缺失时由调用方兜底。"""
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=16000,
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_prompt(question, sources)}],
    )
    if response.stop_reason == "refusal":
        return {"answer": "(模型拒绝回答该问题)", "model": CLAUDE_MODEL, "usage": {}}
    answer = "".join(b.text for b in response.content if b.type == "text")
    return {
        "answer": answer,
        "model": CLAUDE_MODEL,
        "usage": {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        },
    }
