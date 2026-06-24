"""prompt 构建纯函数测试(不调 API)。"""

from vec_stream_rag.llm import build_context, build_user_prompt

SOURCES = [
    {"n": 1, "title": "pgvector 入门", "content": "pgvector 支持 HNSW 索引。"},
    {"n": 2, "title": None, "content": "无标题资料内容。"},
]


def test_build_context_numbers_and_titles():
    ctx = build_context(SOURCES)
    assert "[1] pgvector 入门" in ctx
    assert "pgvector 支持 HNSW 索引。" in ctx
    assert "[2]" in ctx
    assert "None" not in ctx  # title 为 None 不能渗进 prompt


def test_build_user_prompt_contains_question_and_context():
    p = build_user_prompt("HNSW 是什么?", SOURCES)
    assert p.index("资料:") < p.index("问题:HNSW 是什么?")
    assert "[1] pgvector 入门" in p
