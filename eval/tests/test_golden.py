"""Golden set 加载单测 + 仓库内 golden/queries.jsonl 可解析校验。"""
import pytest

from vec_stream_eval.golden import GoldenQuery, load_golden


def test_repo_golden_loads():
    # 默认 golden set(eval/golden/queries.jsonl)能解析,且非空
    queries = load_golden()
    assert len(queries) >= 5
    for q in queries:
        assert q.query
        assert q.expected_pks


def test_load_custom(tmp_path):
    p = tmp_path / "g.jsonl"
    p.write_text(
        '{"query": "q", "tenant_id": "t", '
        '"expected_pks": [{"source_table": "article", "source_pk": "1"}]}\n'
        "\n",  # 空行应被跳过
        encoding="utf-8",
    )
    queries = load_golden(p)
    assert len(queries) == 1
    assert queries[0].expected_keys() == {("article", "1")}


def test_bad_line_raises(tmp_path):
    p = tmp_path / "bad.jsonl"
    p.write_text("{not json}\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_golden(p)


def test_pk_coerced_to_str():
    # 标注里 source_pk 写成数字,key() 归一为字符串
    q = GoldenQuery.model_validate(
        {"query": "x", "expected_pks": [{"source_table": "article", "source_pk": 1}]}
    )
    assert q.expected_keys() == {("article", "1")}
