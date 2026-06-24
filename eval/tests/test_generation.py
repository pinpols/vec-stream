"""生成质量指标纯函数单测:引用覆盖率 / 带引用句子比 / 引用解析。"""
from vec_stream_eval.generation import (
    evaluate_generation,
    extract_citations,
    lightweight_metrics,
    split_sentences,
)


def test_extract_citations_simple():
    assert extract_citations("答案见 [1] 和 [2]。") == [1, 2]


def test_extract_citations_grouped():
    # [1,2] 多编号一组
    assert extract_citations("综合 [1, 2] 可知。") == [1, 2]


def test_extract_citations_none():
    assert extract_citations("没有任何引用。") == []


def test_split_sentences_mixed_punct():
    sents = split_sentences("这是第一句。这是第二句!第三句?")
    assert len(sents) == 3


def test_citation_coverage_all_valid():
    # 两个引用都指向合法 source {1,2}
    m = lightweight_metrics("结论 [1] 与 [2] 成立。", {1, 2})
    assert m.citation_coverage == 1.0
    assert m.num_valid_citations == 2


def test_citation_coverage_hallucinated():
    # [3] 越界(只有 2 个 source)-> 覆盖率 1/2
    m = lightweight_metrics("依据 [1] 和 [3]。", {1, 2})
    assert m.citation_coverage == 0.5
    assert m.num_valid_citations == 1
    assert m.num_citations == 2


def test_citation_coverage_no_citation():
    m = lightweight_metrics("纯属臆断,没有引用。", {1, 2})
    assert m.citation_coverage == 0.0


def test_cited_sentence_ratio():
    # 2 句,1 句带引用 -> 0.5
    m = lightweight_metrics("第一句有据 [1]。第二句没有。", {1})
    assert m.num_sentences == 2
    assert m.cited_sentence_ratio == 0.5


def test_evaluate_generation_aggregate():
    samples = [
        {
            "query": "q1",
            "answer": "结论 [1]。",
            "sources": [{"n": 1, "content": "x"}],
        },
        {
            "query": "q2",
            # 引用 [2] 越界(只有 1 个 source n=1)-> coverage 0
            "answer": "结论 [2]。",
            "sources": [{"n": 1, "content": "y"}],
        },
    ]
    rep = evaluate_generation(samples)
    assert rep.num_queries == 2
    # q1 coverage=1, q2 coverage=0 -> mean 0.5
    assert rep.mean_citation_coverage == 0.5
    assert rep.backend in ("ragas", "lightweight")
    assert '"mean_citation_coverage"' in rep.to_json()


def test_evaluate_generation_uses_explicit_source_n():
    # source 的 n 是 5(非连续),引用 [5] 应判为合法
    samples = [{"query": "q", "answer": "见 [5]。", "sources": [{"n": 5}]}]
    rep = evaluate_generation(samples)
    assert rep.mean_citation_coverage == 1.0
