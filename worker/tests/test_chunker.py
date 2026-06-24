import pytest

from vec_stream_worker.chunker import split_text


def test_short_text_single_chunk():
    assert split_text("短文本", 400, 50) == ["短文本"]


def test_empty_text_no_chunks():
    assert split_text("   ", 400, 50) == []


def test_long_text_chunks_cover_all_content():
    text = "x" * 1000
    chunks = split_text(text, 400, 50)
    assert all(len(c) <= 400 for c in chunks)
    # 重叠拼接后必须覆盖原文
    rebuilt = chunks[0]
    for c in chunks[1:]:
        rebuilt += c[50:]
    assert rebuilt == text


def test_overlap_between_adjacent_chunks():
    text = "abcdefghij" * 20  # 200 chars
    chunks = split_text(text, 100, 20)
    assert chunks[0][-20:] == chunks[1][:20]


def test_invalid_overlap_raises():
    with pytest.raises(ValueError):
        split_text("abc", 100, 100)
