"""文本切分。阶段 0 按字符长度切(中文场景字符数≈token 数量级),
阶段 1 再换成模型 tokenizer 精确按 token 切。"""


def split_text(text: str, chunk_size: int = 400, overlap: int = 50) -> list[str]:
    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")
    text = text.strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    step = chunk_size - overlap
    for start in range(0, len(text), step):
        chunk = text[start : start + chunk_size]
        chunks.append(chunk)
        if start + chunk_size >= len(text):
            break
    return chunks
