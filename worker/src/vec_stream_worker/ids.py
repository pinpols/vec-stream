"""确定性 ID 与文本 hash(幂等基石,见 DESIGN.md §3.3 b/c)。"""
import hashlib
import uuid

# vec_stream 自己的 UUID 命名空间(随机生成一次后固定,不可变更否则 ID 全变)
_NAMESPACE = uuid.UUID("e7b2c4a0-5d1f-4c8e-9a3b-6f2d8e1c4b7a")


def vector_id(tenant_id: str, table: str, pk: str, chunk_index: int) -> str:
    return hashlib.sha256(f"{tenant_id}:{table}:{pk}:{chunk_index}".encode()).hexdigest()


def qdrant_point_id(tenant_id: str, table: str, pk: str, chunk_index: int) -> str:
    """Qdrant 只接受 UUID / 整数作为 point id:同一逻辑键派生 uuid5,幂等性等价。"""
    return str(uuid.uuid5(_NAMESPACE, f"{tenant_id}:{table}:{pk}:{chunk_index}"))


def text_hash(source_text: str) -> str:
    return hashlib.sha256(source_text.encode()).hexdigest()
