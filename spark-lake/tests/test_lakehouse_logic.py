import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lakehouse_logic import (
    DEBEZIUM_UNAVAILABLE_VALUE,
    DEBEZIUM_UNAVAILABLE_VALUE_B64,
    format_event_pos,
    normalize_hudi_record_key,
    parse_query_key_args,
    resolve_toasted,
)


class LakehouseLogicTest(unittest.TestCase):
    def test_format_event_pos_sorts_by_lsn_then_offset(self):
        older = format_event_pos(100, 999)
        newer = format_event_pos(101, 1)
        same_lsn_newer_offset = format_event_pos(100, 1000)

        self.assertLess(older, newer)
        self.assertLess(older, same_lsn_newer_offset)

    def test_format_event_pos_handles_null_lsn_without_overflow_math(self):
        self.assertEqual(
            format_event_pos(None, 7),
            "00000000000000000000:00000000000000000007",
        )

    def test_normalize_hudi_record_key_variants(self):
        self.assertEqual(normalize_hudi_record_key("tenant_id,id"), "tenant_id,id")
        self.assertEqual(normalize_hudi_record_key("[tenant_id, id]"), "tenant_id,id")
        self.assertEqual(normalize_hudi_record_key(" id "), "id")
        self.assertEqual(normalize_hudi_record_key(None), "")

    def test_resolve_toasted_placeholder_falls_back_to_before(self):
        # pgoutput 对 UPDATE 中未变更的 TOAST 大列在 after 填占位符,
        # REPLICA IDENTITY FULL 保证 before 有真值 → 必须回退 before
        self.assertEqual(resolve_toasted(DEBEZIUM_UNAVAILABLE_VALUE, "真正文"), "真正文")

    def test_resolve_toasted_base64_placeholder_falls_back_to_before(self):
        # bytea 列经 JSON converter base64 后,占位符是其 base64 形态
        self.assertEqual(
            DEBEZIUM_UNAVAILABLE_VALUE_B64, "X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ=="
        )
        self.assertEqual(resolve_toasted(DEBEZIUM_UNAVAILABLE_VALUE_B64, "raw"), "raw")

    def test_resolve_toasted_normal_value_passes_through(self):
        self.assertEqual(resolve_toasted("新正文", "旧正文"), "新正文")
        self.assertIsNone(resolve_toasted(None, "旧正文"))

    def test_parse_query_key_args_is_backward_compatible(self):
        self.assertEqual(parse_query_key_args([]), ("default", None))
        self.assertEqual(parse_query_key_args(["42"]), ("default", "42"))
        self.assertEqual(parse_query_key_args(["acme", "42"]), ("acme", "42"))


if __name__ == "__main__":
    unittest.main()
