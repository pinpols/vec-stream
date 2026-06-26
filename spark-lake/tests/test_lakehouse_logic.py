import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lakehouse_logic import format_event_pos, normalize_hudi_record_key, parse_query_key_args


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

    def test_parse_query_key_args_is_backward_compatible(self):
        self.assertEqual(parse_query_key_args([]), ("default", None))
        self.assertEqual(parse_query_key_args(["42"]), ("default", "42"))
        self.assertEqual(parse_query_key_args(["acme", "42"]), ("acme", "42"))


if __name__ == "__main__":
    unittest.main()
