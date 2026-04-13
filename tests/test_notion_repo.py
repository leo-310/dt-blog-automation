from __future__ import annotations

import unittest

from blog_agent.notion_repo import (
    _as_rich_text_array,
    _get_checkbox,
    _get_relation_ids,
    _get_rich_text,
    _safe_json,
)


class NotionMappingTests(unittest.TestCase):
    def test_rich_text_chunking(self) -> None:
        source = "a" * 4100
        chunks = _as_rich_text_array(source, chunk_size=1800)
        self.assertEqual(len(chunks), 3)
        rebuilt = "".join(item["text"]["content"] for item in chunks)
        self.assertEqual(rebuilt, source)

    def test_get_rich_text(self) -> None:
        prop = {
            "rich_text": [
                {"plain_text": "hello"},
                {"plain_text": " world"},
            ]
        }
        self.assertEqual(_get_rich_text(prop), "hello world")

    def test_relation_and_checkbox(self) -> None:
        relation_prop = {"relation": [{"id": "abc"}, {"id": "xyz"}]}
        self.assertEqual(_get_relation_ids(relation_prop), ["abc", "xyz"])
        self.assertTrue(_get_checkbox({"checkbox": True}, default=False))
        self.assertFalse(_get_checkbox({"checkbox": False}, default=True))
        self.assertTrue(_get_checkbox({}, default=True))

    def test_safe_json(self) -> None:
        self.assertEqual(_safe_json('{"a":1}')["a"], 1)
        self.assertIsNone(_safe_json("{not-json}"))


if __name__ == "__main__":
    unittest.main()
