from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from blog_agent.storage import parse_markdown_file
from blog_agent.text_files import write_text_file


class TextFileEncodingTests(unittest.TestCase):
    def test_parse_markdown_file_accepts_windows_1252_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "post.md"
            path.write_bytes(
                b"---\n"
                b"title: Windows encoded post\n"
                b"---\n"
                b"Body uses a Windows em dash \x97 and smart quotes \x93like this\x94.\n"
            )

            frontmatter, body = parse_markdown_file(path)

        self.assertEqual(frontmatter["title"], "Windows encoded post")
        self.assertIn("em dash -", body.replace("\u2014", "-"))
        self.assertIn("\u201clike this\u201d", body)

    def test_write_text_file_emits_utf8(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "post.md"
            write_text_file(path, "Body uses an em dash \u2014 safely.\n")

            raw = path.read_bytes()

        self.assertEqual(raw.decode("utf-8"), "Body uses an em dash \u2014 safely.\n")
        self.assertNotIn(b"\x97", raw)


if __name__ == "__main__":
    unittest.main()
