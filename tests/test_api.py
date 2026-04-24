from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from blog_agent.api import BlogAgentApi, build_orphan_pipeline_rows_from_posts


def run_wsgi_request(app: BlogAgentApi, method: str, path: str, payload: dict | None = None) -> tuple[str, dict]:
    body = b""
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    status_line = ""
    headers = []

    def start_response(status: str, response_headers: list[tuple[str, str]]) -> None:
        nonlocal status_line, headers
        status_line = status
        headers = response_headers

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": io.BytesIO(body),
    }
    response_body = b"".join(app.wsgi_app(environ, start_response))
    parsed = json.loads(response_body.decode("utf-8"))
    _ = headers
    return status_line, parsed


class ApiRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["BLOG_AGENT_USE_NOTION"] = "0"
        self._temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp_dir.cleanup)
        self.app = BlogAgentApi()
        self.app.local_settings_file = Path(self._temp_dir.name) / "automation_settings.yaml"

    def test_get_settings_route(self) -> None:
        status, payload = run_wsgi_request(self.app, "GET", "/api/settings")
        self.assertTrue(status.startswith("200"))
        self.assertIn("settings", payload)
        self.assertIn("dailyTime", payload["settings"])

    def test_put_settings_route(self) -> None:
        status, payload = run_wsgi_request(
            self.app,
            "PUT",
            "/api/settings",
            payload={"settings": {"dailyTime": "08:45", "timezone": "UTC", "enabled": True}},
        )
        self.assertTrue(status.startswith("200"))
        self.assertEqual(payload["settings"]["dailyTime"], "08:45")
        self.assertEqual(payload["settings"]["timezone"], "UTC")

    def test_run_now_route(self) -> None:
        self.app.run_automation_now = lambda: {"executed": True, "reason": "test"}  # type: ignore[method-assign]
        status, payload = run_wsgi_request(self.app, "POST", "/api/automation/run-now", payload={})
        self.assertTrue(status.startswith("201"))
        self.assertTrue(payload["executed"])

    def test_build_orphan_pipeline_rows_from_posts(self) -> None:
        existing = [
            {
                "id": "topic-1",
                "post_id": "already-tracked.md",
                "path": "/tmp/already-tracked.md",
            }
        ]
        posts = [
            {
                "id": "already-tracked.md",
                "title": "Already Tracked",
                "date": "2026-04-20",
            },
            {
                "id": "manual-generated.md",
                "title": "Manual Generated",
                "description": "Manual description",
                "excerpt": "Manual excerpt",
                "date": "2026-04-19",
                "html": "<p>Manual body</p>",
            },
        ]

        rows = build_orphan_pipeline_rows_from_posts(posts=posts, existing_items=existing)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["post_id"], "manual-generated.md")
        self.assertEqual(row["status"], "draft")
        self.assertEqual(row["scheduled_for"], "2026-04-19")
        self.assertEqual(row["created_at"], "2026-04-19T00:00:00+00:00")
        self.assertTrue(row["hasGeneratedDraft"])
        self.assertTrue(row["metadata"].get("manual_import"))


if __name__ == "__main__":
    unittest.main()
