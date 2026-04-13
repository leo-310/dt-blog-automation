from __future__ import annotations

import io
import json
import os
import unittest

from blog_agent.api import BlogAgentApi


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
        self.app = BlogAgentApi()

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


if __name__ == "__main__":
    unittest.main()
