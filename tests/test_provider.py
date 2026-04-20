from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import httpx

from blog_agent.config import AgentConfig
from blog_agent.provider import BlogAgentProvider


class ProviderRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["OPENAI_API_KEY"] = "test-key"
        os.environ["BLOG_AGENT_API_MODE"] = "responses"
        os.environ["BLOG_AGENT_API_MAX_RETRIES"] = "2"
        os.environ["BLOG_AGENT_API_RETRY_BASE_SECONDS"] = "0.01"
        os.environ["BLOG_AGENT_API_RETRY_MAX_SECONDS"] = "0.05"
        self.provider = BlogAgentProvider(AgentConfig())

    @patch("blog_agent.provider.time.sleep", return_value=None)
    def test_complete_retries_on_429_then_succeeds(self, _sleep) -> None:
        request = httpx.Request("POST", "https://api.openai.com/v1/responses")
        first = httpx.Response(429, json={"error": {"message": "rate limit"}}, headers={"Retry-After": "0"}, request=request)
        second = httpx.Response(
            200,
            json={
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "ok"}],
                    }
                ]
            },
            request=request,
        )
        with patch("blog_agent.provider.httpx.post", side_effect=[first, second]) as mocked:
            text = self.provider.complete("sys", "user")

        self.assertEqual(text, "ok")
        self.assertEqual(mocked.call_count, 2)


if __name__ == "__main__":
    unittest.main()
