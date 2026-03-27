from __future__ import annotations

import json

import httpx

from .config import AgentConfig


class BlogAgentProvider:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        model: str | None = None,
        max_output_tokens: int | None = None,
    ) -> str:
        if not self.config.api_key:
            raise RuntimeError(
                "Missing API key. Set BLOG_AGENT_API_KEY or OPENAI_API_KEY before running the generator."
            )
        mode = self._resolve_mode()
        if mode == "responses":
            try:
                return self._complete_with_responses(
                    system_prompt,
                    user_prompt,
                    model=model,
                    max_output_tokens=max_output_tokens,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in (400, 404, 422):
                    raise
        return self._complete_with_chat_completions(
            system_prompt,
            user_prompt,
            model=model,
            max_output_tokens=max_output_tokens,
        )

    def _resolve_mode(self) -> str:
        configured = self.config.api_mode.strip().lower()
        if configured in {"responses", "chat"}:
            return configured
        if "api.openai.com" in self.config.api_base_url:
            return "responses"
        return "chat"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _complete_with_responses(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        model: str | None = None,
        max_output_tokens: int | None = None,
    ) -> str:
        url = f"{self.config.api_base_url.rstrip('/')}/responses"
        payload = {
            "model": model or self.config.model,
            "temperature": self.config.temperature,
            "input": [
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": system_prompt}],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_prompt}],
                },
            ],
            "text": {"format": {"type": "text"}},
        }
        token_limit = self.config.max_output_tokens if max_output_tokens is None else max_output_tokens
        if token_limit is not None:
            payload["max_output_tokens"] = token_limit
        response = httpx.post(url, headers=self._headers(), json=payload, timeout=120.0)
        response.raise_for_status()
        body = response.json()
        output = body.get("output", [])
        chunks: list[str] = []
        for item in output:
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    chunks.append(content.get("text", ""))
        text = "".join(chunks).strip()
        if not text:
            raise RuntimeError(f"Unexpected responses payload: {json.dumps(body)[:500]}")
        return text

    def _complete_with_chat_completions(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        model: str | None = None,
        max_output_tokens: int | None = None,
    ) -> str:
        url = f"{self.config.api_base_url.rstrip('/')}/chat/completions"
        payload = {
            "model": model or self.config.model,
            "temperature": self.config.temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        token_limit = self.config.max_output_tokens if max_output_tokens is None else max_output_tokens
        if token_limit is not None:
            payload["max_tokens"] = token_limit
        response = httpx.post(url, headers=self._headers(), json=payload, timeout=120.0)
        response.raise_for_status()
        body = response.json()
        choice = body["choices"][0]["message"]["content"]
        if isinstance(choice, list):
            return "".join(
                part.get("text", "") for part in choice if isinstance(part, dict)
            )
        if not isinstance(choice, str):
            raise RuntimeError(f"Unexpected provider response: {json.dumps(body)[:500]}")
        return choice

    def generate_image(
        self,
        *,
        prompt: str,
        model: str,
        quality: str,
        size: str,
        output_format: str,
    ) -> dict:
        if not self.config.api_key:
            raise RuntimeError(
                "Missing API key. Set BLOG_AGENT_API_KEY or OPENAI_API_KEY before generating images."
            )
        url = f"{self.config.api_base_url.rstrip('/')}/images/generations"
        payload = {
            "model": model,
            "prompt": prompt,
            "quality": quality,
            "size": size,
            "output_format": output_format,
        }
        response = httpx.post(url, headers=self._headers(), json=payload, timeout=180.0)
        response.raise_for_status()
        body = response.json()
        data = body.get("data", [])
        if not data or not isinstance(data[0], dict) or not data[0].get("b64_json"):
            raise RuntimeError(f"Unexpected image response payload: {json.dumps(body)[:500]}")
        return body
