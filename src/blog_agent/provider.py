from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

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
        if self._resolve_provider() == "gemini":
            return self._complete_with_gemini(
                system_prompt,
                user_prompt,
                model=model,
                max_output_tokens=max_output_tokens,
            )
        self._require_openai_api_key()
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

    def _resolve_provider(self) -> str:
        provider = self.config.provider.strip().lower()
        if provider in {"gemini", "google"}:
            return "gemini"
        return "openai"

    def _require_openai_api_key(self) -> None:
        if not self.config.api_key:
            raise RuntimeError(
                "Missing API key. Set BLOG_AGENT_API_KEY or OPENAI_API_KEY before running the OpenAI generator."
            )

    def _require_gemini_api_key(self) -> None:
        if not self.config.gemini_api_key:
            raise RuntimeError(
                "Missing Gemini API key. Set GEMINI_API_KEY before running the Gemini generator."
            )

    def _resolve_mode(self) -> str:
        configured = self.config.api_mode.strip().lower()
        if configured in {"responses", "chat"}:
            return configured
        if "api.openai.com" in self.config.api_base_url:
            return "responses"
        return "chat"

    def _openai_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def _gemini_headers(self) -> dict[str, str]:
        return {
            "x-goog-api-key": self.config.gemini_api_key,
            "Content-Type": "application/json",
        }

    def _max_retries(self) -> int:
        raw = os.getenv("BLOG_AGENT_API_MAX_RETRIES", "4")
        try:
            return max(0, int(raw))
        except ValueError:
            return 4

    def _retry_base_seconds(self) -> float:
        raw = os.getenv("BLOG_AGENT_API_RETRY_BASE_SECONDS", "2")
        try:
            return max(0.1, float(raw))
        except ValueError:
            return 2.0

    def _retry_max_seconds(self) -> float:
        raw = os.getenv("BLOG_AGENT_API_RETRY_MAX_SECONDS", "30")
        try:
            return max(1.0, float(raw))
        except ValueError:
            return 30.0

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 409, 425, 429, 500, 502, 503, 504}

    def _retry_delay_seconds(self, *, response: httpx.Response | None, attempt: int) -> float:
        retry_max = self._retry_max_seconds()
        if response is not None:
            retry_after = str(response.headers.get("Retry-After", "")).strip()
            if retry_after:
                # Retry-After can be seconds or an HTTP date.
                try:
                    return min(retry_max, max(0.0, float(retry_after)))
                except ValueError:
                    try:
                        retry_at = parsedate_to_datetime(retry_after)
                        if retry_at.tzinfo is None:
                            retry_at = retry_at.replace(tzinfo=timezone.utc)
                        delta = (retry_at - datetime.now(timezone.utc)).total_seconds()
                        return min(retry_max, max(0.0, delta))
                    except (TypeError, ValueError):
                        pass

        backoff = min(retry_max, self._retry_base_seconds() * (2**attempt))
        jitter = min(1.5, backoff * 0.25) * random.random()
        return min(retry_max, backoff + jitter)

    def _post_with_retries(
        self,
        *,
        url: str,
        payload: dict,
        timeout: float,
        headers: dict[str, str],
        provider_name: str,
    ) -> httpx.Response:
        max_retries = self._max_retries()
        attempt = 0
        while True:
            try:
                response = httpx.post(url=url, headers=headers, json=payload, timeout=timeout)
                if response.status_code < 400:
                    return response
                if self._is_retryable_status(response.status_code) and attempt < max_retries:
                    delay = self._retry_delay_seconds(response=response, attempt=attempt)
                    attempt += 1
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                if exc.response is not None and exc.response.status_code == 429:
                    detail = self._extract_error_message(exc.response)
                    raise RuntimeError(
                        f"{provider_name} rate limit or quota reached after retries. "
                        "Wait 1-2 minutes and retry, or lower request frequency. "
                        f"Details: {detail}"
                    ) from exc
                raise
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as exc:
                if attempt >= max_retries:
                    raise RuntimeError(f"Provider request failed after retries: {exc}") from exc
                delay = self._retry_delay_seconds(response=None, attempt=attempt)
                attempt += 1
                time.sleep(delay)

    @staticmethod
    def _extract_error_message(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text[:300] or "unknown error"
        if isinstance(payload, dict):
            error_block = payload.get("error")
            if isinstance(error_block, dict):
                message = str(error_block.get("message", "")).strip()
                if message:
                    return message
        return json.dumps(payload)[:300]

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
        response = self._post_with_retries(
            url=url,
            payload=payload,
            timeout=120.0,
            headers=self._openai_headers(),
            provider_name="OpenAI",
        )
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
        response = self._post_with_retries(
            url=url,
            payload=payload,
            timeout=120.0,
            headers=self._openai_headers(),
            provider_name="OpenAI",
        )
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

    def _complete_with_gemini(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        model: str | None = None,
        max_output_tokens: int | None = None,
    ) -> str:
        self._require_gemini_api_key()
        model_name = self._resolve_gemini_model(model)
        url = f"{self.config.gemini_api_base_url.rstrip('/')}/models/{model_name}:generateContent"
        generation_config: dict[str, object] = {
            "temperature": self.config.temperature,
            "responseMimeType": "text/plain",
        }
        token_limit = self.config.max_output_tokens if max_output_tokens is None else max_output_tokens
        if token_limit is not None:
            generation_config["maxOutputTokens"] = token_limit
        payload = {
            "system_instruction": {
                "parts": [{"text": system_prompt}],
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}],
                }
            ],
            "generationConfig": generation_config,
        }
        response = self._post_with_retries(
            url=url,
            payload=payload,
            timeout=120.0,
            headers=self._gemini_headers(),
            provider_name="Gemini",
        )
        response.raise_for_status()
        body = response.json()
        candidates = body.get("candidates", [])
        chunks: list[str] = []
        for candidate in candidates:
            content = candidate.get("content", {}) if isinstance(candidate, dict) else {}
            parts = content.get("parts", []) if isinstance(content, dict) else []
            for part in parts:
                if isinstance(part, dict) and part.get("text"):
                    chunks.append(str(part.get("text", "")))
        text = "".join(chunks).strip()
        if not text:
            block_reason = str((body.get("promptFeedback") or {}).get("blockReason", "")).strip()
            if block_reason:
                raise RuntimeError(f"Gemini blocked the prompt: {block_reason}")
            raise RuntimeError(f"Unexpected Gemini payload: {json.dumps(body)[:500]}")
        return text

    def _resolve_gemini_model(self, model: str | None) -> str:
        candidate = str(model or "").strip()
        if candidate.startswith("gemini-") or candidate.startswith("models/gemini-"):
            return candidate.removeprefix("models/")
        fallback = str(self.config.gemini_model or "gemini-2.5-flash").strip()
        return fallback.removeprefix("models/")

    def generate_image(
        self,
        *,
        prompt: str,
        model: str,
        quality: str,
        size: str,
        output_format: str,
    ) -> dict:
        self._require_openai_api_key()
        url = f"{self.config.api_base_url.rstrip('/')}/images/generations"
        payload = {
            "model": model,
            "prompt": prompt,
            "quality": quality,
            "size": size,
            "output_format": output_format,
        }
        response = self._post_with_retries(
            url=url,
            payload=payload,
            timeout=180.0,
            headers=self._openai_headers(),
            provider_name="OpenAI",
        )
        response.raise_for_status()
        body = response.json()
        data = body.get("data", [])
        if not data or not isinstance(data[0], dict) or not data[0].get("b64_json"):
            raise RuntimeError(f"Unexpected image response payload: {json.dumps(body)[:500]}")
        return body
