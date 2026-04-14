from __future__ import annotations

import base64
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from .config import ROOT_DIR, load_dotenv


load_dotenv(ROOT_DIR / ".env")


class ShopifyPublisher:
    def __init__(self) -> None:
        self.domain = os.getenv("MYSHOPIFY_DOMAIN", "").strip()
        self.client_id = os.getenv("SHOPIFY_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("SHOPIFY_CLIENT_SECRET", "").strip()
        self.api_version = os.getenv("SHOPIFY_API_VERSION", "2026-01").strip()
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.domain and self.client_id and self.client_secret)

    def list_blogs(self, limit: int = 25) -> list[dict[str, Any]]:
        query = """
        query ListBlogs($first: Int!) {
          blogs(first: $first) {
            edges {
              node {
                id
                title
                handle
                updatedAt
              }
            }
          }
        }
        """
        data = self._graphql(query, {"first": max(1, min(100, limit))})
        return [edge["node"] for edge in data["blogs"]["edges"]]

    def create_article(
        self,
        *,
        blog_id: str,
        title: str,
        author_name: str,
        body_html: str,
        summary_html: str | None = None,
        tags: list[str] | None = None,
        image_url: str | None = None,
        image_alt_text: str | None = None,
        is_published: bool = False,
        publish_date: str | None = None,
    ) -> dict[str, Any]:
        mutation = """
        mutation CreateArticle($article: ArticleCreateInput!) {
          articleCreate(article: $article) {
            article {
              id
              title
              handle
              publishedAt
              image {
                altText
                url
              }
              blog {
                id
                title
                handle
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        article_input: dict[str, Any] = {
            "blogId": blog_id,
            "title": title,
            "author": {"name": author_name},
            "body": body_html,
            "isPublished": is_published,
        }
        if summary_html:
            article_input["summary"] = summary_html
        if tags:
            article_input["tags"] = tags
        if publish_date:
            article_input["publishDate"] = publish_date
        if image_url:
            image_input: dict[str, Any] = {"url": image_url}
            if image_alt_text:
                image_input["altText"] = image_alt_text
            article_input["image"] = image_input

        data = self._graphql(mutation, {"article": article_input})
        user_errors = data["articleCreate"]["userErrors"]
        if user_errors:
            error_text = ", ".join(
                f"{'.'.join(err.get('field') or []) or 'field'}: {err.get('message', 'unknown error')}"
                for err in user_errors
            )
            raise RuntimeError(f"Shopify articleCreate failed: {error_text}")
        return data["articleCreate"]["article"]

    def attach_article_image(
        self,
        *,
        blog_id: str,
        article_id: str,
        image_path: Path,
        alt_text: str | None = None,
    ) -> dict[str, Any]:
        if not image_path.exists():
            raise RuntimeError(f"Image file does not exist: {image_path}")
        blog_numeric = self._extract_numeric_id(blog_id)
        article_numeric = self._extract_numeric_id(article_id)
        image_payload = {
            "attachment": base64.b64encode(image_path.read_bytes()).decode("ascii"),
        }
        if alt_text:
            image_payload["alt"] = alt_text
        payload = {
            "article": {
                "id": int(article_numeric),
                "image": image_payload,
            }
        }
        response = self._rest_json(
            "PUT",
            f"/blogs/{blog_numeric}/articles/{article_numeric}.json",
            payload,
            timeout_seconds=120.0,
        )
        article = response.get("article")
        if not isinstance(article, dict):
            raise RuntimeError("Invalid Shopify REST response for article image update.")
        image = article.get("image")
        if not isinstance(image, dict):
            raise RuntimeError("Shopify did not return article image details after upload.")
        return image

    def _graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        token = self._get_access_token()
        url = f"https://{self.domain}/admin/api/{self.api_version}/graphql.json"
        response = httpx.post(
            url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=45.0,
        )
        response.raise_for_status()
        payload = response.json()
        errors = payload.get("errors", [])
        if errors:
            messages = ", ".join(error.get("message", "Unknown Shopify error") for error in errors)
            raise RuntimeError(f"Shopify GraphQL error: {messages}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Invalid Shopify response payload: missing data object.")
        return data

    def _rest_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: float = 45.0,
    ) -> dict[str, Any]:
        token = self._get_access_token()
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"https://{self.domain}/admin/api/{self.api_version}{normalized_path}"
        response = httpx.request(
            method=method.upper(),
            url=url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError("Invalid Shopify REST response payload.")
        return body

    @staticmethod
    def _extract_numeric_id(value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise RuntimeError("Missing Shopify ID value.")
        if cleaned.isdigit():
            return cleaned
        if cleaned.startswith("gid://"):
            maybe_numeric = cleaned.rsplit("/", 1)[-1].strip()
            if maybe_numeric.isdigit():
                return maybe_numeric
        raise RuntimeError(f"Unsupported Shopify ID format: {cleaned}")

    def _get_access_token(self) -> str:
        now = datetime.now(UTC)
        if self._access_token and self._token_expires_at and now < self._token_expires_at:
            return self._access_token

        if not self.enabled:
            raise RuntimeError(
                "Shopify is not configured. Set SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, and MYSHOPIFY_DOMAIN."
            )

        token_url = f"https://{self.domain}/admin/oauth/access_token"
        response = httpx.post(
            token_url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=30.0,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Shopify token exchange failed: missing access_token.")
        expires_in = int(payload.get("expires_in", 86400))
        # Refresh 5 minutes early to avoid edge-of-expiry failures.
        self._token_expires_at = now + timedelta(seconds=max(300, expires_in - 300))
        self._access_token = token
        return token
