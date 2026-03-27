from __future__ import annotations

import base64
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .config import AgentConfig, CONTENT_DIR
from .models import PipelineItem
from .storage import load_history, load_pipeline, parse_markdown_file

GENERATED_IMAGE_DIR = CONTENT_DIR.parent / "images"


@dataclass
class SupabaseConfig:
    url: str
    service_role_key: str
    table: str
    chunk_size: int
    logical_namespace: str

    @classmethod
    def from_env(cls) -> "SupabaseConfig":
        url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        table = os.getenv("SUPABASE_BLOG_TABLE", "blog_entries").strip() or "blog_entries"
        chunk_size = max(1, min(25, int(os.getenv("SUPABASE_SYNC_CHUNK_SIZE", "3"))))
        logical_namespace = (
            os.getenv("SUPABASE_LOGICAL_NAMESPACE", "pillar_architecture_blog_entries").strip()
            or "pillar_architecture_blog_entries"
        )
        if not url or not service_role_key:
            raise RuntimeError(
                "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY. "
                "Set both environment variables before syncing."
            )
        return cls(
            url=url,
            service_role_key=service_role_key,
            table=table,
            chunk_size=chunk_size,
            logical_namespace=logical_namespace,
        )


def sync_blog_entries_to_supabase(config: AgentConfig) -> tuple[int, str]:
    supabase = SupabaseConfig.from_env()
    rows = build_rows(config)
    if not rows:
        return 0, f"No pipeline entries found in {config.pipeline_file}."

    endpoint = f"{supabase.url}/rest/v1/{supabase.table}"
    headers = {
        "apikey": supabase.service_role_key,
        "Authorization": f"Bearer {supabase.service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": "pipeline_id"}

    with httpx.Client(timeout=30.0) as client:
        if supabase.table == "table_name":
            delete_response = client.delete(
                endpoint,
                headers=headers,
                params={"name": f"eq.{supabase.logical_namespace}"},
            )
            if delete_response.status_code >= 300:
                raise RuntimeError(
                    "Supabase delete-before-sync failed "
                    f"({delete_response.status_code}): {delete_response.text[:500]}"
                )
            wrapped_rows = [{"name": supabase.logical_namespace, "data": row} for row in rows]
            for offset in range(0, len(wrapped_rows), supabase.chunk_size):
                chunk = wrapped_rows[offset : offset + supabase.chunk_size]
                response = client.post(endpoint, headers=headers, json=chunk)
                if response.status_code >= 300:
                    raise RuntimeError(
                        "Supabase insert failed "
                        f"({response.status_code}): {response.text[:500]}"
                    )
            return len(rows), f"{supabase.table}:{supabase.logical_namespace}"

        for offset in range(0, len(rows), supabase.chunk_size):
            chunk = rows[offset : offset + supabase.chunk_size]
            response = client.post(endpoint, headers=headers, params=params, json=chunk)
            if response.status_code >= 300:
                raise RuntimeError(
                    "Supabase upsert failed "
                    f"({response.status_code}): {response.text[:500]}"
                )
    return len(rows), supabase.table


def build_rows(config: AgentConfig) -> list[dict[str, Any]]:
    pipeline = load_pipeline(config.pipeline_file)
    history = load_history(config.history_file)
    history_lookup = {Path(item.output_path).name: item for item in history}
    rows: list[dict[str, Any]] = []

    for item in pipeline:
        post_frontmatter, post_markdown = load_post_content(item)
        history_item = history_lookup.get(item.post_id or "")
        image_name, image_mime, image_b64 = load_image_content(item)
        row: dict[str, Any] = {
            "pipeline_id": item.id,
            "post_id": item.post_id,
            "title": item.title,
            "query": item.query,
            "cluster": item.cluster,
            "pillar_id": item.pillar_id,
            "pillar_name": item.pillar_name,
            "pillar_claim": item.pillar_claim,
            "main_topic": item.main_topic,
            "sub_blog_tag": item.sub_blog_tag,
            "is_pillar_head": item.is_pillar_head,
            "pillar_head_post_id": item.pillar_head_post_id,
            "pillar_head_slug": item.pillar_head_slug,
            "planned_keywords": item.planned_keywords,
            "path": item.path,
            "scheduled_for": item.scheduled_for.isoformat(),
            "status": item.status,
            "topic_role": item.topic_role,
            "created_at": item.created_at or None,
            "approved_at": item.approved_at,
            "pushed_at": item.pushed_at,
            "shopify_article_id": item.shopify_article_id,
            "shopify_blog_id": item.shopify_blog_id,
            "shopify_article_handle": item.shopify_article_handle,
            "topic_angle": item.topic_angle,
            "topic_outline": item.topic_outline,
            "topic_internal_links": item.topic_internal_links,
            "guideline_report": item.guideline_report.model_dump(mode="json")
            if item.guideline_report
            else None,
            "pipeline_metadata": item.metadata or {},
            "post_frontmatter": post_frontmatter or None,
            "post_markdown": post_markdown,
            "history_title": history_item.title if history_item else None,
            "history_slug": history_item.slug if history_item else None,
            "history_query": history_item.query if history_item else None,
            "history_cluster": history_item.cluster if history_item else None,
            "history_created_on": history_item.created_on.isoformat() if history_item else None,
            "generated_image_file": image_name,
            "generated_image_mime_type": image_mime,
            "generated_image_base64": image_b64,
        }
        rows.append(row)

    return rows


def load_post_content(item: PipelineItem) -> tuple[dict[str, Any], str]:
    if not item.post_id:
        return {}, ""
    path = CONTENT_DIR / item.post_id
    if not path.exists():
        return {}, ""
    frontmatter, body = parse_markdown_file(path)
    return frontmatter, body


def load_image_content(item: PipelineItem) -> tuple[str, str, str]:
    image_name = str(item.metadata.get("generated_image_file", "")).strip()
    if not image_name:
        return "", "", ""
    image_path = GENERATED_IMAGE_DIR / image_name
    if not image_path.exists():
        return image_name, "", ""
    mime = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
    image_b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return image_name, mime, image_b64
