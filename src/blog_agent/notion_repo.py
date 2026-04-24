from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml

from .models import PipelineItem
from .text_files import read_text_file, write_text_file


@dataclass(slots=True)
class NotionState:
    parent_page_id: str = ""
    pillars_db_id: str = ""
    blog_pipeline_db_id: str = ""
    settings_db_id: str = ""
    settings_page_id: str = ""
    pillars_db_url: str = ""
    blog_pipeline_db_url: str = ""
    settings_db_url: str = ""


class NotionRepository:
    def __init__(self, *, state_file: Path) -> None:
        self.base_url = os.getenv("NOTION_API_BASE_URL", "https://api.notion.com/v1").rstrip("/")
        self.token = os.getenv("NOTION_API_TOKEN", "").strip()
        self.notion_version = os.getenv("NOTION_VERSION", "2022-06-28").strip() or "2022-06-28"
        self.state_file = state_file
        self.state = self._load_state()
        self._db_property_cache: dict[str, set[str]] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.token)

    @property
    def configured(self) -> bool:
        return self.enabled and bool(self.state.blog_pipeline_db_id and self.state.pillars_db_id and self.state.settings_db_id)

    def _load_state(self) -> NotionState:
        state = NotionState(
            parent_page_id=_extract_notion_id(os.getenv("NOTION_PARENT_PAGE_ID", "").strip()),
            pillars_db_id=_extract_notion_id(os.getenv("NOTION_PILLARS_DB_ID", "").strip()),
            blog_pipeline_db_id=_extract_notion_id(os.getenv("NOTION_BLOG_PIPELINE_DB_ID", "").strip()),
            settings_db_id=_extract_notion_id(os.getenv("NOTION_SETTINGS_DB_ID", "").strip()),
            settings_page_id=_extract_notion_id(os.getenv("NOTION_SETTINGS_PAGE_ID", "").strip()),
        )
        if self.state_file.exists():
            raw = yaml.safe_load(read_text_file(self.state_file)) or {}
            state.parent_page_id = _extract_notion_id(str(raw.get("parent_page_id", state.parent_page_id)).strip())
            state.pillars_db_id = _extract_notion_id(str(raw.get("pillars_db_id", state.pillars_db_id)).strip())
            state.blog_pipeline_db_id = _extract_notion_id(
                str(raw.get("blog_pipeline_db_id", state.blog_pipeline_db_id)).strip()
            )
            state.settings_db_id = _extract_notion_id(str(raw.get("settings_db_id", state.settings_db_id)).strip())
            state.settings_page_id = _extract_notion_id(str(raw.get("settings_page_id", state.settings_page_id)).strip())
            state.pillars_db_url = str(raw.get("pillars_db_url", "")).strip()
            state.blog_pipeline_db_url = str(raw.get("blog_pipeline_db_url", "")).strip()
            state.settings_db_url = str(raw.get("settings_db_url", "")).strip()
        if not state.pillars_db_id and state.pillars_db_url:
            state.pillars_db_id = _extract_notion_id(state.pillars_db_url)
        if not state.blog_pipeline_db_id and state.blog_pipeline_db_url:
            state.blog_pipeline_db_id = _extract_notion_id(state.blog_pipeline_db_url)
        if not state.settings_db_id and state.settings_db_url:
            state.settings_db_id = _extract_notion_id(state.settings_db_url)
        return state

    def _save_state(self) -> None:
        payload = {
            "parent_page_id": _extract_notion_id(self.state.parent_page_id),
            "pillars_db_id": _extract_notion_id(self.state.pillars_db_id),
            "blog_pipeline_db_id": _extract_notion_id(self.state.blog_pipeline_db_id),
            "settings_db_id": _extract_notion_id(self.state.settings_db_id),
            "settings_page_id": _extract_notion_id(self.state.settings_page_id),
            "pillars_db_url": self.state.pillars_db_url,
            "blog_pipeline_db_url": self.state.blog_pipeline_db_url,
            "settings_db_url": self.state.settings_db_url,
        }
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        write_text_file(self.state_file, yaml.safe_dump(payload, sort_keys=False))

    def diagnostics(self) -> dict[str, Any]:
        missing: list[str] = []
        if not self.enabled:
            missing.append("NOTION_API_TOKEN")
        if not self.state.parent_page_id:
            missing.append("NOTION_PARENT_PAGE_ID")
        if not self.state.pillars_db_id:
            missing.append("NOTION_PILLARS_DB_ID")
        if not self.state.blog_pipeline_db_id:
            missing.append("NOTION_BLOG_PIPELINE_DB_ID")
        if not self.state.settings_db_id:
            missing.append("NOTION_SETTINGS_DB_ID")
        return {
            "enabled": self.enabled,
            "configured": self.configured,
            "missing": missing,
            "stateFile": str(self.state_file),
            "ids": {
                "parentPageId": self.state.parent_page_id,
                "pillarsDbId": self.state.pillars_db_id,
                "blogPipelineDbId": self.state.blog_pipeline_db_id,
                "settingsDbId": self.state.settings_db_id,
                "settingsPageId": self.state.settings_page_id,
            },
            "links": {
                "pillars": self.state.pillars_db_url,
                "blogs": self.state.blog_pipeline_db_url,
                "settings": self.state.settings_db_url,
            },
        }

    def setup_databases(
        self,
        *,
        parent_page_id: str,
        pillars_seed: list[dict[str, Any]],
        overwrite_existing: bool = False,
    ) -> dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("NOTION_API_TOKEN is missing.")
        parent = str(parent_page_id or self.state.parent_page_id).strip()
        if not parent:
            raise RuntimeError("Provide a Notion parent page id for setup.")

        if self.configured and not overwrite_existing:
            return self._state_payload(status="already_configured")

        self.state.parent_page_id = parent
        pillars_db = self._create_database(
            parent_page_id=parent,
            title="SEO Pillars",
            properties={
                "Name": {"title": {}},
                "Pillar ID": {"rich_text": {}},
                "Priority": {"number": {"format": "number"}},
                "Target Keyword": {"rich_text": {}},
                "Pillar Thesis": {"rich_text": {}},
                "Cluster Topics": {"rich_text": {}},
                "Status": {"select": {"options": [{"name": "active"}, {"name": "paused"}]}},
            },
        )
        self.state.pillars_db_id = pillars_db.get("id", "")
        self.state.pillars_db_url = pillars_db.get("url", "")

        pipeline_db = self._create_database(
            parent_page_id=parent,
            title="Blog Pipeline",
            properties={
                "Name": {"title": {}},
                "Pipeline ID": {"rich_text": {}},
                "Slug": {"rich_text": {}},
                "Query": {"rich_text": {}},
                "Pillar": {
                    "relation": {
                        "database_id": self.state.pillars_db_id,
                        "single_property": {},
                    }
                },
                "Topic Role": {"select": {"options": [{"name": "main"}, {"name": "side"}]}},
                "Hierarchy Role": {
                    "select": {
                        "options": [
                            {"name": "pillar-company"},
                            {"name": "main-ceo"},
                            {"name": "sub-reports-to-main"},
                        ]
                    }
                },
                "Reports To Main ID": {"rich_text": {}},
                "Reports To Main Title": {"rich_text": {}},
                "Reports To Main URL": {"url": {}},
                "Status": {
                    "select": {
                        "options": [
                            {"name": "topic"},
                            {"name": "draft"},
                            {"name": "approved"},
                            {"name": "pushed"},
                            {"name": "rejected"},
                        ]
                    }
                },
                "Scheduled For": {"date": {}},
                "Planned Keywords": {"rich_text": {}},
                "Topic Angle": {"rich_text": {}},
                "Outline": {"rich_text": {}},
                "Internal Links": {"rich_text": {}},
                "Meta Description": {"rich_text": {}},
                "Excerpt": {"rich_text": {}},
                "Body Markdown": {"rich_text": {}},
                "Guideline Score": {"number": {"format": "number"}},
                "Guideline Summary": {"rich_text": {}},
                "Generated Image URL": {"url": {}},
                "Shopify Blog ID": {"rich_text": {}},
                "Ready to Push": {"checkbox": {}},
                "Shopify IDs": {"rich_text": {}},
                "Action Error": {"rich_text": {}},
                "Created At": {"rich_text": {}},
                "Approved At": {"rich_text": {}},
                "Pushed At": {"rich_text": {}},
                "Payload JSON": {"rich_text": {}},
                "Post Frontmatter": {"rich_text": {}},
            },
        )
        self.state.blog_pipeline_db_id = pipeline_db.get("id", "")
        self.state.blog_pipeline_db_url = pipeline_db.get("url", "")

        settings_db = self._create_database(
            parent_page_id=parent,
            title="Automation Settings",
            properties={
                "Name": {"title": {}},
                "Enabled": {"checkbox": {}},
                "Daily Time": {"rich_text": {}},
                "Timezone": {"rich_text": {}},
                "Run Now": {"checkbox": {}},
                "Last Run At": {"rich_text": {}},
                "Next Run At": {"rich_text": {}},
                "Pillars Link": {"url": {}},
                "Blogs Link": {"url": {}},
                "Settings Link": {"url": {}},
            },
        )
        self.state.settings_db_id = settings_db.get("id", "")
        self.state.settings_db_url = settings_db.get("url", "")

        settings_page = self._create_page(
            parent={"database_id": self.state.settings_db_id},
            properties={
                "Name": _title_prop("Default Automation Settings"),
                "Enabled": {"checkbox": True},
                "Daily Time": _rich_text_prop("09:00"),
                "Timezone": _rich_text_prop("Asia/Kolkata"),
                "Run Now": {"checkbox": False},
                "Last Run At": _rich_text_prop(""),
                "Next Run At": _rich_text_prop(""),
                "Pillars Link": {"url": self.state.pillars_db_url or None},
                "Blogs Link": {"url": self.state.blog_pipeline_db_url or None},
                "Settings Link": {"url": self.state.settings_db_url or None},
            },
        )
        self.state.settings_page_id = settings_page.get("id", "")

        self._save_state()
        self.seed_pillars(pillars_seed)
        return self._state_payload(status="created")

    def seed_pillars(self, pillars_seed: list[dict[str, Any]]) -> int:
        if not self.configured:
            return 0
        existing = {pillar["pillarId"]: pillar for pillar in self.load_pillars()}
        created = 0
        for pillar in pillars_seed:
            pillar_id = str(pillar.get("pillarId", "")).strip()
            if not pillar_id or pillar_id in existing:
                continue
            self._create_page(
                parent={"database_id": self.state.pillars_db_id},
                properties={
                    "Name": _title_prop(str(pillar.get("pillarName", pillar_id))),
                    "Pillar ID": _rich_text_prop(pillar_id),
                    "Priority": {"number": float(pillar.get("priority", 999))},
                    "Target Keyword": _rich_text_prop(str(pillar.get("targetKeyword", ""))),
                    "Pillar Thesis": _rich_text_prop(str(pillar.get("pillarThesis", ""))),
                    "Cluster Topics": _rich_text_prop("\n".join(pillar.get("clusterTopics", []))),
                    "Status": {"select": {"name": str(pillar.get("status", "active")) or "active"}},
                },
            )
            created += 1
        return created

    def load_pillars(self) -> list[dict[str, Any]]:
        if not self.configured:
            return []
        pages = self._query_all_pages(self.state.pillars_db_id)
        payload: list[dict[str, Any]] = []
        for page in pages:
            props = page.get("properties", {})
            payload.append(
                {
                    "pageId": page.get("id", ""),
                    "pillarId": _get_rich_text(props.get("Pillar ID")) or _slugify(_get_title(props.get("Name"))),
                    "pillarName": _get_title(props.get("Name")),
                    "priority": int(_get_number(props.get("Priority")) or 999),
                    "targetKeyword": _get_rich_text(props.get("Target Keyword")),
                    "pillarThesis": _get_rich_text(props.get("Pillar Thesis")),
                    "clusterTopics": [line.strip() for line in _get_rich_text(props.get("Cluster Topics")).splitlines() if line.strip()],
                    "status": _get_select(props.get("Status")) or "active",
                    "url": page.get("url", ""),
                }
            )
        payload.sort(key=lambda row: row.get("priority", 999))
        return payload

    def load_pipeline_items(self) -> list[dict[str, Any]]:
        if not self.configured:
            return []
        pages = self._query_all_pages(self.state.blog_pipeline_db_id)
        pillar_lookup = self._pillar_page_id_to_machine_id_map()
        payload: list[dict[str, Any]] = []
        for page in pages:
            props = page.get("properties", {})
            relation_ids = _get_relation_ids(props.get("Pillar"))
            relation_pillar_id = pillar_lookup.get(relation_ids[0], "") if relation_ids else ""
            payload_json = _get_rich_text(props.get("Payload JSON"))
            parsed = _safe_json(payload_json)
            if isinstance(parsed, dict):
                item = parsed
            else:
                item = {
                    "id": _get_rich_text(props.get("Pipeline ID")),
                    "title": _get_title(props.get("Name")),
                    "query": _get_rich_text(props.get("Query")),
                    "cluster": "",
                    "pillar_id": relation_pillar_id,
                    "pillar_name": "",
                    "main_topic": "",
                    "sub_blog_tag": "",
                    "scheduled_for": _get_date(props.get("Scheduled For")),
                    "status": _get_select(props.get("Status")) or "topic",
                    "topic_role": _get_select(props.get("Topic Role")) or "side",
                    "created_at": _get_rich_text(props.get("Created At")),
                    "approved_at": _get_rich_text(props.get("Approved At")),
                    "pushed_at": _get_rich_text(props.get("Pushed At")),
                    "topic_angle": _get_rich_text(props.get("Topic Angle")),
                    "topic_outline": [line for line in _get_rich_text(props.get("Outline")).splitlines() if line.strip()],
                    "topic_internal_links": [line for line in _get_rich_text(props.get("Internal Links")).splitlines() if line.strip()],
                    "planned_keywords": [line.strip() for line in _get_rich_text(props.get("Planned Keywords")).split(",") if line.strip()],
                    "metadata": {
                        "slug": _get_rich_text(props.get("Slug")),
                        "meta_description": _get_rich_text(props.get("Meta Description")),
                    },
                }
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            metadata["notion_page_id"] = str(page.get("id", "")).strip()
            metadata["notion_page_url"] = str(page.get("url", "")).strip()
            item["metadata"] = metadata
            item.setdefault("id", _get_rich_text(props.get("Pipeline ID")))
            if not str(item.get("id", "")).strip():
                item["id"] = str(page.get("id", "")).strip()
            # Prefer current Notion property values over stale payload JSON values.
            prop_title = _get_title(props.get("Name"))
            prop_query = _get_rich_text(props.get("Query"))
            prop_status = _get_select(props.get("Status")) or "topic"
            prop_topic_role = _get_select(props.get("Topic Role")) or "side"
            prop_scheduled_for = _get_date(props.get("Scheduled For"))

            if prop_title:
                item["title"] = prop_title
            else:
                item.setdefault("title", "")
            if prop_query:
                item["query"] = prop_query
            else:
                item.setdefault("query", "")
            item["status"] = prop_status
            item["topic_role"] = prop_topic_role
            if relation_pillar_id:
                item["pillar_id"] = relation_pillar_id
            else:
                item.setdefault("pillar_id", "")
            if prop_scheduled_for:
                item["scheduled_for"] = prop_scheduled_for
            item["description"] = _get_rich_text(props.get("Meta Description"))
            item["excerpt"] = _get_rich_text(props.get("Excerpt"))
            item["post_markdown"] = _get_rich_text(props.get("Body Markdown"))
            item["generatedImageUrl"] = _get_url(props.get("Generated Image URL"))
            item["shopify_blog_id"] = _get_rich_text(props.get("Shopify Blog ID"))
            item["ready_to_push"] = _get_checkbox(props.get("Ready to Push"), default=False)
            item["action_error"] = _get_rich_text(props.get("Action Error"))
            item["hierarchy_role"] = _get_select(props.get("Hierarchy Role")) or ""
            item["reports_to_main_id"] = _get_rich_text(props.get("Reports To Main ID"))
            item["reports_to_main_title"] = _get_rich_text(props.get("Reports To Main Title"))
            item["reports_to_main_url"] = _get_url(props.get("Reports To Main URL"))
            shopify_ids = _safe_json(_get_rich_text(props.get("Shopify IDs")))
            if isinstance(shopify_ids, dict):
                item["shopify_article_id"] = str(shopify_ids.get("article_id") or item.get("shopify_article_id") or "")
                item["shopify_blog_id"] = str(shopify_ids.get("blog_id") or item.get("shopify_blog_id") or "")
                item["shopify_article_handle"] = str(shopify_ids.get("handle") or item.get("shopify_article_handle") or "")
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            metadata["ready_to_push"] = bool(item.get("ready_to_push", False))
            metadata["notion_action_error"] = str(item.get("action_error", "")).strip()
            if item.get("hierarchy_role"):
                metadata["hierarchy_role"] = item.get("hierarchy_role")
            if item.get("reports_to_main_id"):
                metadata["reports_to_main_id"] = item.get("reports_to_main_id")
            if item.get("reports_to_main_title"):
                metadata["reports_to_main_title"] = item.get("reports_to_main_title")
            if item.get("reports_to_main_url"):
                metadata["reports_to_main_url"] = item.get("reports_to_main_url")
            if item.get("shopify_blog_id"):
                metadata["shopify_blog_id"] = item["shopify_blog_id"]
            notion_page_id = str(page.get("id", "")).strip()
            notion_row_id = str(item.get("id", "")).strip()
            if notion_row_id and notion_page_id and notion_row_id != notion_page_id:
                metadata["pipeline_id_original"] = notion_row_id
            item["metadata"] = metadata
            item["notionPageId"] = notion_page_id
            item["notionPageUrl"] = page.get("url", "")
            if notion_page_id:
                # Use Notion page ID as canonical row identifier for API actions.
                item["id"] = notion_page_id
            payload.append(item)

        payload.sort(
            key=lambda row: (
                str(row.get("scheduled_for", "")),
                str(row.get("created_at", "")),
            ),
            reverse=True,
        )
        return payload

    def load_pipeline_models(self) -> list[PipelineItem]:
        rows = self.load_pipeline_items()
        models: list[PipelineItem] = []
        for row in rows:
            cleaned = dict(row)
            cleaned.pop("description", None)
            cleaned.pop("excerpt", None)
            cleaned.pop("post_markdown", None)
            cleaned.pop("generatedImageUrl", None)
            cleaned.pop("notionPageId", None)
            cleaned.pop("notionPageUrl", None)
            if "scheduled_for" not in cleaned or not cleaned["scheduled_for"]:
                cleaned["scheduled_for"] = "1970-01-01"
            if "created_at" not in cleaned or not cleaned["created_at"]:
                cleaned["created_at"] = "1970-01-01T00:00:00"
            if "cluster" not in cleaned or not cleaned["cluster"]:
                cleaned["cluster"] = "unknown"
            if "query" not in cleaned or not cleaned["query"]:
                cleaned["query"] = cleaned.get("title", "untitled")
            if "title" not in cleaned or not cleaned["title"]:
                cleaned["title"] = "Untitled topic"
            if "id" not in cleaned or not str(cleaned["id"]).strip():
                metadata = cleaned.get("metadata") if isinstance(cleaned.get("metadata"), dict) else {}
                notion_page_id = str(metadata.get("notion_page_id", "")).strip()
                cleaned["id"] = notion_page_id or _slugify(cleaned["title"]) or f"topic-{len(models)+1}"
            models.append(PipelineItem.model_validate(cleaned))
        return models

    def upsert_pipeline_item(
        self,
        item: PipelineItem,
        *,
        post_frontmatter: dict[str, Any] | None = None,
        post_markdown: str = "",
    ) -> dict[str, Any]:
        if not self.configured:
            return {}
        page = None
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        notion_page_id = str(metadata.get("notion_page_id", "")).strip()
        if notion_page_id:
            try:
                page = self._get(f"/pages/{notion_page_id}")
            except Exception:  # noqa: BLE001
                page = None
        if not page:
            page = self._find_pipeline_page_by_pipeline_id(item.id)
        pillar_relation = self._relation_for_pillar_id(item.pillar_id)
        guideline_score = item.guideline_report.score if item.guideline_report else None
        guideline_summary = item.guideline_report.summary if item.guideline_report else ""
        payload_json = json.dumps(item.model_dump(mode="json"), ensure_ascii=False)
        shopify_ids = json.dumps(
            {
                "article_id": item.shopify_article_id,
                "blog_id": item.shopify_blog_id,
                "handle": item.shopify_article_handle,
            },
            ensure_ascii=False,
        )

        properties = {
            "Name": _title_prop(item.title),
            "Pipeline ID": _rich_text_prop(item.id),
            "Slug": _rich_text_prop(str(item.metadata.get("slug", ""))),
            "Query": _rich_text_prop(item.query),
            "Pillar": {"relation": pillar_relation},
            "Topic Role": {"select": {"name": item.topic_role}},
            "Status": {"select": {"name": item.status}},
            "Scheduled For": {"date": {"start": item.scheduled_for.isoformat()}},
            "Planned Keywords": _rich_text_prop(", ".join(item.planned_keywords)),
            "Topic Angle": _rich_text_prop(item.topic_angle),
            "Outline": _rich_text_prop("\n".join(item.topic_outline)),
            "Internal Links": _rich_text_prop("\n".join(item.topic_internal_links)),
            "Meta Description": _rich_text_prop(str(item.metadata.get("meta_description", ""))),
            "Excerpt": _rich_text_prop(str((post_frontmatter or {}).get("excerpt", ""))),
            "Body Markdown": _rich_text_prop(post_markdown),
            "Guideline Score": {"number": float(guideline_score) if guideline_score is not None else None},
            "Guideline Summary": _rich_text_prop(guideline_summary),
            "Generated Image URL": {"url": str(item.metadata.get("generated_image_url", "")) or None},
            "Created At": _rich_text_prop(item.created_at),
            "Approved At": _rich_text_prop(item.approved_at or ""),
            "Pushed At": _rich_text_prop(item.pushed_at or ""),
            "Payload JSON": _rich_text_prop(payload_json),
            "Post Frontmatter": _rich_text_prop(json.dumps(post_frontmatter or {}, ensure_ascii=False)),
        }
        pipeline_properties = self._database_property_names(self.state.blog_pipeline_db_id)
        if "Shopify IDs" in pipeline_properties:
            properties["Shopify IDs"] = _rich_text_prop(shopify_ids)
        if "Shopify Blog ID" in pipeline_properties:
            blog_id_value = item.shopify_blog_id or str(item.metadata.get("shopify_blog_id", ""))
            properties["Shopify Blog ID"] = _rich_text_prop(blog_id_value)
        if "Ready to Push" in pipeline_properties:
            properties["Ready to Push"] = {"checkbox": bool(item.metadata.get("ready_to_push", False))}
        if "Action Error" in pipeline_properties:
            properties["Action Error"] = _rich_text_prop(str(item.metadata.get("notion_action_error", "")))
        if "Hierarchy Role" in pipeline_properties:
            hierarchy_role = str(item.metadata.get("hierarchy_role", "")).strip()
            if hierarchy_role:
                properties["Hierarchy Role"] = {"select": {"name": hierarchy_role}}
            else:
                properties["Hierarchy Role"] = {"select": None}
        if "Reports To Main ID" in pipeline_properties:
            properties["Reports To Main ID"] = _rich_text_prop(str(item.metadata.get("reports_to_main_id", "")))
        if "Reports To Main Title" in pipeline_properties:
            properties["Reports To Main Title"] = _rich_text_prop(str(item.metadata.get("reports_to_main_title", "")))
        if "Reports To Main URL" in pipeline_properties:
            reports_to_url = str(item.metadata.get("reports_to_main_url", "")).strip()
            properties["Reports To Main URL"] = {"url": reports_to_url or None}

        if page:
            response = self._patch(f"/pages/{page['id']}", {"properties": properties})
        else:
            response = self._create_page(
                parent={"database_id": self.state.blog_pipeline_db_id},
                properties=properties,
            )
        return {
            "id": response.get("id", ""),
            "url": response.get("url", ""),
        }

    def load_settings(self) -> dict[str, Any]:
        if not self.configured:
            return self._default_settings_payload()
        page = self._resolve_settings_page()
        if not page:
            return self._default_settings_payload()
        props = page.get("properties", {})
        return {
            "enabled": _get_checkbox(props.get("Enabled"), default=True),
            "dailyTime": _get_rich_text(props.get("Daily Time")) or "09:00",
            "timezone": _get_rich_text(props.get("Timezone")) or "Asia/Kolkata",
            "runNow": _get_checkbox(props.get("Run Now"), default=False),
            "lastRunAt": _get_rich_text(props.get("Last Run At")),
            "nextRunAt": _get_rich_text(props.get("Next Run At")),
            "notionLinks": {
                "pillars": _get_url(props.get("Pillars Link")) or self.state.pillars_db_url,
                "blogs": _get_url(props.get("Blogs Link")) or self.state.blog_pipeline_db_url,
                "settings": _get_url(props.get("Settings Link")) or self.state.settings_db_url,
            },
        }

    def update_settings(self, updates: dict[str, Any]) -> dict[str, Any]:
        if not self.configured:
            return self._default_settings_payload()
        page = self._resolve_settings_page()
        if not page:
            raise RuntimeError("Automation settings row is missing.")

        current = self.load_settings()
        merged = {**current, **updates}
        merged_links = {**current.get("notionLinks", {}), **(updates.get("notionLinks") or {})}
        merged["notionLinks"] = merged_links

        props = {
            "Enabled": {"checkbox": bool(merged.get("enabled", True))},
            "Daily Time": _rich_text_prop(str(merged.get("dailyTime", "09:00"))),
            "Timezone": _rich_text_prop(str(merged.get("timezone", "Asia/Kolkata"))),
            "Run Now": {"checkbox": bool(merged.get("runNow", False))},
            "Last Run At": _rich_text_prop(str(merged.get("lastRunAt", ""))),
            "Next Run At": _rich_text_prop(str(merged.get("nextRunAt", ""))),
            "Pillars Link": {"url": str(merged_links.get("pillars", "")) or None},
            "Blogs Link": {"url": str(merged_links.get("blogs", "")) or None},
            "Settings Link": {"url": str(merged_links.get("settings", "")) or None},
        }
        self._patch(f"/pages/{page['id']}", {"properties": props})
        return self.load_settings()

    def _default_settings_payload(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "dailyTime": "09:00",
            "timezone": "Asia/Kolkata",
            "runNow": False,
            "lastRunAt": "",
            "nextRunAt": "",
            "notionLinks": {
                "pillars": self.state.pillars_db_url,
                "blogs": self.state.blog_pipeline_db_url,
                "settings": self.state.settings_db_url,
            },
        }

    def _state_payload(self, *, status: str) -> dict[str, Any]:
        return {
            "status": status,
            "parentPageId": self.state.parent_page_id,
            "databaseIds": {
                "pillars": self.state.pillars_db_id,
                "blogPipeline": self.state.blog_pipeline_db_id,
                "settings": self.state.settings_db_id,
            },
            "settingsPageId": self.state.settings_page_id,
            "links": {
                "pillars": self.state.pillars_db_url,
                "blogPipeline": self.state.blog_pipeline_db_url,
                "settings": self.state.settings_db_url,
            },
        }

    def _resolve_settings_page(self) -> dict[str, Any] | None:
        if self.state.settings_page_id:
            try:
                page = self._get(f"/pages/{self.state.settings_page_id}")
                return page
            except Exception:  # noqa: BLE001
                pass
        pages = self._query_all_pages(self.state.settings_db_id, page_size=1)
        if not pages:
            page = self._create_page(
                parent={"database_id": self.state.settings_db_id},
                properties={
                    "Name": _title_prop("Default Automation Settings"),
                    "Enabled": {"checkbox": True},
                    "Daily Time": _rich_text_prop("09:00"),
                    "Timezone": _rich_text_prop("Asia/Kolkata"),
                    "Run Now": {"checkbox": False},
                    "Last Run At": _rich_text_prop(""),
                    "Next Run At": _rich_text_prop(""),
                    "Pillars Link": {"url": self.state.pillars_db_url or None},
                    "Blogs Link": {"url": self.state.blog_pipeline_db_url or None},
                    "Settings Link": {"url": self.state.settings_db_url or None},
                },
            )
            self.state.settings_page_id = page.get("id", "")
            self._save_state()
            return page
        page = pages[0]
        self.state.settings_page_id = page.get("id", "")
        self._save_state()
        return page

    def _find_pipeline_page_by_pipeline_id(self, pipeline_id: str) -> dict[str, Any] | None:
        query = self._post(
            f"/databases/{self.state.blog_pipeline_db_id}/query",
            {
                "filter": {
                    "property": "Pipeline ID",
                    "rich_text": {"equals": pipeline_id},
                },
                "page_size": 1,
            },
        )
        results = query.get("results", [])
        return results[0] if results else None

    def _relation_for_pillar_id(self, pillar_id: str) -> list[dict[str, str]]:
        if not pillar_id:
            return []
        pages = self._query_all_pages(
            self.state.pillars_db_id,
            filter_payload={
                "property": "Pillar ID",
                "rich_text": {"equals": pillar_id},
            },
        )
        if not pages:
            return []
        return [{"id": pages[0].get("id", "")}]

    def _pillar_page_id_to_machine_id_map(self) -> dict[str, str]:
        pages = self._query_all_pages(self.state.pillars_db_id)
        mapping: dict[str, str] = {}
        for page in pages:
            props = page.get("properties", {})
            mapping[page.get("id", "")] = _get_rich_text(props.get("Pillar ID"))
        return mapping

    def _query_all_pages(
        self,
        database_id: str,
        *,
        filter_payload: dict[str, Any] | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        cursor = None
        while True:
            payload: dict[str, Any] = {"page_size": page_size}
            if cursor:
                payload["start_cursor"] = cursor
            if filter_payload:
                payload["filter"] = filter_payload
            response = self._post(f"/databases/{database_id}/query", payload)
            pages.extend(response.get("results", []))
            if not response.get("has_more"):
                break
            cursor = response.get("next_cursor")
            if not cursor:
                break
        return pages

    def _database_property_names(self, database_id: str) -> set[str]:
        if not database_id:
            return set()
        cached = self._db_property_cache.get(database_id)
        if cached is not None:
            return cached
        database = self._get(f"/databases/{database_id}")
        props = database.get("properties", {})
        names = set(props.keys()) if isinstance(props, dict) else set()
        self._db_property_cache[database_id] = names
        return names

    def _create_database(self, *, parent_page_id: str, title: str, properties: dict[str, Any]) -> dict[str, Any]:
        return self._post(
            "/databases",
            {
                "parent": {"type": "page_id", "page_id": parent_page_id},
                "title": [{"type": "text", "text": {"content": title}}],
                "properties": properties,
            },
        )

    def _create_page(self, *, parent: dict[str, Any], properties: dict[str, Any]) -> dict[str, Any]:
        return self._post(
            "/pages",
            {
                "parent": parent,
                "properties": properties,
            },
        )

    def _get(self, path: str) -> dict[str, Any]:
        return self._request("GET", path)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", path, payload=payload)

    def _patch(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", path, payload=payload)

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("Notion is not enabled.")
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=30.0) as client:
            response = client.request(method, url, headers=headers, json=payload)
        if response.status_code >= 300:
            body = response.text[:500]
            raise RuntimeError(f"Notion API {response.status_code} error: {body}")
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected Notion API response payload.")
        return data


def _title_prop(value: str) -> dict[str, Any]:
    return {
        "title": _as_rich_text_array(value),
    }


def _rich_text_prop(value: str) -> dict[str, Any]:
    return {
        "rich_text": _as_rich_text_array(value),
    }


def _as_rich_text_array(value: str, *, chunk_size: int = 1800) -> list[dict[str, Any]]:
    text = str(value or "")
    if not text:
        return []
    chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
    return [{"type": "text", "text": {"content": chunk}} for chunk in chunks]


def _get_title(prop: dict[str, Any] | None) -> str:
    if not isinstance(prop, dict):
        return ""
    return "".join(item.get("plain_text", "") for item in prop.get("title", []))


def _get_rich_text(prop: dict[str, Any] | None) -> str:
    if not isinstance(prop, dict):
        return ""
    return "".join(item.get("plain_text", "") for item in prop.get("rich_text", []))


def _get_select(prop: dict[str, Any] | None) -> str:
    if not isinstance(prop, dict):
        return ""
    value = prop.get("select")
    if isinstance(value, dict):
        return str(value.get("name", ""))
    return ""


def _get_number(prop: dict[str, Any] | None) -> float | None:
    if not isinstance(prop, dict):
        return None
    value = prop.get("number")
    if value is None:
        return None
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _get_date(prop: dict[str, Any] | None) -> str:
    if not isinstance(prop, dict):
        return ""
    value = prop.get("date")
    if isinstance(value, dict):
        return str(value.get("start", "") or "")
    return ""


def _get_url(prop: dict[str, Any] | None) -> str:
    if not isinstance(prop, dict):
        return ""
    return str(prop.get("url", "") or "")


def _get_checkbox(prop: dict[str, Any] | None, *, default: bool) -> bool:
    if not isinstance(prop, dict):
        return default
    value = prop.get("checkbox")
    if isinstance(value, bool):
        return value
    return default


def _get_relation_ids(prop: dict[str, Any] | None) -> list[str]:
    if not isinstance(prop, dict):
        return []
    relation = prop.get("relation")
    if not isinstance(relation, list):
        return []
    ids: list[str] = []
    for row in relation:
        if isinstance(row, dict) and row.get("id"):
            ids.append(str(row.get("id")))
    return ids


def _safe_json(value: str) -> Any:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def _slugify(value: str) -> str:
    raw = str(value or "").strip().lower()
    out = []
    last_dash = False
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        elif not last_dash:
            out.append("-")
            last_dash = True
    return "".join(out).strip("-")


def _extract_notion_id(value: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""

    hyphenated_match = re.search(
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
        cleaned,
    )
    if hyphenated_match:
        return hyphenated_match.group(1).lower()

    compact_match = re.search(r"([0-9a-fA-F]{32})", cleaned)
    if compact_match:
        raw = compact_match.group(1).lower()
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

    return cleaned
