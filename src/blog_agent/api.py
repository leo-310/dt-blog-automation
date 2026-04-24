from __future__ import annotations

import base64
import json
import os
import re
import mimetypes
import socketserver
import threading
import time
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from wsgiref.simple_server import WSGIServer, make_server

import httpx
import markdown

from .agent import BlogAgent
from .automation import (
    AutomationSettings,
    build_post_run_updates,
    evaluate_automation_schedule,
)
from .config import AgentConfig, CONTENT_DIR, ROOT_DIR
from .keyword_research import KeywordResearchRequest, KeywordResearchService
from .models import BlogPlan, KeywordCluster, PipelineItem
from .notion_repo import NotionRepository
from .provider import BlogAgentProvider
from .shopify import ShopifyPublisher
from .storage import (
    ensure_directories,
    load_automation_settings,
    load_history,
    load_keyword_clusters,
    load_pipeline,
    parse_markdown_file,
    save_automation_settings,
    save_pipeline,
)
from .text_files import read_text_file, write_text_file
from .visibility import load_latest_visibility_report

GENERATED_IMAGE_DIR = CONTENT_DIR.parent / "images"
DIST_DIR = ROOT_DIR / "dist"
NOTION_STATE_FILE = ROOT_DIR / "data" / "notion_state.yaml"
LOCAL_AUTOMATION_SETTINGS_FILE = ROOT_DIR / "data" / "automation_settings.yaml"


class ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True


def main() -> None:
    host = os.getenv("BLOG_AGENT_API_HOST", "0.0.0.0")
    port = int(os.getenv("BLOG_AGENT_API_PORT", os.getenv("PORT", "8124")))
    app = BlogAgentApi()
    with make_server(host, port, app.wsgi_app, server_class=ThreadingWSGIServer) as server:
        print(f"Blog agent API running at http://{host}:{port}")
        server.serve_forever()


class BlogAgentApi:
    def __init__(self) -> None:
        self.config = AgentConfig()
        self.agent = BlogAgent(self.config)
        self.keyword_research = KeywordResearchService()
        self.shopify = ShopifyPublisher()
        self.default_shopify_blog_id = os.getenv("SHOPIFY_DEFAULT_BLOG_ID", "").strip()
        self.notion = NotionRepository(state_file=NOTION_STATE_FILE)
        self.use_notion = env_flag(
            "BLOG_AGENT_USE_NOTION",
            default=self.notion.enabled,
        )
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        self.supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        self.supabase_table = os.getenv("SUPABASE_BLOG_TABLE", "table_name").strip() or "table_name"
        self.supabase_namespace = (
            os.getenv("SUPABASE_LOGICAL_NAMESPACE", "pillar_architecture_blog_entries").strip()
            or "pillar_architecture_blog_entries"
        )
        self.use_supabase_namespace = env_flag(
            "BLOG_AGENT_USE_SUPABASE_NAMESPACE",
            default=bool(self.supabase_url and self.supabase_service_role_key),
        )
        self.local_settings_file = LOCAL_AUTOMATION_SETTINGS_FILE
        self._automation_lock = threading.RLock()
        self._background_loop_enabled = env_flag("BLOG_AGENT_BACKGROUND_LOOP", default=True)
        self._background_loop_interval_seconds = max(
            15,
            int(os.getenv("BLOG_AGENT_BACKGROUND_LOOP_INTERVAL_SECONDS", "60") or "60"),
        )
        requested_role = str(os.getenv("BLOG_AGENT_AUTOMATION_TOPIC_ROLE", "side") or "side").strip().lower()
        self._automation_topic_role = "main" if requested_role == "main" else "side"
        self._automation_topic_count = max(1, int(os.getenv("BLOG_AGENT_AUTOMATION_TOPIC_COUNT", "1") or "1"))
        self._automation_generate_image = env_flag("BLOG_AGENT_AUTOMATION_GENERATE_IMAGE", default=True)
        self._automation_push = env_flag("BLOG_AGENT_AUTOMATION_PUSH", default=True)
        ensure_directories([CONTENT_DIR, GENERATED_IMAGE_DIR])
        if self._background_loop_enabled:
            threading.Thread(
                target=self._background_loop,
                name="blog-agent-background-loop",
                daemon=True,
            ).start()

    def wsgi_app(self, environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        parsed = urlparse(environ.get("PATH_INFO", "/"))

        if method == "OPTIONS":
            start_response(
                "204 No Content",
                cors_headers([("Content-Length", "0")]),
            )
            return [b""]

        try:
            if method == "GET" and parsed.path == "/api/health":
                return self.respond(start_response, {"ok": True})
            if method == "GET" and parsed.path == "/api/settings":
                return self.respond(start_response, {"settings": self.load_settings()})
            if method == "GET" and parsed.path == "/api/posts":
                return self.respond(start_response, {"posts": self.load_posts()})
            if method == "GET" and parsed.path == "/api/pipeline":
                return self.respond(
                    start_response,
                    {"pipeline": self.load_pipeline_items()},
                )
            if method == "GET" and parsed.path == "/api/pillars":
                return self.respond(start_response, {"pillars": self.load_pillars()})
            if method == "GET" and parsed.path == "/api/notion/state":
                notion_diag = self.notion.diagnostics()
                return self.respond(
                    start_response,
                    {
                        "enabled": self.use_notion and self.notion.enabled,
                        "configured": self.notion.configured if self.use_notion else False,
                        "state": self.notion._state_payload(status="current"),
                        "diagnostics": notion_diag,
                    },
                )
            if method == "GET" and parsed.path.startswith("/api/images/"):
                image_name = parsed.path.removeprefix("/api/images/")
                return self.serve_generated_image(start_response, image_name)
            if method == "GET" and parsed.path == "/api/shopify/blogs":
                if not self.shopify.enabled:
                    return self.respond(
                        start_response,
                        {
                            "error": "Shopify credentials are missing. Set SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, and MYSHOPIFY_DOMAIN.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                blogs = self.shopify.list_blogs(limit=50)
                return self.respond(start_response, {"blogs": blogs})
            if method == "GET" and parsed.path == "/api/keyword-research":
                query_params = parse_qs(environ.get("QUERY_STRING", ""))
                seeds = normalize_seed_keywords(query_params.get("seed", []))
                if not seeds:
                    return self.respond(
                        start_response,
                        {"error": "Pass at least one `seed` query parameter."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                research = self.keyword_research.research(
                    KeywordResearchRequest(
                        seed_keywords=seeds,
                        geo=query_params.get("geo", ["US"])[0],
                        timeframe=query_params.get("timeframe", ["today 3-m"])[0],
                    )
                )
                return self.respond(
                    start_response,
                    {"research": research.model_dump(mode="json")},
                )
            if method == "GET" and parsed.path == "/api/insights/hot-feed":
                query_params = parse_qs(environ.get("QUERY_STRING", ""))
                topic_targets = normalize_seed_keywords(query_params.get("topic", []))
                feed = self.load_hot_feed(topic_targets=topic_targets)
                return self.respond(start_response, {"feed": feed})
            if method == "POST" and parsed.path == "/api/generate":
                generated = self.agent.generate_post()
                created = self.load_post_by_name(Path(generated.output_path).name)
                self.upsert_pipeline_item(generated)
                return self.respond(
                    start_response,
                    {"post": created, "message": "Generated a new post."},
                    status=HTTPStatus.CREATED,
                )
            if method == "PUT" and parsed.path == "/api/settings":
                payload = self.read_json_body(environ)
                updated = self.update_settings(payload)
                return self.respond(start_response, {"settings": updated})
            if method == "POST" and parsed.path == "/api/notion/setup":
                payload = self.read_json_body(environ)
                parent_page_id = str(payload.get("parentPageId", "")).strip()
                overwrite_existing = bool(payload.get("overwriteExisting", False))
                result = self.setup_notion(parent_page_id=parent_page_id, overwrite_existing=overwrite_existing)
                return self.respond(start_response, result, status=HTTPStatus.CREATED)
            if method == "POST" and parsed.path == "/api/notion/migrate":
                result = self.migrate_local_content_to_notion()
                return self.respond(start_response, result)
            if method == "POST" and parsed.path == "/api/notion/sync":
                result = self.sync_pipeline_to_notion()
                return self.respond(start_response, result)
            if method == "POST" and parsed.path == "/api/notion/actions/run":
                result = self.run_notion_actions()
                return self.respond(start_response, result)
            if method == "POST" and parsed.path == "/api/automation/run-now":
                result = self.run_automation_now()
                return self.respond(start_response, result, status=HTTPStatus.CREATED)
            if method == "POST" and parsed.path == "/api/automation/tick":
                result = self.automation_tick()
                return self.respond(start_response, result)
            if method == "POST" and parsed.path == "/api/pipeline/generate":
                payload = self.read_json_body(environ)
                count = int(payload.get("count", payload.get("weeks", 4)))
                count = max(1, min(20, count))
                pillar_id = str(payload.get("pillarId", "")).strip() or None
                topic_role = str(payload.get("role", "side")).strip().lower()
                if topic_role not in {"main", "side"}:
                    topic_role = "side"
                required_keywords = normalize_requested_keywords(payload.get("keywords", []))
                created = self.generate_pipeline(
                    count=count,
                    pillar_id=pillar_id,
                    topic_role=topic_role,
                    required_keywords=required_keywords,
                )
                return self.respond(
                    start_response,
                    {
                        "created": created,
                        "pipeline": self.load_pipeline_items(),
                        "message": (
                            f"Generated {len(created)} {topic_role} topic(s). "
                            + (
                                f"Keyword focus applied: {', '.join(required_keywords)}."
                                if required_keywords
                                else "Auto-selected SEO keywords were applied."
                            )
                        ),
                    },
                    status=HTTPStatus.CREATED,
                )
            if method == "POST" and parsed.path == "/api/images/generate":
                payload = self.read_json_body(environ)
                prompt = str(payload.get("prompt", "")).strip()
                pipeline_id = str(payload.get("pipelineId", "")).strip() or None
                result = self.generate_pipeline_image(prompt=prompt, pipeline_id=pipeline_id)
                return self.respond(start_response, result, status=HTTPStatus.CREATED)
            if method == "POST" and parsed.path.startswith("/api/pipeline/"):
                parts = [part for part in parsed.path.split("/") if part]
                if len(parts) != 4:
                    return self.respond(
                        start_response,
                        {"error": "Invalid pipeline action path."},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                _, _, pipeline_id, action = parts
                payload = self.read_json_body(environ)
                transition = self.transition_pipeline_item(pipeline_id, action, payload)
                return self.respond(start_response, transition)
            if method == "GET" and not parsed.path.startswith("/api/"):
                return self.serve_web_app(start_response, parsed.path)
        except Exception as exc:  # noqa: BLE001
            return self.respond(
                start_response,
                {"error": str(exc)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return self.respond(
            start_response,
            {"error": "Not found"},
            status=HTTPStatus.NOT_FOUND,
        )

    def load_posts(self) -> list[dict]:
        pipeline = self._load_pipeline_models()
        status_lookup = {item.post_id: item for item in pipeline if item.post_id}
        history = load_history(self.config.history_file)
        history_lookup = {Path(item.output_path).name: item for item in history}
        posts: list[dict] = []

        for path in sorted(CONTENT_DIR.glob("*.md"), reverse=True):
            frontmatter, body = parse_markdown_file(path)
            history_item = history_lookup.get(path.name)
            posts.append(
                {
                    "id": path.name,
                    "title": str(frontmatter.get("title", path.stem)),
                    "description": str(frontmatter.get("description", "")),
                    "excerpt": str(frontmatter.get("excerpt", "")),
                    "date": str(frontmatter.get("date", "")),
                    "query": history_item.query if history_item else "Not logged",
                    "cluster": history_item.cluster if history_item else "Unknown",
                    "pillarName": status_lookup.get(path.name).pillar_name
                    if status_lookup.get(path.name)
                    else "",
                    "mainTopic": status_lookup.get(path.name).main_topic
                    if status_lookup.get(path.name)
                    else "",
                    "subBlogTag": status_lookup.get(path.name).sub_blog_tag
                    if status_lookup.get(path.name)
                    else "",
                    "path": str(path),
                    "pipelineStatus": status_lookup.get(path.name).status
                    if status_lookup.get(path.name)
                    else "draft",
                    "guidelineReport": status_lookup.get(path.name).guideline_report.model_dump(mode="json")
                    if status_lookup.get(path.name) and status_lookup.get(path.name).guideline_report
                    else None,
                    "html": render_post_html(body, self.config.website_url),
                }
            )

        if self.use_notion and self.notion.configured:
            existing_ids = {post["id"] for post in posts}
            for item in self.notion.load_pipeline_items():
                post_markdown = str(item.get("post_markdown", "")).strip()
                post_id = str(item.get("post_id", "")).strip()
                if not post_markdown or not post_id or post_id in existing_ids:
                    continue
                posts.append(
                    {
                        "id": post_id,
                        "title": str(item.get("title", post_id)),
                        "description": str(item.get("description", "")),
                        "excerpt": str(item.get("excerpt", "")),
                        "date": str(item.get("scheduled_for", "")),
                        "query": str(item.get("query", "")),
                        "cluster": str(item.get("cluster", "")),
                        "pillarName": str(item.get("pillar_name", "")),
                        "mainTopic": str(item.get("main_topic", "")),
                        "subBlogTag": str(item.get("sub_blog_tag", "")),
                        "path": str(item.get("path") or ""),
                        "pipelineStatus": str(item.get("status", "draft")),
                        "guidelineReport": item.get("guideline_report"),
                        "html": render_post_html(post_markdown, self.config.website_url),
                    }
                )
                existing_ids.add(post_id)

        return posts

    def load_settings(self) -> dict:
        if self.use_notion and self.notion.configured:
            return self.notion.load_settings()
        defaults = {
            "enabled": True,
            "dailyTime": "09:00",
            "timezone": "Asia/Kolkata",
            "runNow": False,
            "lastRunAt": "",
            "nextRunAt": "",
            "notionLinks": {
                "pillars": self.notion.state.pillars_db_url,
                "blogs": self.notion.state.blog_pipeline_db_url,
                "settings": self.notion.state.settings_db_url,
            },
        }
        local = load_automation_settings(self.local_settings_file)
        notion_links = local.get("notionLinks") if isinstance(local.get("notionLinks"), dict) else {}
        merged_links = {**defaults["notionLinks"], **{k: str(v or "") for k, v in notion_links.items()}}
        return {
            "enabled": bool(local.get("enabled", defaults["enabled"])),
            "dailyTime": str(local.get("dailyTime", defaults["dailyTime"])).strip() or defaults["dailyTime"],
            "timezone": str(local.get("timezone", defaults["timezone"])).strip() or defaults["timezone"],
            "runNow": bool(local.get("runNow", defaults["runNow"])),
            "lastRunAt": str(local.get("lastRunAt", defaults["lastRunAt"])).strip(),
            "nextRunAt": str(local.get("nextRunAt", defaults["nextRunAt"])).strip(),
            "notionLinks": merged_links,
        }

    def update_settings(self, payload: dict) -> dict:
        updates = payload.get("settings", payload)
        clean = {
            "enabled": bool(updates.get("enabled", True)),
            "dailyTime": str(updates.get("dailyTime", "09:00")).strip() or "09:00",
            "timezone": str(updates.get("timezone", "Asia/Kolkata")).strip() or "Asia/Kolkata",
            "runNow": bool(updates.get("runNow", False)),
            "lastRunAt": str(updates.get("lastRunAt", "")).strip(),
            "nextRunAt": str(updates.get("nextRunAt", "")).strip(),
        }
        notion_links = updates.get("notionLinks") or {}
        if isinstance(notion_links, dict):
            clean["notionLinks"] = {
                "pillars": str(notion_links.get("pillars", "")).strip(),
                "blogs": str(notion_links.get("blogs", "")).strip(),
                "settings": str(notion_links.get("settings", "")).strip(),
            }

        if self.use_notion and self.notion.configured:
            return self.notion.update_settings(clean)
        current = self.load_settings()
        merged_links = {
            **(current.get("notionLinks") or {}),
            **(clean.get("notionLinks") or {}),
        }
        merged = {**current, **clean, "notionLinks": merged_links}
        save_automation_settings(self.local_settings_file, merged)
        return merged

    def setup_notion(self, *, parent_page_id: str, overwrite_existing: bool) -> dict:
        if not self.use_notion:
            raise RuntimeError("Enable Notion integration by setting BLOG_AGENT_USE_NOTION=1.")
        pillars_seed = build_seo_pillars_seed(load_keyword_clusters(self.config.topic_file))
        result = self.notion.setup_databases(
            parent_page_id=parent_page_id,
            pillars_seed=pillars_seed,
            overwrite_existing=overwrite_existing,
        )
        settings = self.notion.update_settings(
            {
                "notionLinks": {
                    "pillars": self.notion.state.pillars_db_url,
                    "blogs": self.notion.state.blog_pipeline_db_url,
                    "settings": self.notion.state.settings_db_url,
                }
            }
        )
        return {
            "notion": result,
            "settings": settings,
        }

    def migrate_local_content_to_notion(self) -> dict:
        if not (self.use_notion and self.notion.configured):
            raise RuntimeError("Notion is not configured. Run /api/notion/setup first.")
        migrated = 0
        pipeline = load_pipeline(self.config.pipeline_file)
        for item in pipeline:
            frontmatter: dict = {}
            body_markdown = ""
            if item.path:
                path = Path(item.path)
                if path.exists():
                    frontmatter, body_markdown = parse_markdown_file(path)
            self.notion.upsert_pipeline_item(
                item,
                post_frontmatter=frontmatter,
                post_markdown=body_markdown,
            )
            migrated += 1
        return {
            "migrated": migrated,
            "source": str(self.config.pipeline_file),
            "targetDatabaseId": self.notion.state.blog_pipeline_db_id,
        }

    def sync_pipeline_to_notion(self) -> dict:
        if not (self.use_notion and self.notion.configured):
            raise RuntimeError("Notion is not configured. Run /api/notion/setup first.")
        synced = 0
        for item in self._load_pipeline_models():
            frontmatter: dict = {}
            body_markdown = ""
            if item.path:
                path = Path(item.path)
                if path.exists():
                    frontmatter, body_markdown = parse_markdown_file(path)
            self.notion.upsert_pipeline_item(
                item,
                post_frontmatter=frontmatter,
                post_markdown=body_markdown,
            )
            synced += 1
        return {"synced": synced}

    def run_automation_now(self) -> dict:
        settings = self.load_settings()
        settings["runNow"] = True
        self.update_settings(settings)
        return self.automation_tick(force=True)

    def automation_tick(self, *, force: bool = False) -> dict:
        with self._automation_lock:
            notion_actions = self._run_notion_actions_impl()
            settings_payload = self.load_settings()
            schedule = AutomationSettings(
                enabled=bool(settings_payload.get("enabled", True)),
                daily_time=str(settings_payload.get("dailyTime", "09:00")),
                timezone=str(settings_payload.get("timezone", "Asia/Kolkata")),
                run_now=bool(settings_payload.get("runNow", False)),
                last_run_at=str(settings_payload.get("lastRunAt", "")),
                next_run_at=str(settings_payload.get("nextRunAt", "")),
            )
            decision = evaluate_automation_schedule(schedule, now_utc=datetime.now(UTC))
            if force:
                decision.should_run = True
                decision.reason = "forced"
            if not decision.should_run:
                settings_payload["nextRunAt"] = decision.next_run_at
                updated = self.update_settings(settings_payload)
                return {
                    "executed": False,
                    "reason": decision.reason,
                    "settings": updated,
                    "notionActions": notion_actions,
                }

            workflow_result = self._run_scheduled_full_workflow()
            updates = build_post_run_updates(schedule, now_utc=datetime.now(UTC))
            next_settings = {**settings_payload, **updates}
            next_settings["nextRunAt"] = decision.next_run_at
            updated = self.update_settings(next_settings)
            return {
                "executed": True,
                "reason": decision.reason,
                "workflow": workflow_result,
                "settings": updated,
                "notionActions": notion_actions,
            }

    def _run_scheduled_full_workflow(self) -> dict:
        created = self.generate_pipeline(
            count=self._automation_topic_count,
            topic_role=self._automation_topic_role,
        )
        if not created:
            raise RuntimeError("Scheduled automation did not create a pipeline topic.")

        processed_items: list[dict] = []
        for created_row in created:
            generated_id = str(created_row.get("id", "")).strip()
            generated_title = str(created_row.get("title", "")).strip()
            generated_query = str(created_row.get("query", "")).strip()
            generated_created_at = str(created_row.get("created_at", "")).strip()

            item = self._resolve_pipeline_item_for_automation(
                created_id=generated_id,
                title=generated_title,
                query=generated_query,
                created_at=generated_created_at,
            )
            if not item:
                raise RuntimeError("Unable to resolve scheduled pipeline topic after generation.")

            self.transition_pipeline_item(item.id, "approve", {"pillarId": item.pillar_id})
            item = self._resolve_pipeline_item_for_automation(
                created_id=generated_id,
                title=generated_title,
                query=generated_query,
                created_at=generated_created_at,
            ) or item

            image_result = None
            if self._automation_generate_image:
                image_result = self.generate_pipeline_image(prompt="", pipeline_id=item.id)
                item = self._resolve_pipeline_item_for_automation(
                    created_id=generated_id,
                    title=generated_title,
                    query=generated_query,
                    created_at=generated_created_at,
                ) or item

            push_result = None
            if self._automation_push:
                push_payload: dict[str, str] = {}
                blog_id = resolve_shopify_blog_id(
                    item=item,
                    shopify=self.shopify,
                    default_shopify_blog_id=self.default_shopify_blog_id,
                )
                if blog_id:
                    push_payload["blogId"] = blog_id
                push_result = self.transition_pipeline_item(item.id, "push", push_payload)
                item = self._resolve_pipeline_item_for_automation(
                    created_id=generated_id,
                    title=generated_title,
                    query=generated_query,
                    created_at=generated_created_at,
                ) or item

            processed_items.append(
                {
                    "generatedTopicId": item.id,
                    "title": item.title,
                    "query": item.query,
                    "status": item.status,
                    "postId": item.post_id,
                    "path": item.path,
                    "generatedImageUrl": build_generated_image_url(item),
                    "imageResult": image_result,
                    "pushResult": push_result,
                }
            )

        return {
            "topicRole": self._automation_topic_role,
            "requestedCount": self._automation_topic_count,
            "processedCount": len(processed_items),
            "items": processed_items,
        }

    def _resolve_pipeline_item_for_automation(
        self,
        *,
        created_id: str,
        title: str,
        query: str,
        created_at: str,
    ) -> PipelineItem | None:
        pipeline = self._load_pipeline_models()
        if not pipeline:
            return None

        if created_id:
            exact = next((item for item in pipeline if item.id == created_id), None)
            if exact:
                return exact
            by_original_id = next(
                (
                    item
                    for item in pipeline
                    if str((item.metadata or {}).get("pipeline_id_original", "")).strip() == created_id
                ),
                None,
            )
            if by_original_id:
                return by_original_id

        def matches(item: PipelineItem) -> bool:
            title_ok = not title or item.title == title
            query_ok = not query or item.query == query
            created_ok = not created_at or item.created_at == created_at
            return title_ok and query_ok and created_ok

        candidates = [item for item in pipeline if matches(item)]
        if candidates:
            return sorted(candidates, key=lambda row: -created_at_sort_key(row.created_at))[0]

        fallback = [item for item in pipeline if (not title or item.title == title) and (not query or item.query == query)]
        if fallback:
            return sorted(fallback, key=lambda row: -created_at_sort_key(row.created_at))[0]

        return None

    def run_notion_actions(self) -> dict:
        with self._automation_lock:
            return self._run_notion_actions_impl()

    def _run_notion_actions_impl(self) -> dict:
        if not (self.use_notion and self.notion.configured):
            return {"executed": False, "reason": "notion_not_configured", "processed": 0, "approved": 0, "pushed": 0, "errors": 0}

        processed = 0
        attempted = 0
        generated = 0
        approved = 0
        pushed = 0
        shopify_path_hydrated = 0
        shopify_backlinks_repaired = 0
        errors = 0
        error_details: list[dict[str, str]] = []

        pipeline = self._load_pipeline_models()
        shopify_path_hydrated = self._hydrate_shopify_paths_for_pipeline(pipeline)
        shopify_backlinks_repaired = self._repair_shopify_backlinks(pipeline)
        hierarchy_repaired = 0
        repaired_ids: set[str] = set()
        for current_pillar_id in sorted({str(item.pillar_id or "").strip() for item in pipeline if str(item.pillar_id or "").strip()}):
            for changed_item in synchronize_pillar_hierarchy(pipeline=pipeline, pillar_id=current_pillar_id):
                if changed_item.id in repaired_ids:
                    continue
                self._persist_pipeline_item(changed_item, pipeline=pipeline)
                repaired_ids.add(changed_item.id)
                hierarchy_repaired += 1
        actionable = [item for item in pipeline if is_action_candidate(item)]
        ordered = sorted(
            actionable,
            key=lambda item: (
                action_priority(item),
                -created_at_sort_key(item.created_at),
            ),
        )
        max_actions_per_run = int(os.getenv("BLOG_AGENT_NOTION_MAX_ACTIONS_PER_RUN", "20") or "20")

        for item in ordered:
            try:
                metadata = item.metadata if isinstance(item.metadata, dict) else {}
                requested_push = is_push_requested(item)
                has_shopify_article = bool(item.shopify_article_id)

                if should_auto_generate_from_notion_row(item):
                    attempted += 1
                    if not str(item.id or "").strip():
                        notion_page_id = str(item.metadata.get("notion_page_id", "")).strip()
                        item.id = notion_page_id or f"topic-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    queued = bool(item.metadata.get("queued_generation", False))
                    if not queued:
                        item.title = "Generating blog..."
                        if not str(item.query or "").strip():
                            item.query = f"{item.pillar_name or item.pillar_id} {item.topic_role} blog".strip()
                        item.metadata["notion_action_error"] = ""
                        item.metadata["queued_generation"] = True
                        self._persist_pipeline_item(item)
                        queued = True
                    blocked_queries = [entry.query for entry in pipeline if entry.id != item.id]
                    plan, cluster = self.agent.plan_topic(
                        blocked_queries=blocked_queries,
                        preferred_pillar_id=item.pillar_id or None,
                    )
                    item.title = plan.title
                    item.query = plan.target_query
                    item.cluster = cluster.name
                    item.pillar_id = cluster.pillar_id
                    item.pillar_name = cluster.pillar_name
                    item.main_topic = cluster.main_topic
                    item.sub_blog_tag = cluster.sub_blog_tag
                    item.topic_angle = plan.angle
                    item.topic_outline = plan.outline
                    item.topic_internal_links = plan.internal_links
                    item.planned_keywords = plan.keywords_to_use or cluster.supporting_keywords[:8]
                    item.metadata["slug"] = plan.slug
                    item.metadata["meta_description"] = plan.meta_description
                    item.metadata["queued_generation"] = False
                    synchronize_pillar_hierarchy(pipeline=pipeline, pillar_id=item.pillar_id)
                    self._persist_pipeline_item(item)
                    processed += 1
                    generated += 1
                    if processed >= max_actions_per_run:
                        break

                needs_approve = item.status in {"approved", "pushed"} and not item.post_id

                if needs_approve:
                    attempted += 1
                    self.transition_pipeline_item(item.id, "approve", {"pillarId": item.pillar_id})
                    processed += 1
                    approved += 1
                    item = next((entry for entry in self._load_pipeline_models() if entry.id == item.id), item)
                    if processed >= max_actions_per_run:
                        break

                if requested_push and not has_shopify_article:
                    attempted += 1
                    payload = {}
                    blog_id = resolve_shopify_blog_id(
                        item=item,
                        shopify=self.shopify,
                        default_shopify_blog_id=self.default_shopify_blog_id,
                    )
                    if blog_id:
                        payload["blogId"] = blog_id
                    self.transition_pipeline_item(item.id, "push", payload)
                    processed += 1
                    pushed += 1
                    item = next((entry for entry in self._load_pipeline_models() if entry.id == item.id), item)
                    if processed >= max_actions_per_run:
                        break

                if item.metadata.get("notion_action_error"):
                    item.metadata["notion_action_error"] = ""
                    if item.status == "pushed":
                        item.metadata["ready_to_push"] = False
                    self._persist_pipeline_item(item)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if len(error_details) < 12:
                    error_details.append(
                        {
                            "id": item.id,
                            "status": item.status,
                            "message": str(exc),
                        }
                    )
                item.metadata["notion_action_error"] = str(exc)
                if bool(item.metadata.get("queued_generation", False)):
                    item.title = "Generation failed"
                    item.metadata["queued_generation"] = False
                self._persist_pipeline_item(item)

        return {
            "executed": True,
            "processed": processed,
            "attempted": attempted,
            "generated": generated,
            "approved": approved,
            "pushed": pushed,
            "shopifyPathHydrated": shopify_path_hydrated,
            "shopifyBacklinksRepaired": shopify_backlinks_repaired,
            "hierarchyRepaired": hierarchy_repaired,
            "errors": errors,
            "errorDetails": error_details,
        }

    def _hydrate_shopify_paths_for_pipeline(self, pipeline: list[PipelineItem]) -> int:
        if not self.shopify.enabled:
            return 0
        hydrated = 0
        for item in pipeline:
            if item.status != "pushed" or not str(item.shopify_article_id or "").strip():
                continue
            metadata = item.metadata if isinstance(item.metadata, dict) else {}
            existing_url = str(metadata.get("shopify_article_url", "")).strip()
            has_canonical_url = existing_url.startswith("/blogs/") and existing_url.count("/") >= 3
            has_blog_handle = bool(str(metadata.get("shopify_blog_handle", "")).strip())
            has_article_handle = bool(str(item.shopify_article_handle or "").strip())
            if has_canonical_url and has_blog_handle and has_article_handle:
                continue
            try:
                article = self.shopify.get_article(article_id=str(item.shopify_article_id))
            except Exception:  # noqa: BLE001
                continue
            blog_payload = article.get("blog") if isinstance(article.get("blog"), dict) else {}
            blog_handle = str(blog_payload.get("handle", "")).strip()
            article_handle = str(article.get("handle", "")).strip()
            blog_id = str(blog_payload.get("id", "")).strip()

            changed = False
            if article_handle and article_handle != str(item.shopify_article_handle or "").strip():
                item.shopify_article_handle = article_handle
                changed = True
            if blog_id and blog_id != str(item.shopify_blog_id or "").strip():
                item.shopify_blog_id = blog_id
                changed = True
            if blog_handle and blog_handle != str(metadata.get("shopify_blog_handle", "")).strip():
                metadata["shopify_blog_handle"] = blog_handle
                changed = True
            if blog_handle and article_handle:
                canonical_url = f"/blogs/{sanitize_slug(blog_handle)}/{sanitize_slug(article_handle)}"
                if canonical_url != existing_url:
                    metadata["shopify_article_url"] = canonical_url
                    changed = True
            if changed:
                item.metadata = metadata
                self._persist_pipeline_item(item, pipeline=pipeline)
                hydrated += 1
        return hydrated

    def _repair_shopify_backlinks(self, pipeline: list[PipelineItem]) -> int:
        if not self.shopify.enabled:
            return 0
        blog_slug_lookup = build_pushed_blog_slug_lookup(pipeline)
        repaired = 0
        for item in pipeline:
            if item.status != "pushed" or not str(item.shopify_article_id or "").strip():
                continue
            try:
                article = self.shopify.get_article(article_id=str(item.shopify_article_id))
            except Exception:  # noqa: BLE001
                continue
            body_html = str(article.get("body") or "")
            if not body_html:
                continue

            fallback_url = resolve_connected_main_blog_url(item, pipeline) or build_main_blog_url(item)
            updated_html, changed = rewrite_short_blog_hrefs_in_html(
                body_html,
                blog_slug_lookup=blog_slug_lookup,
                fallback_url=fallback_url,
            )
            if not changed:
                continue
            try:
                self.shopify.update_article_body(
                    article_id=str(item.shopify_article_id),
                    body_html=updated_html,
                )
            except Exception:  # noqa: BLE001
                continue
            repaired += 1
        return repaired

    def _background_loop(self) -> None:
        while True:
            try:
                self.automation_tick(force=False)
            except Exception as exc:  # noqa: BLE001
                print(f"[background-loop] {exc}")
            time.sleep(self._background_loop_interval_seconds)

    def load_post_by_name(self, name: str) -> dict | None:
        for post in self.load_posts():
            if post["id"] == name:
                return post
        return None

    def load_pipeline_items(self) -> list[dict]:
        posts = self.load_posts()
        payload: list[dict] = []

        if self.use_notion and self.notion.configured:
            notion_items = self.notion.load_pipeline_items()
            for item in notion_items:
                body_markdown = str(item.get("post_markdown", "")).strip()
                if body_markdown and not item.get("html"):
                    item["html"] = render_post_html(body_markdown, self.config.website_url)
                item["generatedImageStyle"] = str(item.get("metadata", {}).get("generated_image_style", ""))
                item["hasGeneratedDraft"] = bool(item.get("post_id") or body_markdown)
                item.setdefault("excerpt", "")
                item.setdefault("description", "")
                hydrate_hierarchy_fields(item)
            payload = notion_items

        if not payload and self.use_supabase_namespace:
            supabase_items = self.load_pipeline_items_from_supabase()
            if supabase_items:
                payload = supabase_items

        if not payload:
            posts_lookup = {post["id"]: post for post in posts}
            pipeline_items = load_pipeline(self.config.pipeline_file)
            clusters = load_keyword_clusters(self.config.topic_file)
            if backfill_pipeline_pillar_context(pipeline_items, clusters):
                save_pipeline(self.config.pipeline_file, pipeline_items)
            items = sorted(
                pipeline_items,
                key=lambda item: (item.scheduled_for.isoformat(), item.created_at),
                reverse=True,
            )
            for item in items:
                post = posts_lookup.get(item.post_id)
                payload.append(
                    {
                        **item.model_dump(mode="json"),
                        "html": post["html"] if post else "",
                        "excerpt": post["excerpt"] if post else "",
                        "description": post["description"] if post else "",
                        "generatedImageUrl": build_generated_image_url(item),
                        "generatedImageStyle": str(item.metadata.get("generated_image_style", "")),
                        "hasGeneratedDraft": bool(item.post_id),
                    }
                )
            for row in payload:
                hydrate_hierarchy_fields(row)

        payload.extend(build_orphan_pipeline_rows_from_posts(posts=posts, existing_items=payload))
        payload.sort(key=pipeline_payload_sort_key, reverse=True)
        return payload

    def load_pipeline_items_from_supabase(self) -> list[dict]:
        rows = self.load_supabase_namespace_rows()
        if not rows:
            return []

        payload: list[dict] = []
        for row in rows:
            metadata = row.get("pipeline_metadata") or {}
            import_source = str(metadata.get("import_source", "")).strip()
            if import_source in {"pillar-definitions", "local-images"}:
                continue
            if str(row.get("sub_blog_tag", "")).strip() == "pillar-definition":
                continue

            image_base64 = str(row.get("generated_image_base64", "")).strip()
            image_mime = str(row.get("generated_image_mime_type", "")).strip() or "image/png"
            image_file = str(row.get("generated_image_file", "")).strip()
            image_url = ""
            if image_base64:
                image_url = f"data:{image_mime};base64,{image_base64}"
            elif image_file:
                image_url = f"/api/images/{image_file}"

            frontmatter = row.get("post_frontmatter") or {}
            post_markdown = str(row.get("post_markdown", "") or "")

            payload.append(
                {
                    "id": row.get("pipeline_id", ""),
                    "post_id": row.get("post_id"),
                    "title": row.get("title", ""),
                    "query": row.get("query", ""),
                    "cluster": row.get("cluster", ""),
                    "pillar_id": row.get("pillar_id", ""),
                    "pillar_name": row.get("pillar_name", ""),
                    "pillar_claim": row.get("pillar_claim", ""),
                    "main_topic": row.get("main_topic", ""),
                    "sub_blog_tag": row.get("sub_blog_tag", ""),
                    "is_pillar_head": bool(row.get("is_pillar_head", False)),
                    "pillar_head_post_id": row.get("pillar_head_post_id"),
                    "pillar_head_slug": row.get("pillar_head_slug"),
                    "planned_keywords": row.get("planned_keywords") or [],
                    "path": row.get("path"),
                    "scheduled_for": row.get("scheduled_for"),
                    "status": row.get("status", "topic"),
                    "topic_role": row.get("topic_role", "side"),
                    "created_at": row.get("created_at"),
                    "approved_at": row.get("approved_at"),
                    "pushed_at": row.get("pushed_at"),
                    "shopify_article_id": row.get("shopify_article_id"),
                    "shopify_blog_id": row.get("shopify_blog_id"),
                    "shopify_article_handle": row.get("shopify_article_handle"),
                    "topic_angle": row.get("topic_angle", ""),
                    "topic_outline": row.get("topic_outline") or [],
                    "topic_internal_links": row.get("topic_internal_links") or [],
                    "guideline_report": row.get("guideline_report"),
                    "metadata": metadata,
                    "html": render_post_html(post_markdown, self.config.website_url) if post_markdown else "",
                    "excerpt": str(frontmatter.get("excerpt", "")),
                    "description": str(frontmatter.get("description", "")),
                    "generatedImageUrl": image_url,
                    "generatedImageStyle": str(metadata.get("generated_image_style", "")),
                    "hasGeneratedDraft": bool(post_markdown or row.get("post_id")),
                }
            )
        for row in payload:
            hydrate_hierarchy_fields(row)
        payload.sort(
            key=lambda item: (
                str(item.get("scheduled_for", "")),
                str(item.get("created_at", "")),
            ),
            reverse=True,
        )
        return payload

    def generate_pipeline_image(self, *, prompt: str, pipeline_id: str | None) -> dict:
        pipeline = self._load_pipeline_models()
        item = next((entry for entry in pipeline if entry.id == pipeline_id), None) if pipeline_id else None
        if item and item.status not in {"approved", "pushed"}:
            raise RuntimeError("Approve the blog first, then generate or regenerate its cover image.")
        image_prompt = prompt or build_image_prompt(item=item, provider=self.agent.provider, config=self.config)
        if not image_prompt:
            raise RuntimeError("Provide `prompt` or `pipelineId` to generate an image.")

        image_response = self.agent.provider.generate_image(
            prompt=image_prompt,
            model=self.config.image_model,
            quality=self.config.image_quality,
            size=self.config.image_size,
            output_format=self.config.image_format,
        )
        image_payload = image_response.get("data", [{}])[0]
        image_base64 = str(image_payload.get("b64_json", "")).strip()
        if not image_base64:
            raise RuntimeError("Image generation succeeded but no image payload was returned.")
        image_bytes = base64.b64decode(image_base64, validate=True)

        image_name = build_generated_image_name(
            prompt=image_prompt,
            output_format=self.config.image_format,
        )
        image_file = GENERATED_IMAGE_DIR / image_name
        image_file.write_bytes(image_bytes)

        if item:
            item.metadata["generated_image_file"] = image_name
            item.metadata["generated_image_prompt"] = image_prompt
            item.metadata["generated_image_style"] = "Custom Prompt" if prompt else "Topic-Aware Abstract"
            self._persist_pipeline_item(item)

        return {
            "imageUrl": f"/api/images/{image_name}",
            "prompt": image_prompt,
            "model": self.config.image_model,
            "quality": self.config.image_quality,
            "size": self.config.image_size,
            "pipelineId": pipeline_id,
            "styleName": "Custom Prompt" if prompt else "Topic-Aware Abstract",
        }

    def load_pillars(self) -> list[dict]:
        if self.use_notion and self.notion.configured:
            pillars = self.notion.load_pillars()
            return [
                {
                    "pillarId": row.get("pillarId", ""),
                    "pillarName": row.get("pillarName", ""),
                    "pillarClaim": row.get("pillarThesis", ""),
                    "mainTopic": row.get("pillarName", ""),
                    "targetKeyword": row.get("targetKeyword", ""),
                    "priority": row.get("priority", 999),
                    "status": row.get("status", "active"),
                    "clusters": [
                        {"name": topic}
                        for topic in row.get("clusterTopics", [])
                    ],
                }
                for row in pillars
            ]

        if self.use_supabase_namespace:
            supabase_pillars = self.load_pillars_from_supabase()
            if supabase_pillars:
                return supabase_pillars

        clusters = load_keyword_clusters(self.config.topic_file)
        grouped: dict[str, dict] = {}
        for cluster in clusters:
            pillar_id = cluster.pillar_id or sanitize_slug(cluster.pillar_name or cluster.name)
            if pillar_id not in grouped:
                grouped[pillar_id] = {
                    "pillarId": pillar_id,
                    "pillarName": cluster.pillar_name or cluster.name,
                    "pillarClaim": cluster.pillar_claim,
                    "mainTopic": cluster.main_topic,
                    "clusters": [],
                }
            grouped[pillar_id]["clusters"].append(cluster.model_dump(mode="json"))
        return list(grouped.values())

    def load_pillars_from_supabase(self) -> list[dict]:
        rows = self.load_supabase_namespace_rows()
        grouped: dict[str, dict] = {}
        for row in rows:
            pillar_id = str(row.get("pillar_id", "")).strip()
            if not pillar_id:
                continue
            if pillar_id not in grouped:
                grouped[pillar_id] = {
                    "pillarId": pillar_id,
                    "pillarName": row.get("pillar_name", ""),
                    "pillarClaim": row.get("pillar_claim", ""),
                    "mainTopic": row.get("main_topic", ""),
                    "clusters": [],
                }
        return list(grouped.values())

    def load_supabase_namespace_rows(self) -> list[dict]:
        if not self.supabase_url or not self.supabase_service_role_key:
            return []
        endpoint = f"{self.supabase_url}/rest/v1/{self.supabase_table}"
        headers = {
            "apikey": self.supabase_service_role_key,
            "Authorization": f"Bearer {self.supabase_service_role_key}",
        }
        params = {"select": "data"} if self.supabase_table == "table_name" else {"select": "*"}
        if self.supabase_table == "table_name":
            params["name"] = f"eq.{self.supabase_namespace}"
        try:
            with httpx.Client(timeout=20.0) as client:
                response = client.get(endpoint, headers=headers, params=params)
            if response.status_code >= 300:
                return []
            payload = response.json()
            if not isinstance(payload, list):
                return []
            if self.supabase_table == "table_name":
                return [row.get("data", {}) for row in payload if isinstance(row, dict)]
            return [row for row in payload if isinstance(row, dict)]
        except Exception:  # noqa: BLE001
            return []

    def load_hot_feed(self, topic_targets: list[str] | None = None) -> dict:
        pipeline = self.load_pipeline_items()
        posts = self.load_posts()
        topic_targets = topic_targets or []

        if not topic_targets:
            topic_targets = []
            for item in pipeline:
                query = str(item.get("query", "")).strip()
                if query and query.lower() not in {value.lower() for value in topic_targets}:
                    topic_targets.append(query)
            topic_targets = topic_targets[:8]

        if not topic_targets:
            topic_targets = [
                "acne-safe towels",
                "face towel for sensitive skin",
                "how often should you change towels",
                "best towel material for acne-prone skin",
            ]

        derived_seeds = topic_targets[:5]
        trend_cards: list[dict] = []
        notes: list[str] = []

        try:
            research = self.keyword_research.research(
                KeywordResearchRequest(seed_keywords=derived_seeds)
            )
            for index, idea in enumerate(research.ideas[:12]):
                trend_cards.append(
                    {
                        "id": f"google-{index}",
                        "channel": "search" if idea.kind in {"top", "rising"} else "insight",
                        "rank": index + 1,
                        "keyword": idea.keyword,
                        "delta": format_trend_delta(idea),
                        "kind": idea.kind,
                        "source": idea.source,
                    }
                )
            notes.extend(research.notes)
        except Exception as exc:  # noqa: BLE001
            notes.append(f"Google Trends request failed: {exc}")

        market_cards = build_market_cards(trend_cards, derived_seeds)
        latest_visibility = load_latest_visibility_report()
        if latest_visibility:
            chatgpt_visibility = build_chatgpt_visibility_from_report(
                report=latest_visibility, pipeline=pipeline
            )
            visibility_score = int(
                latest_visibility.get("aggregateVisibilityScore", 0)
            )
            visibility_stats = {
                "mentions": int(latest_visibility.get("mentions", 0)),
                "citedPages": int(latest_visibility.get("citedPages", 0)),
                "providerMentions": latest_visibility.get("providerMentions", {}),
            }
        else:
            chatgpt_visibility = build_chatgpt_visibility_cards(topic_targets, posts, pipeline)
            visibility_score = score_chatgpt_visibility(chatgpt_visibility)
            visibility_stats = {}

        return {
            "updatedAt": datetime.now().isoformat(timespec="seconds"),
            "sections": [
                {
                    "id": "google-keywords",
                    "title": "Google Keywords",
                    "description": "Rising and top terms from Google Trends relevance pools.",
                    "cards": trend_cards,
                },
                {
                    "id": "hot-topics",
                    "title": "Hot Topics",
                    "description": "Breakout angles blended from trend movement and editorial focus.",
                    "cards": market_cards,
                },
                {
                    "id": "chatgpt-visibility",
                    "title": "ChatGPT Visibility",
                    "description": "Do we currently rank for our fixed discovery prompts across AI search responses?",
                    "score": visibility_score,
                    "cards": chatgpt_visibility,
                    "stats": visibility_stats,
                },
            ],
            "notes": notes[:4],
        }

    def generate_pipeline(
        self,
        *,
        count: int,
        pillar_id: str | None = None,
        topic_role: str = "side",
        required_keywords: list[str] | None = None,
    ) -> list[dict]:
        pipeline = self._load_pipeline_models()
        clusters = load_keyword_clusters(self.config.topic_file)
        rotating_pillar_ids = self._active_pillar_ids_for_generation(clusters=clusters)
        rotation_cursor = 0
        last_generated_pillar_id = self._latest_pipeline_pillar_id(pipeline)
        start = date.today()
        created: list[dict] = []
        enforced_keywords = normalize_requested_keywords(required_keywords or [])
        blocked_queries = [item.query for item in pipeline]
        for offset in range(count):
            target_date = start + timedelta(days=offset)
            preferred_pillar_id = pillar_id
            if not preferred_pillar_id and rotating_pillar_ids:
                preferred_pillar_id, rotation_cursor = pick_next_pillar_for_rotation(
                    rotating_pillar_ids,
                    last_pillar_id=last_generated_pillar_id,
                    cursor=rotation_cursor,
                )
            plan, cluster = self.agent.plan_topic(
                blocked_queries=blocked_queries,
                preferred_pillar_id=preferred_pillar_id,
            )
            if (
                not pillar_id
                and len(rotating_pillar_ids) > 1
                and last_generated_pillar_id
                and cluster.pillar_id == last_generated_pillar_id
            ):
                for candidate_pillar_id in rotating_pillar_ids:
                    if candidate_pillar_id in {last_generated_pillar_id, preferred_pillar_id}:
                        continue
                    alt_plan, alt_cluster = self.agent.plan_topic(
                        blocked_queries=blocked_queries,
                        preferred_pillar_id=candidate_pillar_id,
                    )
                    plan, cluster = alt_plan, alt_cluster
                    break
            plan.keywords_to_use = merge_keyword_targets(
                preferred=enforced_keywords,
                planned=plan.keywords_to_use or [],
                fallback=cluster.supporting_keywords[:12],
            )
            blocked_queries.append(plan.target_query)
            topic_id = f"topic-{target_date.isoformat()}-{offset}-{datetime.now().strftime('%H%M%S')}"
            item = PipelineItem(
                id=topic_id,
                title=plan.title,
                query=plan.target_query,
                cluster=cluster.name,
                pillar_id=cluster.pillar_id,
                pillar_name=cluster.pillar_name,
                main_topic=cluster.main_topic,
                sub_blog_tag=cluster.sub_blog_tag,
                pillar_head_post_id=None,
                pillar_head_slug=None,
                planned_keywords=plan.keywords_to_use,
                scheduled_for=target_date,
                status="topic",
                topic_role="main" if topic_role == "main" else "side",
                created_at=datetime.now().isoformat(timespec="seconds"),
                topic_angle=plan.angle,
                topic_outline=plan.outline,
                topic_internal_links=plan.internal_links,
                metadata={
                    "slug": plan.slug,
                    "meta_description": plan.meta_description,
                    "required_keywords": enforced_keywords,
                },
            )
            enrich_item_pillar_context(item=item, pillar_id=pillar_id or "", clusters=clusters)
            pipeline.append(item)
            self._persist_pipeline_item(item, pipeline=pipeline)
            created.append(item.model_dump(mode="json"))
            if item.pillar_id:
                last_generated_pillar_id = item.pillar_id
        if not (self.use_notion and self.notion.configured):
            save_pipeline(self.config.pipeline_file, pipeline)
        return created

    def _latest_pipeline_pillar_id(self, pipeline: list[PipelineItem]) -> str:
        sorted_items = sorted(
            pipeline,
            key=lambda row: created_at_sort_key(row.created_at),
            reverse=True,
        )
        for item in sorted_items:
            pillar_id = str(item.pillar_id or "").strip()
            if pillar_id:
                return pillar_id
        return ""

    def _active_pillar_ids_for_generation(self, *, clusters: list[KeywordCluster]) -> list[str]:
        candidate_ids: list[str] = []
        if self.use_notion and self.notion.configured:
            notion_pillars = self.notion.load_pillars()
            for pillar in notion_pillars:
                status = str(pillar.get("status", "active")).strip().lower()
                pillar_id = str(pillar.get("pillarId", "")).strip()
                if not pillar_id or status == "paused":
                    continue
                if pillar_id not in candidate_ids:
                    candidate_ids.append(pillar_id)
        if candidate_ids:
            return candidate_ids

        for cluster in clusters:
            pillar_id = str(getattr(cluster, "pillar_id", "") or "").strip()
            if pillar_id and pillar_id not in candidate_ids:
                candidate_ids.append(pillar_id)
        return candidate_ids

    def upsert_pipeline_item(self, generated) -> PipelineItem:
        pipeline = self._load_pipeline_models()
        post_id = Path(generated.output_path).name
        existing = next((item for item in pipeline if item.post_id == post_id), None)
        now = datetime.now().isoformat(timespec="seconds")
        if existing:
            existing.title = generated.title
            existing.query = generated.query
            existing.cluster = generated.cluster
            existing.pillar_id = generated.pillar_id
            existing.pillar_name = generated.pillar_name
            existing.main_topic = generated.main_topic
            existing.sub_blog_tag = generated.sub_blog_tag
            existing.path = generated.output_path
            existing.scheduled_for = generated.date
            existing.guideline_report = generated.guideline_report
            existing.status = "draft"
            existing.approved_at = None
            existing.pushed_at = None
        else:
            pipeline.append(
                PipelineItem(
                    id=f"{generated.date.isoformat()}-{generated.slug}",
                    post_id=post_id,
                    title=generated.title,
                    query=generated.query,
                    cluster=generated.cluster,
                    pillar_id=generated.pillar_id,
                    pillar_name=generated.pillar_name,
                    main_topic=generated.main_topic,
                    sub_blog_tag=generated.sub_blog_tag,
                    path=generated.output_path,
                    scheduled_for=generated.date,
                    status="draft",
                    created_at=now,
                    guideline_report=generated.guideline_report,
                )
            )
        target = next(item for item in pipeline if item.post_id == post_id)
        synchronize_pillar_hierarchy(pipeline=pipeline, pillar_id=target.pillar_id)
        if self.use_notion and self.notion.configured:
            frontmatter, body_markdown = parse_markdown_file(Path(target.path)) if target.path and Path(target.path).exists() else ({}, "")
            self.notion.upsert_pipeline_item(
                target,
                post_frontmatter=frontmatter,
                post_markdown=body_markdown,
            )
        else:
            save_pipeline(self.config.pipeline_file, pipeline)
        return target

    def transition_pipeline_item(self, pipeline_id: str, action: str, payload: dict | None = None) -> dict:
        pipeline = self._load_pipeline_models()
        item = next((entry for entry in pipeline if entry.id == pipeline_id), None)
        if not item:
            raise RuntimeError("Pipeline item not found.")
        payload = payload or {}
        if action in {"approve", "push"}:
            enrich_item_pillar_context(
                item=item,
                pillar_id=str(payload.get("pillarId", "")).strip(),
                clusters=load_keyword_clusters(self.config.topic_file),
            )
        now = datetime.now().isoformat(timespec="seconds")
        if action == "approve":
            if not item.post_id:
                ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
                plan = BlogPlan(
                    title=item.title,
                    slug=str(item.metadata.get("slug", item.title.lower().replace(" ", "-"))),
                    target_query=item.query,
                    meta_description=str(item.metadata.get("meta_description", ""))[:160],
                    angle=item.topic_angle or "Educational explainer",
                    outline=item.topic_outline or ["Introduction", "Key points", "Conclusion"],
                    internal_links=item.topic_internal_links,
                    keywords_to_use=item.planned_keywords,
                )
                generated = self.agent.generate_post_from_plan(
                    plan=plan,
                    cluster=resolve_cluster_from_item(item),
                    today=item.scheduled_for,
                )
                item.post_id = Path(generated.output_path).name
                item.path = generated.output_path
                item.guideline_report = generated.guideline_report
                item.status = "approved"
                item.approved_at = now
                if item.is_pillar_head:
                    item.pillar_head_post_id = item.post_id
                ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
            else:
                item.status = "approved"
                item.approved_at = now
                ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
                ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
        elif action == "reject":
            item.status = "rejected"
            item.approved_at = None
            item.pushed_at = None
            item.shopify_article_id = None
            item.shopify_blog_id = None
            item.shopify_article_handle = None
        elif action == "push":
            if item.status not in {"approved", "pushed"}:
                raise RuntimeError("Only approved drafts can be pushed.")
            if item.shopify_article_id and item.status == "pushed":
                return {"item": item.model_dump(mode="json"), "message": "Already pushed."}
            if not item.post_id:
                raise RuntimeError("Approve a topic first to generate its full blog content.")
            blog_id = str(payload.get("blogId", "")).strip()
            if not blog_id:
                blog_id = resolve_shopify_blog_id(
                    item=item,
                    shopify=self.shopify,
                    default_shopify_blog_id=self.default_shopify_blog_id,
                )
            if not blog_id:
                raise RuntimeError("Missing blogId in request payload for push action.")
            if not self.shopify.enabled:
                raise RuntimeError(
                    "Shopify is not configured. Set SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET, and MYSHOPIFY_DOMAIN."
                )
            ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
            ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline)
            post = self.load_post_by_name(item.post_id)
            if not post:
                raise RuntimeError("Cannot push: post content file is missing.")
            publish_now = bool(payload.get("publishNow", True))
            publish_date = str(payload.get("publishDate", "")).strip() or None
            article = self.shopify.create_article(
                blog_id=blog_id,
                title=post["title"],
                author_name=self.config.author_name,
                body_html=post["html"],
                summary_html=post.get("excerpt") or post.get("description") or None,
                tags=build_shopify_tags(item),
                is_published=publish_now,
                publish_date=publish_date,
            )
            generated_image_file = str(item.metadata.get("generated_image_file", "")).strip()
            if generated_image_file:
                local_cover_path = GENERATED_IMAGE_DIR / generated_image_file
                if local_cover_path.exists():
                    uploaded_image = self.shopify.attach_article_image(
                        blog_id=blog_id,
                        article_id=str(article.get("id", "")).strip(),
                        image_path=local_cover_path,
                        alt_text=post["title"],
                    )
                    item.metadata["shopify_cover_image_url"] = str(uploaded_image.get("src", "")).strip()
                    item.metadata["shopify_cover_upload_error"] = ""
                else:
                    item.metadata["shopify_cover_upload_error"] = (
                        f"Generated image file missing at push time: {local_cover_path}"
                    )
            item.status = "pushed"
            item.pushed_at = now
            item.shopify_article_id = article.get("id")
            article_blog = article.get("blog") if isinstance(article.get("blog"), dict) else {}
            article_blog_id = str(article_blog.get("id", "")).strip()
            article_blog_handle = str(article_blog.get("handle", "")).strip()
            article_handle = str(article.get("handle", "")).strip()

            item.shopify_blog_id = article_blog_id or blog_id
            item.shopify_article_handle = article_handle or item.shopify_article_handle
            if article_blog_handle:
                item.metadata["shopify_blog_handle"] = article_blog_handle
            if article_blog_handle and article_handle:
                item.metadata["shopify_article_url"] = f"/blogs/{sanitize_slug(article_blog_handle)}/{sanitize_slug(article_handle)}"
            item.metadata["ready_to_push"] = False
            item.metadata["notion_action_error"] = ""
            changed_items = synchronize_pillar_hierarchy(pipeline=pipeline, pillar_id=item.pillar_id)
            for changed in changed_items:
                if changed.id != item.id:
                    self._persist_pipeline_item(changed, pipeline=pipeline)
            self._persist_pipeline_item(item, pipeline=pipeline)
            return {
                "item": item.model_dump(mode="json"),
                "shopifyArticle": article,
            }
        else:
            raise RuntimeError("Unsupported pipeline action.")
        changed_items = synchronize_pillar_hierarchy(pipeline=pipeline, pillar_id=item.pillar_id)
        for changed in changed_items:
            if changed.id != item.id:
                self._persist_pipeline_item(changed, pipeline=pipeline)
        self._persist_pipeline_item(item, pipeline=pipeline)
        return {"item": item.model_dump(mode="json")}

    def _load_pipeline_models(self) -> list[PipelineItem]:
        if self.use_notion and self.notion.configured:
            return self.notion.load_pipeline_models()
        return load_pipeline(self.config.pipeline_file)

    def _persist_pipeline_item(
        self,
        item: PipelineItem,
        *,
        pipeline: list[PipelineItem] | None = None,
    ) -> None:
        if self.use_notion and self.notion.configured:
            frontmatter: dict = {}
            body_markdown = ""
            item.metadata["generated_image_url"] = build_generated_image_url(item)
            if item.path and Path(item.path).exists():
                frontmatter, body_markdown = parse_markdown_file(Path(item.path))
            self.notion.upsert_pipeline_item(
                item,
                post_frontmatter=frontmatter,
                post_markdown=body_markdown,
            )
            return

        if pipeline is None:
            pipeline = load_pipeline(self.config.pipeline_file)
            replaced = False
            for idx, existing in enumerate(pipeline):
                if existing.id == item.id:
                    pipeline[idx] = item
                    replaced = True
                    break
            if not replaced:
                pipeline.append(item)
        save_pipeline(self.config.pipeline_file, pipeline)

    @staticmethod
    def read_json_body(environ) -> dict:
        content_length = int(environ.get("CONTENT_LENGTH", "0") or "0")
        if content_length <= 0:
            return {}
        body = environ["wsgi.input"].read(content_length).decode("utf-8")
        if not body.strip():
            return {}
        return json.loads(body)

    @staticmethod
    def respond(start_response, payload: dict, status: HTTPStatus = HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        start_response(
            f"{status.value} {status.phrase}",
            cors_headers(
                [
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ]
            ),
        )
        return [body]

    @staticmethod
    def respond_binary(
        start_response,
        payload: bytes,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
    ):
        start_response(
            f"{status.value} {status.phrase}",
            cors_headers(
                [
                    ("Content-Type", content_type),
                    ("Content-Length", str(len(payload))),
                    ("Cache-Control", "public, max-age=86400"),
                ]
            ),
        )
        return [payload]

    def serve_generated_image(self, start_response, image_name: str):
        safe_name = Path(image_name).name
        if (
            not safe_name
            or safe_name != image_name
            or not re.fullmatch(r"[a-zA-Z0-9_.-]+", safe_name)
        ):
            return self.respond(
                start_response,
                {"error": "Invalid image name."},
                status=HTTPStatus.BAD_REQUEST,
            )
        image_path = GENERATED_IMAGE_DIR / safe_name
        if not image_path.exists():
            return self.respond(
                start_response,
                {"error": "Image not found."},
                status=HTTPStatus.NOT_FOUND,
            )
        guessed_type, _ = mimetypes.guess_type(image_path.name)
        content_type = guessed_type or "application/octet-stream"
        return self.respond_binary(
            start_response,
            image_path.read_bytes(),
            content_type=content_type,
        )

    def serve_web_app(self, start_response, request_path: str):
        if not DIST_DIR.exists():
            return self.respond(
                start_response,
                {"error": "UI build not found. Run `npm run build` before starting the API."},
                status=HTTPStatus.NOT_FOUND,
            )
        normalized = request_path.lstrip("/")
        requested_file = DIST_DIR / normalized if normalized else DIST_DIR / "index.html"
        if requested_file.exists() and requested_file.is_file():
            return self._serve_static_file(start_response, requested_file)
        return self._serve_static_file(start_response, DIST_DIR / "index.html")

    def _serve_static_file(self, start_response, file_path: Path):
        if not file_path.exists():
            return self.respond(
                start_response,
                {"error": "Requested file not found."},
                status=HTTPStatus.NOT_FOUND,
            )
        payload = file_path.read_bytes()
        guessed_type, _ = mimetypes.guess_type(file_path.name)
        content_type = guessed_type or "application/octet-stream"
        cache = "public, max-age=86400"
        if file_path.name == "index.html":
            cache = "no-store"
        start_response(
            f"{HTTPStatus.OK.value} {HTTPStatus.OK.phrase}",
            cors_headers(
                [
                    ("Content-Type", content_type),
                    ("Content-Length", str(len(payload))),
                    ("Cache-Control", cache),
                ]
            ),
        )
        return [payload]


def cors_headers(extra_headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS"),
        ("Access-Control-Allow-Headers", "Content-Type"),
        *extra_headers,
    ]


def build_generated_image_name(*, prompt: str, output_format: str) -> str:
    suffix = sanitize_slug(prompt)[:60] or "blog-cover"
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    normalized_format = output_format.strip().lower() or "png"
    ext = {"jpg": "jpeg"}.get(normalized_format, normalized_format)
    if ext not in {"png", "jpeg", "webp"}:
        ext = "png"
    return f"{timestamp}-{suffix}.{ext}"


IMAGE_PROMPT_WRITER_SYSTEM = """You are an art director for a premium D2C skin-health textile brand called Doctor Towels. You write image generation prompts in a specific visual style. Every prompt you produce must follow these rules:

BACKGROUND: A seamless dark gradient backdrop. Colors are always deep and saturated - choose from: dark navy/indigo blue, dark burnt orange/amber, dark forest/emerald green, dark crimson/burgundy red. The gradient shifts between two tones within the same color family (e.g. deep burnt orange to dark rust). Describe the gradient with a poetic simile (e.g. "like molten metal cooling", "like light through dense canopy").

SUBJECT: A thin white or cream line illustration - hairline-thin, single-weight linework, slightly imperfect like hand-drawn with a technical pen. Pure outline, no fill, no shading unless stipple dots are used sparingly. The subject should be abstract or diagrammatic - not literal product shots. Think: scientific cross-sections, molecular structures, anatomical blueprints, textile weave patterns, topographic maps, constellation-style scatter diagrams. The illustration should visually map to one of these brand themes: skin science, bacteria/microbiome, textile/fiber, wellness rituals, skin barrier, cellular biology.

COMPOSITION: Ultra-minimal. Generous negative space. Either centered or intentionally asymmetric (weighted to one side with heavy negative space opposite). Never cluttered.

ANNOTATION ELEMENTS (optional): Faint dashed annotation lines, dotted connection lines, tiny open circles at endpoints. No text, no labels - just the gesture of a diagram.

STYLE TAGS: Always end with "Editorial [domain] meets [domain], minimalist poster design/composition. Aspect Ratio 16:9" - where domains reference the two worlds colliding (e.g. "Editorial microbiology meets celestial cartography").

COLOR CODING SYSTEM (loose guideline):
- Dark blue -> molecular / chemistry / science
- Dark orange -> textile / material / fiber
- Dark green -> biology / growth / microbiome
- Dark red -> skin / anatomy / body

When given a blog title, article topic, or brand theme - produce one prompt following all the above rules. The illustration subject should be a clever visual metaphor for the topic, never a literal depiction of the product.

Return exactly one prompt as plain text. No markdown. No bullets. No surrounding quotes."""


def build_image_prompt(*, item: PipelineItem | None, provider: BlogAgentProvider, config: AgentConfig) -> str:
    title = (item.title if item else "").strip()
    topic = (item.query if item else "").strip()
    cluster = (item.cluster if item else "").strip()
    pillar = (item.pillar_name if item else "").strip()
    main_topic = (item.main_topic if item else "").strip()
    user_prompt = (
        "Create one image-generation prompt for this article context.\n"
        f"Blog title: {title or 'N/A'}\n"
        f"Article topic/query: {topic or 'N/A'}\n"
        f"Cluster/theme: {cluster or 'N/A'}\n"
        f"Pillar: {pillar or 'N/A'}\n"
        f"Main topic: {main_topic or 'N/A'}"
    )
    generated = provider.complete(
        IMAGE_PROMPT_WRITER_SYSTEM,
        user_prompt,
        model=config.image_prompt_model,
        max_output_tokens=500,
    ).strip()
    return generated.strip('"').strip("'")


def build_generated_image_url(item: PipelineItem) -> str:
    image_name = str(item.metadata.get("generated_image_file", "")).strip()
    if not image_name:
        return ""
    return f"/api/images/{image_name}"


def normalize_seed_keywords(values: list[str]) -> list[str]:
    parsed: list[str] = []
    for value in values:
        for part in value.split(","):
            cleaned = part.strip()
            if cleaned:
                parsed.append(cleaned)
    deduped: list[str] = []
    seen: set[str] = set()
    for value in parsed:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped[:5]


def ensure_pillar_head_topics(
    *,
    pipeline: list[PipelineItem],
    clusters: list[KeywordCluster],
    start_date: date,
) -> list[PipelineItem]:
    created: list[PipelineItem] = []
    blueprints = build_pillar_blueprints(clusters)
    existing_pillars = {item.pillar_id for item in pipeline if item.is_pillar_head and item.pillar_id}

    for index, blueprint in enumerate(blueprints):
        if blueprint["pillar_id"] in existing_pillars:
            continue
        slug = sanitize_slug(f"{blueprint['pillar_id']}-pillar-head-{blueprint['main_topic']}")
        query = blueprint["keywords"][0] if blueprint["keywords"] else blueprint["main_topic"]
        item = PipelineItem(
            id=f"pillar-head-{blueprint['pillar_id']}",
            title=f"{blueprint['main_topic']}: The Definitive Guide",
            query=query,
            cluster=blueprint["cluster_name"],
            pillar_id=blueprint["pillar_id"],
            pillar_name=blueprint["pillar_name"],
            main_topic=blueprint["main_topic"],
            sub_blog_tag="pillar-head",
            is_pillar_head=True,
            pillar_head_slug=slug,
            planned_keywords=blueprint["keywords"][:12],
            scheduled_for=start_date + timedelta(days=index),
            status="topic",
            created_at=datetime.now().isoformat(timespec="seconds"),
            topic_angle=(
                "Flagship pillar-head post that establishes authority, summarizes the pillar claim, "
                "and becomes the primary internal reference for all related sub-blogs."
            ),
            topic_outline=[
                "H2: The Core Claim and Why It Matters",
                "H2: The Science and Mechanisms",
                "H2: Real-World Scenarios and Mistakes",
                "H2: Practical Prevention Playbook",
                "H2: How This Pillar Connects to Daily Habits",
            ],
            topic_internal_links=["/products/doctor-towels"],
            metadata={
                "slug": slug,
                "meta_description": (
                    f"A comprehensive guide to {blueprint['main_topic'].lower()} and the daily habits that support healthier skin."
                )[:160],
            },
        )
        pipeline.append(item)
        created.append(item)
    return created


def build_pillar_blueprints(clusters: list[KeywordCluster]) -> list[dict]:
    by_pillar: dict[str, dict] = {}
    for cluster in clusters:
        pillar_id = cluster.pillar_id.strip() or sanitize_slug(cluster.pillar_name or cluster.main_topic)
        if not pillar_id:
            continue
        row = by_pillar.setdefault(
            pillar_id,
            {
                "pillar_id": pillar_id,
                "pillar_name": cluster.pillar_name or cluster.main_topic,
                "main_topic": cluster.main_topic or cluster.pillar_name or cluster.name,
                "cluster_name": cluster.name,
                "keywords": [],
            },
        )
        for keyword in cluster.supporting_keywords + cluster.queries:
            if keyword and keyword not in row["keywords"]:
                row["keywords"].append(keyword)
    return sorted(by_pillar.values(), key=lambda row: row["pillar_id"])


def sanitize_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", cleaned)


def resolve_cluster_from_item(item: PipelineItem) -> KeywordCluster:
    return KeywordCluster(
        name=item.cluster,
        intent="informational",
        queries=[item.query],
        notes="Resolved from pipeline item context.",
        pillar_id=item.pillar_id,
        pillar_name=item.pillar_name,
        main_topic=item.main_topic,
        sub_blog_tag=item.sub_blog_tag,
        supporting_keywords=item.planned_keywords,
    )


def find_best_cluster_for_item(item: PipelineItem, clusters: list[KeywordCluster]) -> KeywordCluster | None:
    if not clusters:
        return None

    normalized_query = (item.query or "").strip().lower()
    normalized_cluster = (item.cluster or "").strip().lower()
    normalized_main_topic = (item.main_topic or "").strip().lower()
    normalized_pillar_name = (item.pillar_name or "").strip().lower()

    def tokenize(value: str) -> set[str]:
        return {
            token
            for token in re.findall(r"[a-z0-9]+", value.lower())
            if len(token) > 2 and token not in {"and", "for", "with", "the", "you", "your", "how", "why"}
        }

    item_tokens = tokenize(" ".join(filter(None, [item.query or "", item.title or "", item.cluster or ""])))
    best: tuple[int, KeywordCluster] | None = None
    for cluster in clusters:
        score = 0
        if item.pillar_id and cluster.pillar_id == item.pillar_id:
            score += 16
        if normalized_cluster and (cluster.name or "").strip().lower() == normalized_cluster:
            score += 12
        if normalized_query:
            query_candidates = {
                value.strip().lower()
                for value in [*cluster.queries, *cluster.supporting_keywords]
                if value and value.strip()
            }
            if normalized_query in query_candidates:
                score += 10
        if normalized_main_topic and (cluster.main_topic or "").strip().lower() == normalized_main_topic:
            score += 6
        if normalized_pillar_name and (cluster.pillar_name or "").strip().lower() == normalized_pillar_name:
            score += 6
        if item_tokens:
            cluster_tokens = tokenize(
                " ".join(
                    filter(
                        None,
                        [
                            cluster.name,
                            cluster.main_topic,
                            cluster.pillar_name,
                            *cluster.queries,
                            *cluster.supporting_keywords,
                        ],
                    )
                )
            )
            if cluster_tokens:
                shared = len(item_tokens & cluster_tokens)
                score += min(shared, 8)
        if score <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, cluster)
    return best[1] if best else None


def backfill_pipeline_pillar_context(items: list[PipelineItem], clusters: list[KeywordCluster]) -> bool:
    changed = False
    for item in items:
        if item.pillar_id and item.pillar_name and item.main_topic and item.sub_blog_tag:
            continue
        match = find_best_cluster_for_item(item, clusters)
        if not match:
            continue
        before = (
            item.pillar_id,
            item.pillar_name,
            item.main_topic,
            item.pillar_claim,
            item.sub_blog_tag,
        )
        enrich_item_pillar_context(item=item, pillar_id=match.pillar_id, clusters=clusters)
        if (
            item.pillar_id,
            item.pillar_name,
            item.main_topic,
            item.pillar_claim,
            item.sub_blog_tag,
        ) != before:
            changed = True
    return changed


def enrich_item_pillar_context(item: PipelineItem, pillar_id: str, clusters: list[KeywordCluster]) -> None:
    if not pillar_id:
        return

    matching_cluster = next((cluster for cluster in clusters if cluster.pillar_id == pillar_id), None)
    if not item.pillar_id:
        item.pillar_id = pillar_id
    if matching_cluster:
        if not item.pillar_name:
            item.pillar_name = matching_cluster.pillar_name or matching_cluster.name
        if not item.main_topic:
            item.main_topic = matching_cluster.main_topic or matching_cluster.pillar_name or matching_cluster.name
        if not item.pillar_claim:
            item.pillar_claim = matching_cluster.pillar_claim
    if not item.sub_blog_tag:
        item.sub_blog_tag = sanitize_slug(item.query or item.title)


def normalize_internal_links(links: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        cleaned = str(link).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def blog_slug_from_post_id(post_id: str | None) -> str:
    if not post_id:
        return ""
    stem = Path(post_id).stem
    match = re.match(r"^\d{4}-\d{2}-\d{2}-(.+)$", stem)
    return match.group(1) if match else stem


def build_main_blog_url(item: PipelineItem) -> str:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    canonical = str(metadata.get("shopify_article_url", "")).strip()
    if canonical:
        parsed = urlparse(canonical)
        if parsed.scheme and parsed.netloc:
            if parsed.path.startswith("/blogs/"):
                return parsed.path
        elif canonical.startswith("/blogs/"):
            return canonical

    blog_handle = str(metadata.get("shopify_blog_handle", "")).strip()
    article_handle = (item.shopify_article_handle or "").strip()
    if blog_handle and article_handle:
        return f"/blogs/{sanitize_slug(blog_handle)}/{sanitize_slug(article_handle)}"

    if item.status in {"topic", "draft", "approved"}:
        # Keep preview-friendly fallback links for non-pushed rows.
        if article_handle:
            return f"/blogs/{sanitize_slug(article_handle)}"
        slug = blog_slug_from_post_id(item.post_id)
        if slug:
            return f"/blogs/{slug}"
        metadata_slug = str(item.metadata.get("slug", "")).strip()
        if metadata_slug:
            return f"/blogs/{sanitize_slug(metadata_slug)}"
        if item.pillar_head_slug:
            return f"/blogs/{sanitize_slug(item.pillar_head_slug)}"
        if item.main_topic:
            return f"/blogs/{sanitize_slug(item.main_topic)}"
    return ""


def find_reporting_main_item(item: PipelineItem, pipeline: list[PipelineItem]) -> PipelineItem | None:
    if item.topic_role != "side":
        return None

    allowed_statuses = {"pushed"} if item.status == "pushed" else {"approved", "pushed"}
    candidates = [
        entry
        for entry in pipeline
        if entry.id != item.id
        and entry.topic_role == "main"
        and entry.status in allowed_statuses
        and (
            (item.pillar_id and entry.pillar_id == item.pillar_id)
            or (item.main_topic and entry.main_topic and entry.main_topic == item.main_topic)
        )
    ]
    status_rank = {"pushed": 2, "approved": 1}
    candidates.sort(
        key=lambda entry: (
            status_rank.get(entry.status, 0),
            int(bool(entry.shopify_article_handle)),
            int(bool(entry.post_id)),
            entry.created_at,
        ),
        reverse=True,
    )
    if candidates:
        return candidates[0]
    return None


def resolve_connected_main_blog_url(item: PipelineItem, pipeline: list[PipelineItem]) -> str:
    if item.topic_role != "side":
        return ""

    manager = find_reporting_main_item(item, pipeline)
    if manager:
        return build_main_blog_url(manager)
    return ""


def assign_hierarchy_metadata(item: PipelineItem, pipeline: list[PipelineItem]) -> bool:
    before = {
        "hierarchy_role": str(item.metadata.get("hierarchy_role", "")),
        "reports_to_main_id": str(item.metadata.get("reports_to_main_id", "")),
        "reports_to_main_title": str(item.metadata.get("reports_to_main_title", "")),
        "reports_to_main_url": str(item.metadata.get("reports_to_main_url", "")),
    }

    if item.topic_role == "main":
        item.metadata["hierarchy_role"] = "main-ceo"
        item.metadata["reports_to_main_id"] = ""
        item.metadata["reports_to_main_title"] = ""
        item.metadata["reports_to_main_url"] = ""
    elif item.topic_role == "side":
        manager = find_reporting_main_item(item, pipeline)
        item.metadata["hierarchy_role"] = "sub-reports-to-main"
        if manager:
            item.metadata["reports_to_main_id"] = manager.id
            item.metadata["reports_to_main_title"] = manager.title
            item.metadata["reports_to_main_url"] = build_main_blog_url(manager)
        else:
            item.metadata["reports_to_main_id"] = ""
            item.metadata["reports_to_main_title"] = ""
            item.metadata["reports_to_main_url"] = ""
    else:
        item.metadata["hierarchy_role"] = "pillar-company"
        item.metadata["reports_to_main_id"] = ""
        item.metadata["reports_to_main_title"] = ""
        item.metadata["reports_to_main_url"] = ""

    after = {
        "hierarchy_role": str(item.metadata.get("hierarchy_role", "")),
        "reports_to_main_id": str(item.metadata.get("reports_to_main_id", "")),
        "reports_to_main_title": str(item.metadata.get("reports_to_main_title", "")),
        "reports_to_main_url": str(item.metadata.get("reports_to_main_url", "")),
    }
    return before != after


def synchronize_pillar_hierarchy(*, pipeline: list[PipelineItem], pillar_id: str) -> list[PipelineItem]:
    changed: list[PipelineItem] = []
    blog_slug_lookup = build_pushed_blog_slug_lookup(pipeline)
    for item in pipeline:
        if pillar_id and item.pillar_id != pillar_id:
            continue
        before_links = list(item.topic_internal_links)
        markdown_changed = False
        if item.status in {"approved", "pushed"}:
            strip_unresolved = item.status == "pushed"
            markdown_changed = rewrite_item_markdown_short_blog_links(
                item=item,
                blog_slug_lookup=blog_slug_lookup,
                strip_unresolved=strip_unresolved,
            )
            normalize_item_internal_blog_links(
                item=item,
                blog_slug_lookup=blog_slug_lookup,
                strip_unresolved=strip_unresolved,
            )
        if item.topic_role == "side":
            ensure_sub_blog_has_main_blog_link(item=item, pipeline=pipeline)
            if item.status in {"approved", "pushed"}:
                markdown_changed = ensure_sub_blog_backlink_in_markdown(item=item, pipeline=pipeline) or markdown_changed
        metadata_changed = assign_hierarchy_metadata(item=item, pipeline=pipeline)
        links_changed = before_links != item.topic_internal_links
        if metadata_changed or links_changed or markdown_changed:
            changed.append(item)
    return changed


def build_pushed_blog_slug_lookup(pipeline: list[PipelineItem]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    pushed_items = [item for item in pipeline if item.status == "pushed"]
    pushed_items.sort(key=lambda item: created_at_sort_key(item.created_at), reverse=True)
    for item in pushed_items:
        canonical_url = build_main_blog_url(item)
        if not canonical_url.startswith("/blogs/") or canonical_url.count("/") < 3:
            continue
        for slug in derive_blog_slug_candidates(item):
            lookup.setdefault(slug, canonical_url)
    return lookup


def derive_blog_slug_candidates(item: PipelineItem) -> set[str]:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    raw_candidates = [
        str(item.shopify_article_handle or "").strip(),
        str(blog_slug_from_post_id(item.post_id) or "").strip(),
        str(metadata.get("slug", "")).strip(),
        str(item.main_topic or "").strip(),
        str(item.title or "").strip(),
    ]
    candidates = {sanitize_slug(value) for value in raw_candidates if value}
    return {candidate for candidate in candidates if candidate}


def short_blog_slug_from_target(target: str) -> str:
    parsed = urlparse(str(target or "").strip())
    path = str(parsed.path or "").strip()
    match = re.fullmatch(r"/blogs/([^/]+)/?", path, flags=re.IGNORECASE)
    if not match:
        return ""
    return sanitize_slug(unquote(match.group(1)))


def rewrite_short_blog_links_in_markdown(
    body: str,
    *,
    blog_slug_lookup: dict[str, str],
    strip_unresolved: bool,
) -> tuple[str, bool]:
    if not body:
        return body, False

    changed = False
    pattern = re.compile(r"\[([^\]]+)\]\(\s*([^)]+?)\s*\)")

    def _replace(match: re.Match[str]) -> str:
        nonlocal changed
        anchor = str(match.group(1) or "").strip()
        target = str(match.group(2) or "").strip()
        slug = short_blog_slug_from_target(target)
        if not slug:
            return match.group(0)
        canonical_url = blog_slug_lookup.get(slug, "")
        if canonical_url:
            if target != canonical_url:
                changed = True
            return f"[{anchor}]({canonical_url})"
        if strip_unresolved:
            changed = True
            return anchor
        return match.group(0)

    updated_body = pattern.sub(_replace, body)
    return updated_body, changed


def rewrite_short_blog_hrefs_in_html(
    html: str,
    *,
    blog_slug_lookup: dict[str, str],
    fallback_url: str,
) -> tuple[str, bool]:
    if not html:
        return html, False

    changed = False
    pattern = re.compile(r"href=(['\"])(/blogs/[^'\"#?]+)\1", flags=re.IGNORECASE)

    def _replace(match: re.Match[str]) -> str:
        nonlocal changed
        quote = str(match.group(1) or '"')
        target = str(match.group(2) or "").strip()
        slug = short_blog_slug_from_target(target)
        if not slug:
            return match.group(0)

        canonical_url = blog_slug_lookup.get(slug, "")
        if not canonical_url and fallback_url:
            canonical_url = fallback_url
        if not canonical_url:
            return match.group(0)
        if canonical_url != target:
            changed = True
        return f"href={quote}{canonical_url}{quote}"

    updated_html = pattern.sub(_replace, html)
    return updated_html, changed


def rewrite_item_markdown_short_blog_links(
    *,
    item: PipelineItem,
    blog_slug_lookup: dict[str, str],
    strip_unresolved: bool,
) -> bool:
    if not item.path:
        return False
    file_path = Path(item.path)
    if not file_path.exists():
        return False

    raw = read_text_file(file_path)
    split_token = "\n---\n"
    if raw.startswith("---\n") and split_token in raw:
        _head, rest = raw.split("---\n", 1)
        frontmatter_raw, body = rest.split(split_token, 1)
        updated_body, changed = rewrite_short_blog_links_in_markdown(
            body.strip(),
            blog_slug_lookup=blog_slug_lookup,
            strip_unresolved=strip_unresolved,
        )
        if not changed:
            return False
        new_raw = f"---\n{frontmatter_raw}{split_token}{updated_body.strip()}\n"
    else:
        updated_body, changed = rewrite_short_blog_links_in_markdown(
            raw.strip(),
            blog_slug_lookup=blog_slug_lookup,
            strip_unresolved=strip_unresolved,
        )
        if not changed:
            return False
        new_raw = f"{updated_body.strip()}\n"

    if new_raw == raw:
        return False
    write_text_file(file_path, new_raw)
    return True


def normalize_item_internal_blog_links(
    *,
    item: PipelineItem,
    blog_slug_lookup: dict[str, str],
    strip_unresolved: bool,
) -> bool:
    changed = False
    updated_links: list[str] = []
    for link in item.topic_internal_links:
        normalized_link = str(link or "").strip()
        if not normalized_link:
            continue
        slug = short_blog_slug_from_target(normalized_link)
        if slug:
            canonical_url = blog_slug_lookup.get(slug, "")
            if canonical_url:
                if canonical_url != normalized_link:
                    changed = True
                updated_links.append(canonical_url)
                continue
            if strip_unresolved:
                changed = True
                continue
        updated_links.append(normalized_link)
    normalized = normalize_internal_links(updated_links)
    if normalized != item.topic_internal_links:
        item.topic_internal_links = normalized
        changed = True
    return changed


def hydrate_hierarchy_fields(row: dict) -> None:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    row["hierarchy_role"] = str(
        row.get("hierarchy_role")
        or metadata.get("hierarchy_role")
        or ("main-ceo" if row.get("topic_role") == "main" else "sub-reports-to-main")
    )
    row["reports_to_main_id"] = str(row.get("reports_to_main_id") or metadata.get("reports_to_main_id") or "")
    row["reports_to_main_title"] = str(row.get("reports_to_main_title") or metadata.get("reports_to_main_title") or "")
    row["reports_to_main_url"] = str(row.get("reports_to_main_url") or metadata.get("reports_to_main_url") or "")


def ensure_sub_blog_has_main_blog_link(item: PipelineItem, pipeline: list[PipelineItem]) -> None:
    if item.topic_role != "side":
        return
    main_blog_url = resolve_connected_main_blog_url(item, pipeline)
    legacy_candidates = derive_legacy_main_blog_link_candidates(
        item=item,
        pipeline=pipeline,
        canonical_url=main_blog_url,
    )
    normalized_legacy = {re.sub(r"/+$", "", link.lower()) for link in legacy_candidates}
    kept_links = [
        link
        for link in item.topic_internal_links
        if re.sub(r"/+$", "", str(link or "").strip().lower()) not in normalized_legacy
    ]
    if main_blog_url:
        item.topic_internal_links = normalize_internal_links([*kept_links, main_blog_url])
        return
    item.topic_internal_links = normalize_internal_links(kept_links)


def inject_backlink_into_markdown(body: str, main_blog_url: str, anchor_text: str) -> str:
    if not main_blog_url:
        return body

    link_pattern = re.compile(rf"\]\(\s*{re.escape(main_blog_url)}\s*\)", re.IGNORECASE)
    if link_pattern.search(body):
        return body

    safe_anchor = anchor_text.strip() or "the main pillar guide"
    backlink_line = f"For a full foundation on this pillar, read [{safe_anchor}]({main_blog_url})."
    marker = re.search(r"(?mi)^##\s+Medical Sources & Further Reading\s*$", body)
    if marker:
        before = body[: marker.start()].rstrip()
        after = body[marker.start() :].lstrip("\n")
        return f"{before}\n\n{backlink_line}\n\n{after}"
    return f"{body.rstrip()}\n\n{backlink_line}\n"


def ensure_sub_blog_backlink_in_markdown(item: PipelineItem, pipeline: list[PipelineItem]) -> bool:
    if item.topic_role != "side" or not item.path:
        return False
    file_path = Path(item.path)
    if not file_path.exists():
        return False

    main_blog_url = resolve_connected_main_blog_url(item, pipeline)
    legacy_candidates = derive_legacy_main_blog_link_candidates(item=item, pipeline=pipeline, canonical_url=main_blog_url)
    if not main_blog_url and not legacy_candidates:
        return False

    raw = read_text_file(file_path)
    split_token = "\n---\n"
    if raw.startswith("---\n") and split_token in raw:
        _head, rest = raw.split("---\n", 1)
        frontmatter_raw, body = rest.split(split_token, 1)
        updated_body = body.strip()
        if main_blog_url:
            updated_body = inject_backlink_into_markdown(updated_body, main_blog_url, item.main_topic)
        updated_body = rewrite_legacy_main_blog_links(
            updated_body,
            canonical_url=main_blog_url,
            legacy_candidates=legacy_candidates,
        )
        new_raw = f"---\n{frontmatter_raw}{split_token}{updated_body.strip()}\n"
    else:
        updated_body = raw.strip()
        if main_blog_url:
            updated_body = inject_backlink_into_markdown(updated_body, main_blog_url, item.main_topic)
        updated_body = rewrite_legacy_main_blog_links(
            updated_body,
            canonical_url=main_blog_url,
            legacy_candidates=legacy_candidates,
        )
        new_raw = f"{updated_body.strip()}\n"

    if new_raw != raw:
        write_text_file(file_path, new_raw)
        return True
    return False


def derive_legacy_main_blog_link_candidates(
    *,
    item: PipelineItem,
    pipeline: list[PipelineItem],
    canonical_url: str,
) -> set[str]:
    candidates: set[str] = set()
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    current_report_url = str(metadata.get("reports_to_main_url", "")).strip()
    if current_report_url.startswith("/blogs/"):
        candidates.add(current_report_url)

    manager = find_reporting_main_item(item, pipeline)
    if not manager:
        normalized_canonical = re.sub(r"/+$", "", canonical_url.strip().lower())
        return {candidate for candidate in candidates if re.sub(r"/+$", "", candidate.lower()) != normalized_canonical}

    article_handle = str(manager.shopify_article_handle or "").strip()
    if article_handle:
        candidates.add(f"/blogs/{sanitize_slug(article_handle)}")

    post_slug = blog_slug_from_post_id(manager.post_id)
    if post_slug:
        candidates.add(f"/blogs/{sanitize_slug(post_slug)}")

    metadata_slug = str((manager.metadata if isinstance(manager.metadata, dict) else {}).get("slug", "")).strip()
    if metadata_slug:
        candidates.add(f"/blogs/{sanitize_slug(metadata_slug)}")

    if manager.main_topic:
        candidates.add(f"/blogs/{sanitize_slug(manager.main_topic)}")
    if manager.title:
        candidates.add(f"/blogs/{sanitize_slug(manager.title)}")

    normalized_canonical = re.sub(r"/+$", "", canonical_url.strip().lower())
    return {candidate for candidate in candidates if re.sub(r"/+$", "", candidate.lower()) != normalized_canonical}


def rewrite_legacy_main_blog_links(
    body: str,
    *,
    canonical_url: str,
    legacy_candidates: set[str],
) -> str:
    if not legacy_candidates:
        return body

    normalized_lookup = {
        re.sub(r"/+$", "", candidate.lower()): candidate
        for candidate in legacy_candidates
    }

    if not canonical_url:
        def _strip_markdown_link(match: re.Match[str]) -> str:
            anchor = str(match.group(1) or "")
            target = str(match.group(2) or "").strip()
            normalized = re.sub(r"/+$", "", target.lower())
            if normalized in normalized_lookup:
                return anchor
            return match.group(0)

        return re.sub(r"\[([^\]]+)\]\(\s*([^)]+?)\s*\)", _strip_markdown_link, body)

    def _replace(match: re.Match[str]) -> str:
        target = str(match.group(1) or "").strip()
        normalized = re.sub(r"/+$", "", target.lower())
        if normalized in normalized_lookup:
            return f"]({canonical_url})"
        return match.group(0)

    return re.sub(r"\]\(\s*([^)]+?)\s*\)", _replace, body)


def build_shopify_tags(item: PipelineItem) -> list[str]:
    tags = ["doctor-towels", "blog-agent", item.cluster.lower().replace(" ", "-")]
    if item.pillar_name:
        tags.append(item.pillar_name.lower().replace(" ", "-"))
    if item.sub_blog_tag:
        tags.append(item.sub_blog_tag.lower().replace(" ", "-"))
    return tags


def format_trend_delta(idea) -> str:
    if idea.trend:
        trend = str(idea.trend).strip()
        if trend:
            return trend
    if idea.score is None:
        return "N/A"
    return f"{idea.score}%"


def is_placeholder_title(title: str) -> bool:
    normalized = str(title or "").strip().lower()
    return normalized in {"", "untitled topic", "generating blog...", "new blog", "new post"}


def should_auto_generate_from_notion_row(item: PipelineItem) -> bool:
    if not item.pillar_id or item.topic_role not in {"main", "side"}:
        return False
    if item.post_id:
        return False
    if item.status not in {"topic", "draft", "approved", "pushed"}:
        return False
    queued = bool((item.metadata if isinstance(item.metadata, dict) else {}).get("queued_generation", False))
    return queued or is_placeholder_title(item.title) or not str(item.query or "").strip()


def created_at_sort_key(created_at: str) -> float:
    value = str(created_at or "").strip()
    if not value:
        return float("-inf")
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except (ValueError, OSError):
        return float("-inf")


def scheduled_for_sort_key(scheduled_for: str) -> float:
    value = str(scheduled_for or "").strip()
    if not value:
        return float("-inf")
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.timestamp()
    except (ValueError, OSError):
        return float("-inf")


def pipeline_payload_sort_key(item: dict) -> tuple[float, float]:
    return (
        scheduled_for_sort_key(str(item.get("scheduled_for", ""))),
        created_at_sort_key(str(item.get("created_at", ""))),
    )


def resolve_scheduled_for_from_post(*, date_value: str, post_id: str) -> str:
    cleaned = str(date_value or "").strip()
    if cleaned:
        try:
            return date.fromisoformat(cleaned).isoformat()
        except ValueError:
            pass
    match = re.match(r"^(\d{4}-\d{2}-\d{2})-", str(post_id or "").strip())
    if match:
        return match.group(1)
    return date.today().isoformat()


def build_orphan_pipeline_rows_from_posts(*, posts: list[dict], existing_items: list[dict]) -> list[dict]:
    existing_ids = {str(item.get("id", "")).strip() for item in existing_items if str(item.get("id", "")).strip()}
    existing_post_ids = {
        str(item.get("post_id", "")).strip()
        for item in existing_items
        if str(item.get("post_id", "")).strip()
    }
    existing_path_names = {
        Path(str(item.get("path", "")).strip()).name.lower()
        for item in existing_items
        if str(item.get("path", "")).strip()
    }
    valid_statuses = {"topic", "draft", "approved", "pushed", "rejected"}
    orphans: list[dict] = []

    for post in posts:
        post_id = str(post.get("id", "")).strip()
        if not post_id:
            continue
        if post_id in existing_post_ids or post_id in existing_ids:
            continue

        path_value = str(post.get("path", "")).strip()
        path_name = Path(path_value).name.lower() if path_value else ""
        if path_name and path_name in existing_path_names:
            continue

        base_id = f"manual-{Path(post_id).stem}"
        candidate_id = base_id
        counter = 2
        while candidate_id in existing_ids:
            candidate_id = f"{base_id}-{counter}"
            counter += 1

        status = str(post.get("pipelineStatus", "draft")).strip().lower()
        if status not in valid_statuses:
            status = "draft"

        scheduled_for = resolve_scheduled_for_from_post(
            date_value=str(post.get("date", "")),
            post_id=post_id,
        )
        created_at = f"{scheduled_for}T00:00:00+00:00"
        if path_value:
            local_path = Path(path_value)
            if local_path.exists():
                created_at = datetime.fromtimestamp(local_path.stat().st_mtime, tz=UTC).isoformat(timespec="seconds")

        row = {
            "id": candidate_id,
            "post_id": post_id,
            "title": str(post.get("title", post_id)).strip() or post_id,
            "query": str(post.get("query", "")).strip() or "Manual import",
            "cluster": str(post.get("cluster", "")).strip() or "Manual import",
            "pillar_id": "",
            "pillar_name": str(post.get("pillarName", "")).strip(),
            "pillar_claim": "",
            "main_topic": str(post.get("mainTopic", "")).strip(),
            "sub_blog_tag": str(post.get("subBlogTag", "")).strip(),
            "is_pillar_head": False,
            "pillar_head_post_id": None,
            "pillar_head_slug": None,
            "planned_keywords": [],
            "path": path_value or None,
            "scheduled_for": scheduled_for,
            "status": status,
            "topic_role": "side",
            "created_at": created_at,
            "approved_at": None,
            "pushed_at": None,
            "shopify_article_id": None,
            "shopify_blog_id": None,
            "shopify_article_handle": None,
            "topic_angle": "",
            "topic_outline": [],
            "topic_internal_links": [],
            "guideline_report": None,
            "metadata": {"manual_import": True},
            "html": str(post.get("html", "")),
            "excerpt": str(post.get("excerpt", "")),
            "description": str(post.get("description", "")),
            "generatedImageUrl": "",
            "generatedImageStyle": "",
            "hasGeneratedDraft": True,
        }
        hydrate_hierarchy_fields(row)
        orphans.append(row)
        existing_ids.add(candidate_id)
        existing_post_ids.add(post_id)
        if path_name:
            existing_path_names.add(path_name)
    return orphans


def action_priority(item: PipelineItem) -> tuple[int, float]:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    needs_push = is_push_requested(item) and not bool(item.shopify_article_id)
    needs_approve = item.status in {"approved", "pushed"} and not bool(item.post_id)
    needs_generate = should_auto_generate_from_notion_row(item)
    queued = bool(metadata.get("queued_generation", False))
    role_bonus = 0 if item.topic_role == "main" else 1
    if needs_push:
        # Highest urgency: explicit publish intent from Notion.
        return (0, 0.0)
    if needs_approve:
        # Next: approved rows waiting for full draft generation.
        return (1, 0.0)
    if needs_generate and queued:
        # Continue already-started generation first.
        return (2, float(role_bonus))
    if needs_generate:
        return (3, float(role_bonus))
    return (9, 0.0)


def is_push_requested(item: PipelineItem) -> bool:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    ready_to_push = bool(metadata.get("ready_to_push", False))
    # `status=pushed` in Notion is treated as an intent only until we have a pushed timestamp.
    status_requested = item.status == "pushed" and not bool(item.pushed_at)
    return ready_to_push or status_requested


def resolve_shopify_blog_id(
    *,
    item: PipelineItem,
    shopify: ShopifyPublisher,
    default_shopify_blog_id: str,
) -> str:
    blog_id = (
        str(item.shopify_blog_id or "").strip()
        or str((item.metadata if isinstance(item.metadata, dict) else {}).get("shopify_blog_id", "")).strip()
        or str(default_shopify_blog_id or "").strip()
    )
    if blog_id:
        return blog_id
    if not shopify.enabled:
        return ""
    try:
        blogs = shopify.list_blogs(limit=5)
    except Exception:  # noqa: BLE001
        return ""
    if not blogs:
        return ""
    return str(blogs[0].get("id", "")).strip()


def pick_next_pillar_for_rotation(
    pillar_ids: list[str],
    *,
    last_pillar_id: str,
    cursor: int,
) -> tuple[str, int]:
    if not pillar_ids:
        return "", cursor
    total = len(pillar_ids)
    start_index = cursor % total

    for step in range(total):
        index = (start_index + step) % total
        candidate = pillar_ids[index]
        if total == 1 or candidate != last_pillar_id:
            return candidate, (index + 1) % total
    return pillar_ids[start_index], (start_index + 1) % total


def is_action_candidate(item: PipelineItem) -> bool:
    needs_push = is_push_requested(item) and not bool(item.shopify_article_id)
    needs_approve = item.status in {"approved", "pushed"} and not bool(item.post_id)
    needs_generate = should_auto_generate_from_notion_row(item)
    return needs_generate or needs_approve or needs_push


def build_market_cards(trend_cards: list[dict], seeds: list[str]) -> list[dict]:
    cards: list[dict] = []
    for index, card in enumerate(trend_cards[:8]):
        keyword = str(card.get("keyword", "")).strip()
        if not keyword:
            continue
        cards.append(
            {
                "id": f"market-{index}",
                "channel": "market",
                "rank": index + 1,
                "keyword": keyword,
                "delta": card.get("delta", "N/A"),
                "kind": "breakout" if card.get("kind") == "rising" else "steady",
                "source": "google+editorial",
            }
        )

    if not cards:
        cards = [
            {
                "id": f"market-seed-{index}",
                "channel": "market",
                "rank": index + 1,
                "keyword": seed,
                "delta": "Watch",
                "kind": "seed",
                "source": "editorial",
            }
            for index, seed in enumerate(seeds[:8])
        ]
    return cards


def build_chatgpt_visibility_cards(topics: list[str], posts: list[dict], pipeline: list[dict]) -> list[dict]:
    cards: list[dict] = []
    for index, topic in enumerate(topics[:10]):
        ranked_posts = rank_posts_for_topic(topic, posts)
        ranked = len(ranked_posts) > 0
        status_rank = index + 1 if ranked else None
        coverage = [
            {
                "title": post["title"],
                "id": post["id"],
                "score": round(score, 2),
            }
            for post, score in ranked_posts[:3]
        ]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(topic, item.get("query", "")) >= 0.45
        ][:2]
        cards.append(
            {
                "id": f"visibility-{index}",
                "channel": "chatgpt-search",
                "rank": status_rank,
                "keyword": topic,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "content-audit",
                "visible": ranked,
                "coverage": coverage,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    return cards


def score_chatgpt_visibility(cards: list[dict]) -> int:
    if not cards:
        return 0
    ranked_count = len([card for card in cards if card.get("visible")])
    base = int(round((ranked_count / len(cards)) * 100))
    bonus = min(15, sum(2 for card in cards if card.get("visible") and card.get("rank") == 1))
    return max(0, min(100, base + bonus))


def build_chatgpt_visibility_from_report(report: dict, pipeline: list[dict]) -> list[dict]:
    provider_lookup = {
        str(item.get("provider", "")).lower(): item
        for item in report.get("providers", [])
    }
    provider = (
        provider_lookup.get("openai")
        or provider_lookup.get("chatgpt")
        or next(iter(provider_lookup.values()), {})
    )
    results = provider.get("results", [])
    result_by_prompt_id = {
        str(item.get("promptId", "")).strip(): item for item in results if item.get("promptId")
    }
    prompts = report.get("prompts", [])
    cards: list[dict] = []
    for index, prompt_row in enumerate(prompts):
        prompt_id = str(prompt_row.get("id", f"prompt-{index + 1}")).strip()
        result = result_by_prompt_id.get(prompt_id, {})
        prompt = str(result.get("prompt", "") or prompt_row.get("prompt", "")).strip()
        if not prompt:
            continue
        ranked = bool(result.get("foundDomain", False))
        linked_domains = [domain for domain in result.get("linkedDomains", []) if domain][:3]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(prompt, item.get("query", "")) >= 0.45
        ][:3]
        cards.append(
            {
                "id": prompt_id,
                "channel": "chatgpt-search",
                "rank": index + 1,
                "keyword": prompt,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "llm-visibility",
                "visible": ranked,
                "coverage": [
                    {
                        "title": title,
                        "id": f"title-{index}-{pos}",
                        "score": result.get("score", 0),
                    }
                    for pos, title in enumerate(result.get("foundTitles", [])[:4], start=1)
                ],
                "linkedDomains": linked_domains,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    if cards:
        return cards

    for index, result in enumerate(results):
        prompt = str(result.get("prompt", "")).strip()
        if not prompt:
            continue
        ranked = bool(result.get("foundDomain"))
        linked_domains = [domain for domain in result.get("linkedDomains", []) if domain][:3]
        related_pipeline = [
            item
            for item in pipeline
            if lexical_overlap_score(prompt, item.get("query", "")) >= 0.45
        ][:3]
        cards.append(
            {
                "id": str(result.get("promptId", f"visibility-{index}")),
                "channel": "chatgpt-search",
                "rank": index + 1,
                "keyword": prompt,
                "delta": "Ranked" if ranked else "Not ranked",
                "kind": "ranked" if ranked else "missing",
                "source": "llm-visibility",
                "visible": ranked,
                "coverage": [
                    {"title": title, "id": f"title-{index}-{pos}", "score": result.get("score", 0)}
                    for pos, title in enumerate(result.get("foundTitles", [])[:4], start=1)
                ],
                "linkedDomains": linked_domains,
                "pipelineMatches": [
                    {
                        "title": item.get("title", ""),
                        "status": item.get("status", ""),
                        "query": item.get("query", ""),
                    }
                    for item in related_pipeline
                ],
            }
        )
    return cards


def render_post_html(body_markdown: str, website_url: str) -> str:
    html = markdown.markdown(
        body_markdown,
        extensions=["extra", "sane_lists", "smarty"],
    )
    return absolutize_blog_links(html, website_url)


def absolutize_blog_links(html: str, website_url: str) -> str:
    origin = normalize_origin(website_url)
    if not origin:
        return html
    return re.sub(
        r'href=(["\'])(/blogs/[^"\']*)\1',
        lambda match: f'href={match.group(1)}{origin}{match.group(2)}{match.group(1)}',
        html,
        flags=re.IGNORECASE,
    )


def normalize_origin(website_url: str) -> str:
    value = str(website_url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}"


def normalize_requested_keywords(raw_keywords: list[str] | str) -> list[str]:
    if isinstance(raw_keywords, str):
        candidates = re.split(r"[\n,]", raw_keywords)
    elif isinstance(raw_keywords, list):
        candidates = raw_keywords
    else:
        candidates = []
    deduped: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        keyword = str(value or "").strip()
        if not keyword:
            continue
        key = keyword.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(keyword)
    return deduped[:20]


def merge_keyword_targets(*, preferred: list[str], planned: list[str], fallback: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in [preferred, planned, fallback]:
        for value in source:
            keyword = str(value or "").strip()
            if not keyword:
                continue
            key = keyword.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(keyword)
    return merged[:20]


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    cleaned = value.strip().lower()
    if cleaned in {"1", "true", "yes", "on"}:
        return True
    if cleaned in {"0", "false", "no", "off"}:
        return False
    return default


def build_seo_pillars_seed(clusters: list[KeywordCluster]) -> list[dict]:
    grouped: dict[str, dict] = {}
    default_priority = {
        "pillar-1": 1,
        "pillar-2": 4,
        "pillar-3": 3,
        "pillar-4": 2,
        "pillar-5": 5,
    }
    for cluster in clusters:
        pillar_id = cluster.pillar_id or sanitize_slug(cluster.pillar_name or cluster.name)
        if pillar_id not in grouped:
            grouped[pillar_id] = {
                "pillarId": pillar_id,
                "pillarName": cluster.pillar_name or cluster.main_topic or cluster.name,
                "priority": default_priority.get(pillar_id, 999),
                "targetKeyword": cluster.queries[0] if cluster.queries else "",
                "pillarThesis": cluster.pillar_claim or cluster.notes,
                "clusterTopics": [],
                "status": "active",
            }
        grouped[pillar_id]["clusterTopics"].append(cluster.name)
    return sorted(grouped.values(), key=lambda row: row["priority"])


def rank_posts_for_topic(topic: str, posts: list[dict]) -> list[tuple[dict, float]]:
    scored: list[tuple[dict, float]] = []
    for post in posts:
        combined = " ".join(
            [
                str(post.get("title", "")),
                str(post.get("description", "")),
                str(post.get("excerpt", "")),
                str(post.get("query", "")),
            ]
        )
        score = lexical_overlap_score(topic, combined)
        if score >= 0.42:
            scored.append((post, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def lexical_overlap_score(left: str, right: str) -> float:
    left_tokens = tokenize_phrase(left)
    right_tokens = tokenize_phrase(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens.intersection(right_tokens))
    return overlap / max(1, len(left_tokens))


def tokenize_phrase(value: str) -> set[str]:
    stop_words = {
        "the",
        "a",
        "an",
        "for",
        "to",
        "of",
        "and",
        "or",
        "with",
        "is",
        "in",
        "on",
        "you",
        "your",
        "best",
        "how",
        "what",
    }
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 2 and token not in stop_words
    }
    return tokens
